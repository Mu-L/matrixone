// Copyright 2023 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package lockservice

import (
	"context"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/log"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/common/morpc"
	"github.com/matrixorigin/matrixone/pkg/common/stopper"
	"github.com/matrixorigin/matrixone/pkg/common/util"
	pb "github.com/matrixorigin/matrixone/pkg/pb/lock"
	"go.uber.org/zap"
)

type lockTableAllocator struct {
	service         string
	logger          *log.MOLogger
	stopper         *stopper.Stopper
	keepBindTimeout time.Duration
	address         string
	server          Server
	client          Client
	inactiveService sync.Map // lock service id -> inactive time
	ctl             sync.Map // lock service id -> *commitCtl
	version         uint64
	mu              struct {
		sync.RWMutex
		services   map[string]*serviceBinds
		lockTables map[uint32]map[uint64]pb.LockTable
	}

	// for test
	options struct {
		getActiveTxnFunc         func(sid string) (bool, [][]byte, error)
		removeDisconnectDuration time.Duration
	}
}

type AllocatorOption func(*lockTableAllocator)

// NewLockTableAllocator create a memory based lock table allocator.
func NewLockTableAllocator(
	service string,
	address string,
	keepBindTimeout time.Duration,
	cfg morpc.Config,
	opts ...AllocatorOption,
) LockTableAllocator {
	if keepBindTimeout == 0 {
		panic("invalid lock table bind timeout")
	}

	rpcClient, err := NewClient(service, cfg)
	if err != nil {
		panic(err)
	}

	logger := getLogger(service)
	tag := "lockservice.allocator"
	la := &lockTableAllocator{
		service: service,
		address: address,
		logger:  logger.Named(tag),
		stopper: stopper.NewStopper(tag,
			stopper.WithLogger(logger.RawLogger().Named(tag))),
		keepBindTimeout: keepBindTimeout,
		client:          rpcClient,
		version:         uint64(time.Now().UnixNano()),
	}
	la.mu.lockTables = make(map[uint32]map[uint64]pb.LockTable)
	la.mu.services = make(map[string]*serviceBinds)

	for _, opt := range opts {
		opt(la)
	}

	if err := la.stopper.RunTask(la.checkInvalidBinds); err != nil {
		panic(err)
	}
	if err := la.stopper.RunTask(la.cleanCommitState); err != nil {
		panic(err)
	}

	la.initServer(cfg)
	logLockAllocatorStartSucc(la.logger, la.version)

	return la
}

func (l *lockTableAllocator) Get(
	serviceID string,
	group uint32,
	tableID uint64,
	originTableID uint64,
	sharding pb.Sharding) pb.LockTable {
	binds := l.getServiceBinds(serviceID)
	if binds == nil {
		binds = l.registerService(serviceID)
	}
	return l.registerBind(binds, group, tableID, originTableID, sharding)
}

func (l *lockTableAllocator) KeepLockTableBind(serviceID string) bool {
	b := l.getServiceBinds(serviceID)
	if b == nil {
		return false
	}
	return b.active()
}

func (l *lockTableAllocator) AddCannotCommit(values []pb.OrphanTxn) [][]byte {
	var committing [][]byte
	for _, v := range values {
		c := l.getCtl(v.Service)
		for _, txn := range v.Txn {
			state := c.add(util.UnsafeBytesToString(txn), cannotCommitState)
			if state == committingState {
				committing = append(committing, txn)
			}
		}
	}
	return committing
}

func (l *lockTableAllocator) AddInvalidService(serviceID string) {
	l.inactiveService.Store(serviceID, time.Now())
}

func (l *lockTableAllocator) HasInvalidService(serviceID string) bool {
	_, ok := l.inactiveService.Load(serviceID)
	return ok
}

func (l *lockTableAllocator) Valid(
	serviceID string,
	txnID []byte,
	binds []pb.LockTable,
) ([]uint64, error) {
	var invalid []uint64
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, b := range binds {
		if !b.Valid {
			panic("BUG")
		}

		current, ok := l.getLockTablesLocked(b.Group)[b.Table]
		if !ok ||
			current.Changed(b) {
			l.logger.Info("table and service bind changed",
				zap.String("current", current.DebugString()),
				zap.String("received", b.DebugString()))
			invalid = append(invalid, b.Table)
		}
	}

	if len(invalid) > 0 {
		return invalid, nil
	}

	if _, ok := l.inactiveService.Load(serviceID); ok {
		l.logger.Info("inactive service",
			zap.String("serviceID", serviceID),
		)
		return nil, moerr.NewCannotCommitOnInvalidCNNoCtx()
	}

	c := l.getCtl(serviceID)
	state := c.add(util.UnsafeBytesToString(txnID), committingState)
	if state == cannotCommitState {
		return nil, moerr.NewCannotCommitOrphanNoCtx()
	}
	return nil, nil
}

func (l *lockTableAllocator) Close() error {
	l.stopper.Stop()
	var err error
	err1 := l.server.Close()
	l.logger.Debug("lock service allocator server closed",
		zap.Error(err))
	if err1 != nil {
		err = err1
	}
	err2 := l.client.Close()
	l.logger.Debug("lock service allocator client closed",
		zap.Error(err))
	if err2 != nil {
		err = err2
	}
	return err
}

func (l *lockTableAllocator) GetLatest(groupID uint32, tableID uint64) pb.LockTable {
	l.mu.Lock()
	defer l.mu.Unlock()

	m := l.getLockTablesLocked(groupID)
	if old, ok := m[tableID]; ok {
		return old
	}
	return pb.LockTable{}
}

func (l *lockTableAllocator) GetVersion() uint64 {
	return l.version
}

func (l *lockTableAllocator) setRestartService(serviceID string) {
	b := l.getServiceBindsWithoutPrefix(serviceID)
	if b == nil {
		l.logger.Error("not found restart lock service",
			zap.String("serviceID", serviceID))
		return
	}
	logServiceStatus(
		l.logger,
		"set restart lock service",
		serviceID,
		b.getStatus(),
	)
	b.setStatus(pb.Status_ServiceLockWaiting)
}

func (l *lockTableAllocator) remainTxnInService(serviceID string) int32 {
	b := l.getServiceBindsWithoutPrefix(serviceID)
	if b == nil {
		l.logger.Error("not found restart lock service",
			zap.String("serviceID", serviceID))
		return 0
	}
	txnIDs := b.getTxnIds()
	l.logger.Error("remain txn in restart service",
		bytesArrayField("txnIDs", txnIDs),
		zap.String("serviceID", serviceID),
		zap.String("status", b.getStatus().String()))

	c := len(txnIDs)
	if c == 0 {
		b := l.getServiceBindsWithoutPrefix(serviceID)
		if b == nil ||
			!b.isStatus(pb.Status_ServiceCanRestart) {
			// -1 means can not get right remain txn in restart lock service
			c = -1
			l.logger.Error("can not get right remain txn in restart lock service",
				zap.String("serviceID", serviceID),
				zap.String("status", b.getStatus().String()))
		}

	}
	return int32(c)
}

func (l *lockTableAllocator) validLockTable(group uint32, table uint64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	t, ok := l.mu.lockTables[group][table]
	if !ok {
		return false
	}
	return t.Valid
}

func (l *lockTableAllocator) canRestartService(serviceID string) bool {
	b := l.getServiceBindsWithoutPrefix(serviceID)
	if b == nil {
		l.logger.Error("not found restart lock service",
			zap.String("serviceID", serviceID))
		return true
	}
	logServiceStatus(
		l.logger,
		"can restart lock service",
		serviceID,
		b.getStatus(),
	)
	return b.isStatus(pb.Status_ServiceCanRestart)
}

func (l *lockTableAllocator) disableTableBinds(b *serviceBinds) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// we can't just delete the LockTable's effectiveness binding directly, we
	// need to keep the binding version.
	for g, tables := range b.groupTables {
		for table := range tables {
			if old, ok := l.getLockTablesLocked(g)[table]; ok &&
				old.ServiceID == b.serviceID {
				old.Valid = false
				l.getLockTablesLocked(g)[table] = old
			}
		}
	}

	// service need deleted, because this service may never restart
	delete(l.mu.services, b.serviceID)
	l.logger.Info("service removed",
		zap.String("service", b.serviceID))
}

func (l *lockTableAllocator) disableTableBindsWithoutDelete(b *serviceBinds) {
	l.mu.Lock()
	defer l.mu.Unlock()
	// we can't just delete the LockTable's effectiveness binding directly, we
	// need to keep the binding version.
	for g, tables := range b.groupTables {
		for table := range tables {
			if old, ok := l.getLockTablesLocked(g)[table]; ok &&
				old.ServiceID == b.serviceID {
				old.Valid = false
				l.getLockTablesLocked(g)[table] = old
			}
		}
	}
}

func (l *lockTableAllocator) disableGroupTables(groupTables []pb.LockTable, b *serviceBinds) {
	l.mu.Lock()
	defer l.mu.Unlock()
	b.Lock()
	defer b.Unlock()
	for _, t := range groupTables {
		if old, ok := l.getLockTablesLocked(t.Group)[t.Table]; ok &&
			old.ServiceID == b.serviceID {
			old.Valid = false
			l.getLockTablesLocked(t.Group)[t.Table] = old
			delete(b.groupTables[t.Group], t.Table)
		}
	}
	if len(groupTables) > 0 {
		logBindsMove(l.logger, groupTables)
	}
}

func (l *lockTableAllocator) getServiceBinds(serviceID string) *serviceBinds {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.mu.services[serviceID]
}

func (l *lockTableAllocator) getServiceBindsWithoutPrefix(serviceID string) *serviceBinds {
	l.mu.RLock()
	defer l.mu.RUnlock()
	for k, v := range l.mu.services {
		id := getUUIDFromServiceIdentifier(k)
		if serviceID == id {
			return v
		}
	}
	return nil
}

func (l *lockTableAllocator) getTimeoutBinds(now time.Time) []*serviceBinds {
	l.mu.RLock()
	defer l.mu.RUnlock()

	var values []*serviceBinds
	for _, b := range l.mu.services {
		if b.timeout(now, l.keepBindTimeout) {
			values = append(values, b)
		}
	}
	return values
}

func (l *lockTableAllocator) registerService(
	serviceID string,
) *serviceBinds {
	l.mu.Lock()
	defer l.mu.Unlock()

	b, ok := l.mu.services[serviceID]
	if ok {
		return b
	}
	b = newServiceBinds(
		serviceID,
		l.logger.With(zap.String("lockservice", serviceID)),
		l.logger,
	)
	l.mu.services[serviceID] = b
	return b
}

func (l *lockTableAllocator) registerBind(
	binds *serviceBinds,
	group uint32,
	tableID uint64,
	originTableID uint64,
	sharding pb.Sharding) pb.LockTable {
	l.mu.Lock()
	defer l.mu.Unlock()

	if old, ok := l.getLockTablesLocked(group)[tableID]; ok {
		return l.tryRebindLocked(binds, group, old, tableID)
	}
	return l.createBindLocked(binds, group, tableID, originTableID, sharding)
}

// parseServiceID parses a serviceID into timestamp and uuid parts
// serviceID format: timestamp(19 digits) + uuid
func parseServiceID(serviceID string) (int64, string) {
	if len(serviceID) <= 19 { // timestamp is 19 digits
		return 0, ""
	}
	timestamp, _ := strconv.ParseInt(serviceID[:19], 10, 64)
	return timestamp, serviceID[19:]
}

func (l *lockTableAllocator) tryRebindLocked(
	binds *serviceBinds,
	group uint32,
	old pb.LockTable,
	tableID uint64) pb.LockTable {
	// Compare serviceIDs to check if the new service is a newer version of the same service
	oldTimestamp, oldUUID := parseServiceID(old.ServiceID)
	newTimestamp, newUUID := parseServiceID(binds.serviceID)

	// If the new service has a newer timestamp and same UUID, update the binding
	if newTimestamp > oldTimestamp && oldUUID == newUUID {
		old.Valid = false

		l.logger.Info("bind updated with newer service version",
			zap.Uint64("table", tableID),
			zap.Uint64("version", old.Version),
			zap.String("service", binds.serviceID),
			zap.Int64("old timestamp", oldTimestamp),
			zap.Int64("new timestamp", newTimestamp))
	}
	// find a valid table and service bind
	if old.Valid {
		return old
	}

	// reaches here, it means that the original table and service bindings have
	// been invalidated, and the current service has also been invalidated, so
	// there is no need for any re-bind operation here, and the invalid bind
	// information is directly returned to the service.
	if !binds.bind(group, tableID) {
		return old
	}

	// If not a newer version of the same service, create new binding
	old.ServiceID = binds.serviceID
	old.Version++
	old.Valid = true
	l.getLockTablesLocked(group)[tableID] = old

	l.logger.Info("bind changed",
		zap.Uint64("table", tableID),
		zap.Uint64("version", old.Version),
		zap.String("service", binds.serviceID))
	return old
}

func (l *lockTableAllocator) createBindLocked(
	binds *serviceBinds,
	group uint32,
	tableID uint64,
	originTableID uint64,
	sharding pb.Sharding) pb.LockTable {
	// current service is invalid
	if !binds.bind(group, tableID) {
		return pb.LockTable{}
	}

	if originTableID == 0 {
		if sharding == pb.Sharding_ByRow {
			panic("invalid sharding origin table id")
		}
		originTableID = tableID
	}

	// create new table and service bind
	b := pb.LockTable{
		Table:       tableID,
		OriginTable: originTableID,
		ServiceID:   binds.serviceID,
		Version:     l.version,
		Valid:       true,
		Sharding:    sharding,
		Group:       group,
	}
	l.getLockTablesLocked(group)[tableID] = b
	l.logger.Info("bind created",
		zap.Uint64("table", tableID),
		zap.String("service", binds.serviceID))
	return b
}

func (l *lockTableAllocator) checkInvalidBinds(ctx context.Context) {
	timer := time.NewTimer(l.keepBindTimeout)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			l.logger.Debug("check lock service invalid task stopped")
			return
		case <-timer.C:
			timeoutBinds := l.getTimeoutBinds(time.Now())
			if len(timeoutBinds) > 0 {
				l.logger.Info("get timeout services",
					zap.Duration("timeout", l.keepBindTimeout),
					zap.Int("count", len(timeoutBinds)))
			}
			for _, b := range timeoutBinds {
				valid, err := validateService(
					l.keepBindTimeout,
					b.getServiceID(),
					l.client,
					l.logger,
				)
				if err != nil && isRetryError(err) {
					continue
				}
				if !valid {
					b.disable()
					l.disableTableBinds(b)
				}
			}
			timer.Reset(l.keepBindTimeout)
		}
	}
}

func (l *lockTableAllocator) cleanCommitState(ctx context.Context) {
	defer l.logger.InfoAction("clean cannot commit task")()

	timer := time.NewTimer(l.keepBindTimeout * 2)
	defer timer.Stop()

	getActiveTxnFunc := l.options.getActiveTxnFunc
	if getActiveTxnFunc == nil {
		getActiveTxnFunc = func(sid string) (bool, [][]byte, error) {
			ctx, cancel := context.WithTimeoutCause(context.Background(), defaultRPCTimeout, moerr.CauseCleanCommitState)
			defer cancel()

			req := acquireRequest()
			defer releaseRequest(req)

			req.Method = pb.Method_GetActiveTxn
			req.GetActiveTxn.ServiceID = sid

			resp, err := l.client.Send(ctx, req)
			if err != nil {
				return false, nil, moerr.AttachCause(ctx, err)
			}
			defer releaseResponse(resp)

			if !resp.GetActiveTxn.Valid {
				return false, nil, nil
			}

			return true, resp.GetActiveTxn.Txn, nil
		}
	}

	removeDisconnectDuration := l.options.removeDisconnectDuration
	if removeDisconnectDuration == 0 {
		removeDisconnectDuration = time.Hour * 24
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			l.logger.Info("clean commit state")

			var services []string
			var invalidServices []string
			activeTxnMap := make(map[string]map[string]struct{})

			l.ctl.Range(func(key, value any) bool {
				services = append(services, key.(string))
				return true
			})

			l.inactiveService.Range(func(key, value any) bool {
				if time.Since(value.(time.Time)) > removeDisconnectDuration {
					l.logger.Error("remove inactive service",
						zap.String("serviceID", key.(string)))
					l.inactiveService.Delete(key)
				}
				return true
			})

			retryCount := math.MaxInt - 1

			for _, sid := range services {
				for i := 0; i < retryCount+1; i++ {
					valid, actives, err := getActiveTxnFunc(sid)
					if err == nil {
						if !valid {
							invalidServices = append(invalidServices, sid)
						} else {
							m := make(map[string]struct{}, len(actives))
							for _, txn := range actives {
								m[util.UnsafeBytesToString(txn)] = struct{}{}
							}
							activeTxnMap[sid] = m
						}
					} else if isRetryError(err) {
						// retry err
						l.logger.Error("retry to check service if alive",
							zap.String("serviceID", sid),
							zap.Error(err))
						if i < retryCount {
							continue
						}
						l.inactiveService.Store(sid, time.Now())
						l.ctl.Delete(sid)
					} else {
						// is not retry err
						l.logger.Error("get active txn failed",
							zap.String("serviceID", sid),
							zap.Error(err))
						l.inactiveService.Store(sid, time.Now())
						l.ctl.Delete(sid)
					}
					break
				}
			}

			for _, sid := range invalidServices {
				l.ctl.Delete(sid)
			}

			l.ctl.Range(func(key, value any) bool {
				sid := key.(string)
				if m, ok := activeTxnMap[sid]; ok {
					c := value.(*commitCtl)
					c.states.Range(func(key, value any) bool {
						if _, ok := m[key.(string)]; !ok {
							if value.(ctlState) == cannotCommitState {
								logCleanCannotCommitTxn(l.logger, key.(string), int(value.(ctlState)))
							}
							c.states.Delete(key)
						}
						return true
					})
				}
				return true
			})

			timer.Reset(l.keepBindTimeout * 2)
		}
	}
}

// serviceBinds an instance of serviceBinds, recording the bindings of a lockservice
// and the locktable it manages
type serviceBinds struct {
	sync.RWMutex
	logger            *log.MOLogger
	skipLogger        *log.MOLogger
	serviceID         string
	groupTables       map[uint32]map[uint64]struct{}
	lastKeepaliveTime time.Time
	disabled          bool
	status            pb.Status
	txnIDs            [][]byte
}

func newServiceBinds(
	serviceID string,
	logger *log.MOLogger,
	skipLogger *log.MOLogger,
) *serviceBinds {
	return &serviceBinds{
		serviceID:         serviceID,
		logger:            logger,
		skipLogger:        skipLogger,
		groupTables:       make(map[uint32]map[uint64]struct{}),
		lastKeepaliveTime: time.Now(),
	}
}

func (b *serviceBinds) getTxnIds() [][]byte {
	b.RLock()
	defer b.RUnlock()
	return b.txnIDs
}

func (b *serviceBinds) setTxnIds(txnIDs [][]byte) {
	b.Lock()
	defer b.Unlock()
	b.txnIDs = txnIDs
}

func (b *serviceBinds) isStatus(status pb.Status) bool {
	b.RLock()
	defer b.RUnlock()
	return b.status == status
}

func (b *serviceBinds) getStatus() pb.Status {
	b.RLock()
	defer b.RUnlock()
	return b.status
}

func (b *serviceBinds) setStatus(status pb.Status) {
	b.Lock()
	defer b.Unlock()
	logStatusChange(b.skipLogger, b.status, status)
	b.status = status
}

func (b *serviceBinds) active() bool {
	b.Lock()
	defer b.Unlock()
	if b.disabled {
		return false
	}
	b.lastKeepaliveTime = time.Now()
	b.logger.Debug("lock service binds active")
	return true
}

func (b *serviceBinds) bind(
	group uint32,
	tableID uint64) bool {
	b.Lock()
	defer b.Unlock()
	if b.disabled {
		return false
	}
	b.getTablesLocked(group)[tableID] = struct{}{}
	return true
}

func (b *serviceBinds) getServiceID() string {
	b.RLock()
	defer b.RUnlock()
	return b.serviceID
}

func (b *serviceBinds) timeout(
	now time.Time,
	timeout time.Duration) bool {
	b.RLock()
	defer b.RUnlock()
	v := now.Sub(b.lastKeepaliveTime)
	return v >= timeout
}

func (b *serviceBinds) disable() {
	b.Lock()
	defer b.Unlock()
	b.disabled = true
	b.logger.Info("bind disabled",
		zap.String("service", b.serviceID))
}

func (b *serviceBinds) getTablesLocked(group uint32) map[uint64]struct{} {
	m, ok := b.groupTables[group]
	if ok {
		return m
	}
	m = make(map[uint64]struct{}, 1024)
	b.groupTables[group] = m
	return m
}

func (l *lockTableAllocator) initServer(cfg morpc.Config) {
	s, err := NewServer(l.service, l.address, cfg)
	if err != nil {
		panic(err)
	}
	l.server = s
	l.initHandler()

	if err := l.server.Start(); err != nil {
		panic(err)
	}
}

func (l *lockTableAllocator) initHandler() {
	l.server.RegisterMethodHandler(
		pb.Method_GetBind,
		l.handleGetBind,
	)

	l.server.RegisterMethodHandler(
		pb.Method_KeepLockTableBind,
		l.handleKeepLockTableBind,
	)

	l.server.RegisterMethodHandler(
		pb.Method_CanRestartService,
		l.handleCanRestartService,
	)

	l.server.RegisterMethodHandler(
		pb.Method_SetRestartService,
		l.handleSetRestartService,
	)

	l.server.RegisterMethodHandler(
		pb.Method_RemainTxnInService,
		l.handleRemainTxnInService,
	)

	l.server.RegisterMethodHandler(
		pb.Method_CannotCommit,
		l.handleCannotCommit,
	)

	l.server.RegisterMethodHandler(
		pb.Method_CheckOrphan,
		l.handleCheckOrphan,
	)

	l.server.RegisterMethodHandler(
		pb.Method_ResumeInvalidCN,
		l.handleResumeInvalidCN,
	)
}

func (l *lockTableAllocator) handleGetBind(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	if !l.canGetBind(req.GetBind.ServiceID) {
		writeResponse(l.logger, cancel, resp, moerr.NewNewTxnInCNRollingRestart(), cs)
		return
	}
	resp.GetBind.LockTable = l.Get(
		req.GetBind.ServiceID,
		req.GetBind.Group,
		req.GetBind.Table,
		req.GetBind.OriginTable,
		req.GetBind.Sharding)
	writeResponse(l.logger, cancel, resp, nil, cs)
}

func (l *lockTableAllocator) handleKeepLockTableBind(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	resp.KeepLockTableBind.OK = l.KeepLockTableBind(req.KeepLockTableBind.ServiceID)
	if !resp.KeepLockTableBind.OK {
		// resp.KeepLockTableBind.Status = pb.Status_ServiceCanRestart
		writeResponse(l.logger, cancel, resp, nil, cs)
		return
	}
	b := l.getServiceBinds(req.KeepLockTableBind.ServiceID)
	if b.isStatus(pb.Status_ServiceLockEnable) {
		if req.KeepLockTableBind.Status != pb.Status_ServiceLockEnable {
			l.logger.Error("tn has abnormal lock service status",
				zap.String("serviceID", b.serviceID),
				zap.String("status", req.KeepLockTableBind.Status.String()))
		} else {
			writeResponse(l.logger, cancel, resp, nil, cs)
			return
		}
	}
	b.setTxnIds(req.KeepLockTableBind.TxnIDs)
	switch req.KeepLockTableBind.Status {
	case pb.Status_ServiceLockEnable:
		if b.isStatus(pb.Status_ServiceLockWaiting) {
			resp.KeepLockTableBind.Status = pb.Status_ServiceLockWaiting
		}
	case pb.Status_ServiceUnLockSucc:
		b.disable()
		l.disableTableBindsWithoutDelete(b)
		b.setStatus(pb.Status_ServiceCanRestart)
		resp.KeepLockTableBind.Status = pb.Status_ServiceCanRestart
	default:
		b.setStatus(req.KeepLockTableBind.Status)
		resp.KeepLockTableBind.Status = req.KeepLockTableBind.Status
	}
	l.disableGroupTables(req.KeepLockTableBind.LockTables, b)
	writeResponse(l.logger, cancel, resp, nil, cs)
}

func (l *lockTableAllocator) handleSetRestartService(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	l.setRestartService(req.SetRestartService.ServiceID)
	resp.SetRestartService.OK = true
	writeResponse(l.logger, cancel, resp, nil, cs)
}

func (l *lockTableAllocator) handleCanRestartService(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	resp.CanRestartService.OK = l.canRestartService(req.CanRestartService.ServiceID)
	writeResponse(l.logger, cancel, resp, nil, cs)
}

func (l *lockTableAllocator) handleRemainTxnInService(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	resp.RemainTxnInService.RemainTxn = l.remainTxnInService(req.RemainTxnInService.ServiceID)
	writeResponse(l.logger, cancel, resp, nil, cs)
}

func (l *lockTableAllocator) getLockTablesLocked(group uint32) map[uint64]pb.LockTable {
	m, ok := l.mu.lockTables[group]
	if ok {
		return m
	}
	m = make(map[uint64]pb.LockTable, 128)
	l.mu.lockTables[group] = m
	return m
}

func (l *lockTableAllocator) handleCannotCommit(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	committingTxn := l.AddCannotCommit(req.CannotCommit.OrphanTxnList)
	resp.CannotCommit.CommittingTxn = committingTxn
	writeResponse(l.logger, cancel, resp, nil, cs)
}

func (l *lockTableAllocator) handleCheckOrphan(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession) {
	c := l.getCtl(req.CheckOrphan.ServiceID)
	state, ok := c.getCtlState(util.UnsafeBytesToString(req.CheckOrphan.Txn))
	resp.CheckOrphan.Orphan = ok && state == cannotCommitState
	writeResponse(l.logger, cancel, resp, nil, cs)
}

func (l *lockTableAllocator) handleResumeInvalidCN(
	ctx context.Context,
	cancel context.CancelFunc,
	req *pb.Request,
	resp *pb.Response,
	cs morpc.ClientSession,
) {
	l.inactiveService.Delete(req.ResumeInvalidCN.ServiceID)
	writeResponse(l.logger, cancel, resp, nil, cs)
}

func (l *lockTableAllocator) getCtl(serviceID string) *commitCtl {
	if v, ok := l.ctl.Load(serviceID); ok {
		return v.(*commitCtl)
	}

	v := &commitCtl{}
	old, loaded := l.ctl.LoadOrStore(serviceID, v)
	if loaded {
		return old.(*commitCtl)
	}
	return v
}

func validateService(
	timeout time.Duration,
	serviceID string,
	client Client,
	logger *log.MOLogger,
) (bool, error) {
	if timeout < defaultRPCTimeout {
		timeout = defaultRPCTimeout
	}
	ctx, cancel := context.WithTimeoutCause(context.Background(), timeout, moerr.CauseValidateService)
	defer cancel()

	req := acquireRequest()
	defer releaseRequest(req)

	req.Method = pb.Method_ValidateService
	req.ValidateService.ServiceID = serviceID

	resp, err := client.Send(ctx, req)
	if err != nil {
		err = moerr.AttachCause(ctx, err)
		logPingFailed(logger, serviceID, err)
		return false, err
	}
	defer releaseResponse(resp)

	return resp.ValidateService.OK, nil
}

type ctlState int

var (
	committingState   = ctlState(0)
	cannotCommitState = ctlState(1)
)

type commitCtl struct {
	// txn id -> state, 0: cannot commit, 1: committed
	states sync.Map
}

func (c *commitCtl) add(txnID string, state ctlState) ctlState {
	old, loaded := c.states.LoadOrStore(txnID, state)
	if loaded {
		return old.(ctlState)
	}
	return state
}

func (c *commitCtl) getCtlState(
	txnID string,
) (ctlState, bool) {
	old, loaded := c.states.Load(txnID)
	if loaded {
		return old.(ctlState), true
	}
	return ctlState(0), false
}

func (l *lockTableAllocator) canGetBind(serviceID string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()

	b := l.mu.services[serviceID]
	if b != nil &&
		!b.isStatus(pb.Status_ServiceLockEnable) {
		return false
	}

	return true
}

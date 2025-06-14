// Copyright 2021 Matrix Origin
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

package model

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/matrixorigin/matrixone/pkg/common/moerr"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	v2 "github.com/matrixorigin/matrixone/pkg/util/metric/v2"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
)

const (
	TransferDir = "transfer"
)

type HashPageTable = TransferTable[*TransferHashPage]

var (
	ttl      = 5 * time.Second
	diskTTL  = 5 * time.Minute
	ttlLatch sync.RWMutex

	backgroundGCTTL = 10 * time.Minute
)

func init() {
	fileservice.RegisterAppConfig(&fileservice.AppConfig{
		Name: TransferDir,
		GCFn: TransferFileGCFn,
	})
}

func SetTTL(t time.Duration) {
	ttlLatch.Lock()
	defer ttlLatch.Unlock()
	ttl = t
}

func SetDiskTTL(t time.Duration) {
	ttlLatch.Lock()
	defer ttlLatch.Unlock()
	diskTTL = t
}

func GetTTL() time.Duration {
	ttlLatch.RLock()
	defer ttlLatch.RUnlock()
	return ttl
}

func GetDiskTTL() time.Duration {
	ttlLatch.RLock()
	defer ttlLatch.RUnlock()
	return diskTTL
}

type Path struct {
	Name   string
	Offset int64
	Size   int64
}

type TransferHashPage struct {
	common.RefHelper
	bornTS      atomic.Pointer[time.Time]
	id          *common.ID // not include blk offset
	objects     []*objectio.ObjectId
	hashmap     atomic.Pointer[api.TransferMap]
	path        Path
	isTransient bool
	fs          fileservice.FileService
	ttl         time.Duration
	diskTTL     time.Duration
}

func GetTransferFileName() string {
	name := objectio.BuildObjectName(objectio.NewSegmentid(), 0)
	now := time.Now()
	return fmt.Sprintf("%v_%v", now.Format("2006-01-02.15.04.05.MST"), name.String())
}

func DecodeTransferFileName(name string) (time.Time, error) {
	strs := strings.Split(name, "_")
	createTime, err := time.Parse("2006-01-02.15.04.05.MST", strs[0])
	return createTime, err
}

func TransferFileGCFn(filePath string, fs fileservice.FileService) (neesGC bool, err error) {
	createTime, err := DecodeTransferFileName(filePath)
	if err != nil {
		return
	}
	if time.Since(createTime) > backgroundGCTTL {
		neesGC = true
		ctx := context.Background()
		ctx, cancel := context.WithTimeoutCause(ctx, 5*time.Second, moerr.CauseClearPersistTable)
		defer cancel()
		if err = fs.Delete(ctx, filePath); err != nil {
			return
		}
		return
	}
	return
}

func GetTransferFS(fs *fileservice.TmpFileService) (fileservice.FileService, error) {
	return fs.GetOrCreateApp(
		&fileservice.AppConfig{
			Name: "transfer",
			GCFn: TransferFileGCFn,
		},
	)
}

func NewTransferHashPage(id *common.ID, ts time.Time, isTransient bool, fs *fileservice.TmpFileService, ttl, diskTTL time.Duration, createdObjIDs []*objectio.ObjectId) *TransferHashPage {

	transferFS, err := GetTransferFS(fs)
	if err != nil {
		panic(err)
	}
	page := &TransferHashPage{
		id:          id,
		isTransient: isTransient,
		fs:          transferFS,
		ttl:         ttl,
		diskTTL:     diskTTL,
		objects:     createdObjIDs,
	}

	page.bornTS.Store(&ts)
	page.OnZeroCB = page.Close

	return page
}

func (page *TransferHashPage) ID() *common.ID         { return page.id }
func (page *TransferHashPage) BornTS() time.Time      { return *page.bornTS.Load() }
func (page *TransferHashPage) SetBornTS(ts time.Time) { page.bornTS.Store(&ts) }

const (
	notClear    = uint8(0)
	clearMemory = uint8(1)
	clearDisk   = uint8(2)
)

func (page *TransferHashPage) TTL() uint8 {
	now := time.Now()
	if now.After(page.bornTS.Load().Add(page.diskTTL)) {
		return clearDisk
	}
	if now.After(page.bornTS.Load().Add(page.ttl)) {
		return clearMemory
	}
	return notClear
}

func (page *TransferHashPage) Close() {
	logutil.Debugf("Closing %s", page.String())
	page.ClearPersistTable()
	page.hashmap.Store(nil)
}

func (page *TransferHashPage) Length() int {
	m := page.hashmap.Load()
	if m == nil {
		return 0
	}
	return len(*m)
}

func (page *TransferHashPage) String() string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("hashpage[%s][%s][Len=%d]",
		page.id.BlockString(),
		page.bornTS.Load().String(),
		page.Length()))
	return w.String()
}

func (page *TransferHashPage) Pin() *common.PinnedItem[*TransferHashPage] {
	page.Ref()
	return &common.PinnedItem[*TransferHashPage]{
		Val: page,
	}
}

func (page *TransferHashPage) Clear() {
	m := page.hashmap.Load()
	if m == nil {
		return
	}
	if page.bornTS.Load().Add(page.ttl).After(time.Now()) {
		return
	}
	page.hashmap.Store(nil)
	v2.TaskMergeTransferPageLengthGauge.Sub(float64(len(*m)))
}

func (page *TransferHashPage) Train(m api.TransferMap) {
	page.hashmap.Store(&m)
	v2.TransferPageRowHistogram.Observe(float64(len(m)))
}

func (page *TransferHashPage) Transfer(from uint32) (dest types.Rowid, ok bool) {
	m := page.hashmap.Load()
	if m == nil {
		diskStart := time.Now()
		m = page.loadTable()
		diskDuration := time.Since(diskStart)
		v2.TransferDiskLatencyHistogram.Observe(diskDuration.Seconds())
	}
	v2.TransferPageTotalHitHistogram.Observe(1)

	memStart := time.Now()
	var data api.TransferDestPos
	if m == nil {
		ok = false
		return
	}
	data, ok = (*m)[from]
	if ok {
		objID := page.objects[data.ObjIdx]
		dest = objectio.NewRowIDWithObjectIDBlkNumAndRowID(*objID, data.BlkIdx, data.RowIdx)
	}
	memDuration := time.Since(memStart)
	v2.TransferMemLatencyHistogram.Observe(memDuration.Seconds())
	return
}

func (page *TransferHashPage) Marshal() []byte {
	m := page.hashmap.Load()
	if m == nil {
		panic("empty hashmap")
	}

	b := new(bytes.Buffer)
	size := uint64(len(*m))
	if size == 0 {
		return nil
	}
	marshalSize := 8 + size*(4+1+2+4)
	b.Grow(int(marshalSize))
	b.Write(types.EncodeUint64(&size))
	for k, v := range *m {
		b.Write(types.EncodeUint32(&k))
		b.Write(types.EncodeUint8(&v.ObjIdx))
		b.Write(types.EncodeUint16(&v.BlkIdx))
		b.Write(types.EncodeUint32(&v.RowIdx))
	}
	return b.Bytes()
}

func (page *TransferHashPage) Unmarshal(data []byte) (*api.TransferMap, error) {
	if len(data) == 0 {
		emptyMap := make(api.TransferMap)
		return &emptyMap, nil
	}
	b := bytes.NewBuffer(data)
	size := types.DecodeUint64(b.Next(8))
	transferMap := make(api.TransferMap, size)
	for b.Len() != 0 {
		k := types.DecodeUint32(b.Next(4))
		vO := types.DecodeUint8(b.Next(1))
		vB := types.DecodeUint16(b.Next(2))
		vR := types.DecodeUint32(b.Next(4))

		transferMap[k] = api.TransferDestPos{
			ObjIdx: vO,
			BlkIdx: vB,
			RowIdx: vR,
		}
	}
	return &transferMap, nil
}

func (page *TransferHashPage) SetPath(path Path) {
	page.path = path
}

func (page *TransferHashPage) loadTable() *api.TransferMap {
	if page.path.Name == "" {
		return nil
	}

	path := page.path
	name, offset, size := path.Name, path.Offset, path.Size

	ioVector := fileservice.IOVector{
		FilePath: name,
		Entries:  make([]fileservice.IOEntry, 0),
	}

	entry := fileservice.IOEntry{
		Offset: offset,
		Size:   size,
	}
	ioVector.Entries = append(ioVector.Entries, entry)
	m := page.hashmap.Load()
	if m != nil {
		return m
	}
	ctx := context.Background()
	ctx, cancel := context.WithTimeoutCause(ctx, 5*time.Second, moerr.CauseLoadTable)
	defer cancel()
	err := page.fs.Read(ctx, &ioVector)
	if err != nil {
		err = moerr.AttachCause(ctx, err)
		logutil.Errorf("[TransferPage] read persist table %v: %v", page.path.Name, err)
		return nil
	}
	defer ioVector.Release()

	m = page.hashmap.Load()
	if m != nil {
		return m
	}
	m, err = page.Unmarshal(ioVector.Entries[0].Data)
	if err != nil {
		return nil
	}
	now := time.Now()
	page.bornTS.Store(&now)
	page.hashmap.Store(m)
	logutil.Infof("[TransferPage] load transfer page %v", page.String())
	v2.TaskMergeTransferPageLengthGauge.Add(float64(page.Length()))
	return m
}

func (page *TransferHashPage) ClearPersistTable() {
	if page.path.Name == "" {
		return
	}
	ctx := context.Background()
	ctx, cancel := context.WithTimeoutCause(ctx, 5*time.Second, moerr.CauseClearPersistTable)
	defer cancel()
	err := page.fs.Delete(ctx, page.path.Name)
	if err != nil {
		err = moerr.AttachCause(ctx, err)
		logutil.Errorf("[TransferPage] clear transfer table %v: %v", page.path.Name, err)
	}
}

func (page *TransferHashPage) IsPersist() bool {
	return page.hashmap.Load() == nil
}

func InitTransferPageIO() *fileservice.IOVector {
	return &fileservice.IOVector{
		FilePath: GetTransferFileName(),
	}
}

func AddTransferPage(page *TransferHashPage, ioVector *fileservice.IOVector) error {
	data := page.Marshal()
	le := len(ioVector.Entries)
	offset := int64(0)
	if le > 0 {
		offset = ioVector.Entries[le-1].Offset +
			ioVector.Entries[le-1].Size
	}
	ioEntry := fileservice.IOEntry{
		Offset: offset,
		Size:   int64(len(data)),
		Data:   data,
	}
	ioVector.Entries = append(ioVector.Entries, ioEntry)

	return nil
}

func WriteTransferPage(ctx context.Context, fs fileservice.FileService, pages []*TransferHashPage, ioVector fileservice.IOVector) {
	ctx, cancel := context.WithTimeoutCause(ctx, 5*time.Second, moerr.CauseWriteTransferPage)
	defer cancel()
	err := fs.Write(ctx, ioVector)
	if err != nil {
		for _, page := range pages {
			page.SetBornTS(page.BornTS().Add(time.Minute))
		}
		logutil.Errorf("[TransferPage] write transfer page error, page count %v", len(pages))
	}
	for i, page := range pages {
		path := Path{
			Name:   ioVector.FilePath,
			Offset: ioVector.Entries[i].Offset,
			Size:   ioVector.Entries[i].Size,
		}
		page.SetPath(path)
	}
}

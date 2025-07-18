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

package testutil

import (
	"context"
	"fmt"
	"sync"
	"testing"

	pkgcatalog "github.com/matrixorigin/matrixone/pkg/catalog"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/fileservice"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/objectio"
	"github.com/matrixorigin/matrixone/pkg/objectio/ioutil"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/db"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/handle"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/iface/txnif"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/index"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/tables/jobs"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/txn/txnimpl"

	"github.com/panjf2000/ants/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func WithTestAllPKType(t *testing.T, tae *db.DB, test func(*testing.T, *db.DB, *catalog.Schema)) {
	var wg sync.WaitGroup
	pool, _ := ants.NewPool(100)
	defer pool.Release()
	for i := 0; i < 17; i++ {
		schema := catalog.MockSchemaAll(18, i)
		schema.Extra.BlockMaxRows = 10
		schema.Extra.ObjectMaxBlocks = 2
		wg.Add(1)
		_ = pool.Submit(func() {
			defer wg.Done()
			test(t, tae, schema)
		})
	}
	wg.Wait()
}

func LenOfBats(bats []*containers.Batch) int {
	rows := 0
	for _, bat := range bats {
		rows += bat.Length()
	}
	return rows
}

func PrintCheckpointStats(t *testing.T, tae *db.DB) {
	t.Logf("GetCheckpointedLSN: %d", tae.Wal.GetCheckpointed())
	t.Logf("GetPenddingLSNCnt: %d", tae.Wal.GetPenddingCnt())
}

func CreateDB(t *testing.T, e *db.DB, dbName string) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.CreateDatabase(dbName, "", "")
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}

func DropDB(t *testing.T, e *db.DB, dbName string) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	_, err = txn.DropDatabase(dbName)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(context.Background()))
}

func CreateRelation(t *testing.T, e *db.DB, dbName string, schema *catalog.Schema, createDB bool) (db handle.Database, rel handle.Relation) {
	txn, db, rel := CreateRelationNoCommit(t, e, dbName, schema, createDB)
	assert.NoError(t, txn.Commit(context.Background()))
	return
}

func CreateRelationNoCommit(t *testing.T, e *db.DB, dbName string, schema *catalog.Schema, createDB bool) (txn txnif.AsyncTxn, db handle.Database, rel handle.Relation) {
	txn, err := e.StartTxn(nil)
	assert.NoError(t, err)
	if createDB {
		db, err = txn.CreateDatabase(dbName, "", "")
		assert.NoError(t, err)
	} else {
		db, err = txn.GetDatabase(dbName)
		assert.NoError(t, err)
	}
	rel, err = db.CreateRelation(schema)
	assert.NoError(t, err)
	return
}

func makeRespBatchFromSchema(schema *catalog.Schema) *containers.Batch {
	bat := containers.NewBatch()
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(attr, containers.MakeVector(typs[i], common.CheckpointAllocator))
	}
	return bat
}

func DropDatabase2(ctx context.Context, txn txnif.AsyncTxn, dbName string) error {
	db, err := txn.DropDatabase(dbName)
	if err != nil {
		return err
	}

	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeUint32(txn.GetTenantID())
	packer.EncodeStringType([]byte(db.GetName()))

	catalogdb, err := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	if err != nil {
		return err
	}
	dbHandle, err := catalogdb.GetRelationByID(pkgcatalog.MO_DATABASE_ID)
	if err != nil {
		return err
	}
	return dbHandle.DeleteByFilter(ctx, handle.NewEQFilter(packer.Bytes()))
}

func CreateDatabase2Ext(ctx context.Context, txn txnif.AsyncTxn, dbName, createsql, dattype string) (handle.Database, error) {
	db, err := txn.CreateDatabase(dbName, createsql, dattype)
	if err != nil {
		return nil, err
	}
	catalogdb, err := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	if err != nil {
		return nil, err
	}
	dbHandle, err := catalogdb.GetRelationByID(pkgcatalog.MO_DATABASE_ID)
	if err != nil {
		return nil, err
	}
	bat := makeRespBatchFromSchema(catalog.SystemDBSchema)
	for _, def := range catalog.SystemDBSchema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		txnimpl.FillDBRow(db.GetMeta().(*catalog.DBEntry), def.Name, bat.Vecs[def.Idx])
	}

	if err := dbHandle.Append(ctx, bat); err != nil {
		return nil, err
	}
	return db, nil
}

func CreateDatabase2(ctx context.Context, txn txnif.AsyncTxn, dbName string) (handle.Database, error) {
	return CreateDatabase2Ext(ctx, txn, dbName, "", "")
}

func DropRelation2(ctx context.Context, txn txnif.AsyncTxn, db handle.Database, tblName string) error {
	rel, err := db.DropRelationByName(tblName)
	if err != nil {
		return err
	}
	catalogdb, err := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	if err != nil {
		return err
	}
	tblHandle, err := catalogdb.GetRelationByID(pkgcatalog.MO_TABLES_ID)
	if err != nil {
		return err
	}
	colHandle, err := catalogdb.GetRelationByID(pkgcatalog.MO_COLUMNS_ID)
	if err != nil {
		return err
	}
	packer := types.NewPacker()
	defer packer.Close()
	packer.EncodeUint32(txn.GetTenantID())
	packer.EncodeStringType([]byte(db.GetName()))
	packer.EncodeStringType([]byte(tblName))
	if err := tblHandle.DeleteByFilter(ctx, handle.NewEQFilter(packer.Bytes())); err != nil {
		return err
	}

	for _, col := range rel.Schema(false).(*catalog.Schema).ColDefs {
		packer.Reset()
		packer.EncodeUint32(txn.GetTenantID())
		packer.EncodeStringType([]byte(db.GetName()))
		packer.EncodeStringType([]byte(tblName))
		packer.EncodeStringType([]byte(col.Name))
		if err := colHandle.DeleteByFilter(ctx, handle.NewEQFilter(packer.Bytes())); err != nil {
			return err
		}
	}
	return nil
}

func CreateRelation2(ctx context.Context, txn txnif.AsyncTxn, db handle.Database, schema *catalog.Schema) (handle.Relation, error) {
	rel, err := db.CreateRelation(schema)
	if err != nil {
		return nil, err
	}
	relEntry := rel.GetMeta().(*catalog.TableEntry)
	catalogdb, err := txn.GetDatabaseByID(pkgcatalog.MO_CATALOG_ID)
	if err != nil {
		return nil, err
	}
	tblHandle, err := catalogdb.GetRelationByID(pkgcatalog.MO_TABLES_ID)
	if err != nil {
		return nil, err
	}
	colHandle, err := catalogdb.GetRelationByID(pkgcatalog.MO_COLUMNS_ID)
	if err != nil {
		return nil, err
	}

	bat := makeRespBatchFromSchema(catalog.SystemTableSchema)
	for _, def := range catalog.SystemTableSchema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		txnimpl.FillTableRow(relEntry, schema, def.Name, bat.Vecs[def.Idx])
	}
	if err := tblHandle.Append(ctx, bat); err != nil {
		return nil, err
	}
	colBat := makeRespBatchFromSchema(catalog.SystemColumnSchema)
	for _, def := range catalog.SystemColumnSchema.ColDefs {
		if def.IsPhyAddr() {
			continue
		}
		txnimpl.FillColumnRow(relEntry, schema, def.Name, colBat.Vecs[def.Idx])
	}

	if err := colHandle.Append(ctx, colBat); err != nil {
		return nil, err
	}
	return rel, nil
}

func CreateRelationAndAppend2(
	t *testing.T,
	tenantID uint32,
	e *db.DB,
	dbName string,
	schema *catalog.Schema,
	bat *containers.Batch,
	createDB bool) {
	ctx := context.Background()
	txn, err := e.StartTxn(nil)
	txn.BindAccessInfo(tenantID, 0, 0)
	require.NoError(t, err)
	var db handle.Database
	if createDB {
		db, err = CreateDatabase2(ctx, txn, dbName)
		require.NoError(t, err)
	} else {
		db, err = txn.GetDatabase(dbName)
		require.NoError(t, err)
	}
	rel, err := CreateRelation2(ctx, txn, db, schema)
	require.NoError(t, err)
	err = rel.Append(context.Background(), bat)
	require.NoError(t, err)
	require.Nil(t, txn.Commit(context.Background()))
}
func AllCheckpointsFinished(e *db.DB) bool {
	if e.Wal.GetPenddingCnt() != 0 {
		return false
	}
	ckp := e.BGCheckpointRunner.GetICKPIntentOnlyForTest()
	if ckp == nil {
		return true
	}
	return ckp.IsFinished()
}
func CreateRelationAndAppend(
	t *testing.T,
	tenantID uint32,
	e *db.DB,
	dbName string,
	schema *catalog.Schema,
	bat *containers.Batch,
	createDB bool) {
	txn, err := e.StartTxn(nil)
	txn.BindAccessInfo(tenantID, 0, 0)
	assert.NoError(t, err)
	var db handle.Database
	if createDB {
		db, err = txn.CreateDatabase(dbName, "", "")
		assert.NoError(t, err)
	} else {
		db, err = txn.GetDatabase(dbName)
		assert.NoError(t, err)
	}
	rel, err := db.CreateRelation(schema)
	assert.NoError(t, err)
	err = rel.Append(context.Background(), bat)
	assert.NoError(t, err)
	assert.Nil(t, txn.Commit(context.Background()))
}

func GetRelation(t *testing.T, tenantID uint32, e *db.DB, dbName, tblName string) (txn txnif.AsyncTxn, rel handle.Relation) {
	txn, err := e.StartTxn(nil)
	require.NoError(t, err)
	txn.BindAccessInfo(tenantID, 0, 0)
	assert.NoError(t, err)
	db, err := txn.GetDatabase(dbName)
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(tblName)
	assert.NoError(t, err)
	return
}

func GetRelationWithTxn(t *testing.T, txn txnif.AsyncTxn, dbName, tblName string) (rel handle.Relation) {
	db, err := txn.GetDatabase(dbName)
	assert.NoError(t, err)
	rel, err = db.GetRelationByName(tblName)
	assert.NoError(t, err)
	return
}

func GetDefaultRelation(t *testing.T, e *db.DB, name string) (txn txnif.AsyncTxn, rel handle.Relation) {
	return GetRelation(t, 0, e, DefaultTestDB, name)
}

// GetOneObject returns the newest visible object in the relation
func GetOneObject(rel handle.Relation) handle.Object {
	it := rel.MakeObjectIt(false)
	it.Next()
	defer it.Close()
	return it.GetObject()
}

// GetOneBlockMeta returns the oldest visible object's meta in the relation
func GetOneBlockMeta(rel handle.Relation) *catalog.ObjectEntry {
	it := rel.MakeObjectIt(false)
	for it.Next() {
	}
	defer it.Close()
	return it.GetObject().GetMeta().(*catalog.ObjectEntry)
}

func GetOneTombstoneMeta(rel handle.Relation) *catalog.ObjectEntry {
	it := rel.MakeObjectIt(true)
	it.Next()
	it.Close()
	return it.GetObject().GetMeta().(*catalog.ObjectEntry)
}

func GetAllBlockMetas(rel handle.Relation, isTombstone bool) (metas []*catalog.ObjectEntry) {
	it := rel.MakeObjectIt(isTombstone)
	for it.Next() {
		blk := it.GetObject()
		metas = append(metas, blk.GetMeta().(*catalog.ObjectEntry))
	}
	it.Close()
	return
}
func GetAllAppendableMetas(rel handle.Relation, isTombstone bool) (metas []*catalog.ObjectEntry) {
	it := rel.MakeObjectIt(isTombstone)
	for it.Next() {
		blk := it.GetObject()
		meta := blk.GetMeta().(*catalog.ObjectEntry)
		if !meta.IsAppendable() {
			continue
		}
		if meta.HasDropCommitted() {
			continue
		}
		metas = append(metas, meta)
	}
	return
}

func MockObjectStats(t *testing.T, obj handle.Object) {
	objName := objectio.BuildObjectNameWithObjectID(obj.GetID())
	location := objectio.MockLocation(objName)
	stats := objectio.NewObjectStats()
	objectio.SetObjectStatsLocation(stats, location)
	objectio.SetObjectStatsSize(stats, 1)
	err := obj.UpdateStats(*stats)
	assert.Nil(t, err)
}

func CheckAllColRowsByScan(t *testing.T, rel handle.Relation, expectRows int, applyDelete bool) {
	schema := rel.Schema(false).(*catalog.Schema)
	for _, def := range schema.ColDefs {
		rows := GetColumnRowsByScan(t, rel, def.Idx, applyDelete)
		assert.Equal(t, expectRows, rows)
	}
}

func GetColumnRowsByScan(t *testing.T, rel handle.Relation, colIdx int, applyDelete bool) int {
	rows := 0
	ForEachColumnView(t, rel, colIdx, func(view *containers.Batch) (err error) {
		if applyDelete {
			view.Compact()
		}
		rows += view.Length()
		// t.Log(view.String())
		return
	})
	return rows
}

func ForEachColumnView(t *testing.T, rel handle.Relation, colIdx int, fn func(view *containers.Batch) error) {
	ForEachObject(t, rel, func(blk handle.Object) (err error) {
		blkCnt := blk.GetMeta().(*catalog.ObjectEntry).BlockCnt()
		for i := 0; i < blkCnt; i++ {
			var view *containers.Batch
			err := blk.HybridScan(context.Background(), &view, uint16(i), []int{colIdx}, common.DefaultAllocator)
			if err != nil {
				t.Errorf("blk %v, %v", blk.String(), err)
				return err
			}
			if view == nil {
				logutil.Warnf("blk %v", blk.String())
				continue
			}
			defer view.Close()
			err = fn(view)
			if err != nil {
				return err
			}
		}
		return
	})
}

func ForEachObject(t *testing.T, rel handle.Relation, fn func(obj handle.Object) error) {
	forEachObject(t, rel, fn, false)
}
func ForEachTombstone(t *testing.T, rel handle.Relation, fn func(obj handle.Object) error) {
	forEachObject(t, rel, fn, true)
}

func forEachObject(t *testing.T, rel handle.Relation, fn func(obj handle.Object) error, isTombstone bool) {
	it := rel.MakeObjectIt(isTombstone)
	var err error
	for it.Next() {
		obj := it.GetObject()
		defer obj.Close()
		if err = fn(obj); err != nil {
			t.Error(err)
			t.FailNow()
		}
	}
}

func AppendFailClosure(t *testing.T, data *containers.Batch, name string, e *db.DB, wg *sync.WaitGroup) func() {
	return func() {
		if wg != nil {
			defer wg.Done()
		}
		txn, _ := e.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(name)
		err := rel.Append(context.Background(), data)
		assert.NotNil(t, err)
		assert.Nil(t, txn.Rollback(context.Background()))
	}
}

func AppendClosure(t *testing.T, data *containers.Batch, name string, e *db.DB, wg *sync.WaitGroup) func() {
	return func() {
		if wg != nil {
			defer wg.Done()
		}
		txn, _ := e.StartTxn(nil)
		database, _ := txn.GetDatabase("db")
		rel, _ := database.GetRelationByName(name)
		err := rel.Append(context.Background(), data)
		assert.Nil(t, err)
		assert.Nil(t, txn.Commit(context.Background()))
	}
}

func CompactBlocks(t *testing.T, tenantID uint32, e *db.DB, dbName string, schema *catalog.Schema, skipConflict bool) {
	txn, rel := GetRelation(t, tenantID, e, dbName, schema.Name)

	metas := GetAllAppendableMetas(rel, false)
	tombstones := GetAllAppendableMetas(rel, true)
	assert.NoError(t, txn.Commit(context.Background()))
	if len(metas) == 0 && len(tombstones) == 0 {
		return
	}
	txn, _ = GetRelation(t, tenantID, e, dbName, schema.Name)
	task, err := jobs.NewFlushTableTailTask(nil, txn, metas, tombstones, e.Runtime)
	if skipConflict && err != nil {
		_ = txn.Rollback(context.Background())
		return
	}
	assert.NoError(t, err)
	err = task.OnExec(context.Background())
	if skipConflict {
		if err != nil {
			_ = txn.Rollback(context.Background())
		} else {
			_ = txn.Commit(context.Background())
		}
	} else {
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}
}

func MergeBlocks(t *testing.T, tenantID uint32, e *db.DB, dbName string, schema *catalog.Schema, skipConflict bool) {
	mergeBlocks(t, tenantID, e, dbName, schema, skipConflict, false)
	mergeBlocks(t, tenantID, e, dbName, schema, skipConflict, true)
}
func mergeBlocks(t *testing.T, tenantID uint32, e *db.DB, dbName string, schema *catalog.Schema, skipConflict, isTombstone bool) {
	txn, _ := e.StartTxn(nil)
	txn.BindAccessInfo(tenantID, 0, 0)
	db, _ := txn.GetDatabase(dbName)
	rel, _ := db.GetRelationByName(schema.Name)

	var objs []*catalog.ObjectEntry
	objIt := rel.MakeObjectIt(isTombstone)
	for objIt.Next() {
		obj := objIt.GetObject().GetMeta().(*catalog.ObjectEntry)
		if !obj.IsAppendable() {
			objs = append(objs, obj)
		}
	}
	_ = txn.Commit(context.Background())
	metas := make([]*catalog.ObjectEntry, 0)
	for _, obj := range objs {
		txn, _ = e.StartTxn(nil)
		txn.BindAccessInfo(tenantID, 0, 0)
		db, _ = txn.GetDatabase(dbName)
		rel, _ = db.GetRelationByName(schema.Name)
		objHandle, err := rel.GetObject(obj.ID(), isTombstone)
		if err != nil {
			if skipConflict {
				continue
			} else {
				assert.NoErrorf(t, err, "Txn Ts=%d", txn.GetStartTS())
			}
		}
		metas = append(metas, objHandle.GetMeta().(*catalog.ObjectEntry))
	}
	if len(metas) == 0 {
		t.Logf("no objects to merge, type %v", isTombstone)
		return
	}
	task, err := jobs.NewMergeObjectsTask(nil, txn, metas, e.Runtime, 0, isTombstone)
	if skipConflict && err != nil {
		_ = txn.Rollback(context.Background())
		return
	}
	assert.NoError(t, err)
	err = task.OnExec(context.Background())
	if skipConflict {
		if err != nil {
			_ = txn.Rollback(context.Background())
		} else {
			_ = txn.Commit(context.Background())
		}
	} else {
		assert.NoError(t, err)
		assert.NoError(t, txn.Commit(context.Background()))
	}
}

func GetSingleSortKeyValue(bat *containers.Batch, schema *catalog.Schema, row int) (v any) {
	v = bat.Vecs[schema.GetSingleSortKeyIdx()].Get(row)
	return
}

func MockCNDeleteInS3(
	fs fileservice.FileService,
	rowIDVec containers.Vector,
	pkVec containers.Vector,
	schema *catalog.Schema,
	txn txnif.AsyncTxn,
) (stats objectio.ObjectStats, err error) {
	bat := containers.NewBatch()
	bat.AddVector(objectio.TombstoneAttr_Rowid_Attr, rowIDVec)
	bat.AddVector("pk", pkVec)
	name := objectio.MockObjectName()
	writer, err := ioutil.NewBlockWriterNew(fs, name, 0, nil, true)
	writer.SetPrimaryKeyWithType(uint16(objectio.TombstonePrimaryKeyIdx), index.HBF,
		index.ObjectPrefixFn,
		index.BlockPrefixFn)
	if err != nil {
		return
	}
	_, err = writer.WriteBatch(containers.ToCNBatch(bat))
	if err != nil {
		return
	}
	_, _, err = writer.Sync(context.Background())
	//location = blockio.EncodeLocation(name, blks[0].GetExtent(), uint32(bat.Length()), blks[0].GetID())

	stats = writer.GetObjectStats(objectio.WithCNCreated())

	return
}

func CreateOneDatabase(
	ctx context.Context,
	t *testing.T,
	tae *db.DB,
	i int,
) (name string) {
	txn, err := tae.StartTxn(nil)
	assert.NoError(t, err)
	name = fmt.Sprintf("db_%d", i)
	_, err = CreateDatabase2(ctx, txn, name)
	assert.NoError(t, err)
	assert.NoError(t, txn.Commit(ctx))
	return
}

func IsCatalogEqual(t *testing.T, c1, c2 *catalog.Catalog) {
	p := &catalog.LoopProcessor{}
	objCount := 0
	objFn := func(oe *catalog.ObjectEntry) error {
		objCount++
		dbID := oe.GetTable().GetDB().ID
		db, err := c2.GetDatabaseByID(dbID)
		assert.NoError(t, err)
		tid := oe.GetTable().ID
		tbl, err := db.GetTableEntryByID(tid)
		assert.NoError(t, err)
		oe2, err := tbl.GetObjectByID(oe.ID(), oe.IsTombstone)
		assert.NoError(t, err)
		create2 := oe2.CreatedAt
		assert.True(t, oe.CreatedAt.EQ(&create2))
		delete2 := oe2.DeletedAt
		assert.True(t, oe.DeletedAt.EQ(&delete2))
		return nil
	}
	p.ObjectFn = objFn
	p.TombstoneFn = objFn
	err := c1.RecurLoop(p)
	assert.NoError(t, err)

	objCount2 := 0
	p2 := &catalog.LoopProcessor{}
	p2.ObjectFn = func(oe *catalog.ObjectEntry) error {
		objCount2++
		return nil
	}
	p2.TombstoneFn = func(oe *catalog.ObjectEntry) error {
		objCount2++
		return nil
	}
	err = c2.RecurLoop(p2)
	assert.NoError(t, err)
	assert.Equal(t, objCount, objCount2)
}

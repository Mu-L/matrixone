// Copyright 2024 Matrix Origin
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

package cdc

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tidwall/btree"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/testutil"
)

func TestNewAtomicBatch(t *testing.T) {
	actual := NewAtomicBatch(testutil.TestUtilMp)
	assert.NotNil(t, actual.Rows)
	assert.Equal(t, 0, actual.Rows.Len())
}

func TestAtomicBatch_Append(t *testing.T) {
	atomicBat := &AtomicBatch{
		Batches: []*batch.Batch{},
		Rows:    btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64}),
	}
	bat := batch.New([]string{"pk", "ts"})
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil)
	bat.Vecs[1] = testutil.MakeTSVector([]types.TS{types.BuildTS(1, 1)}, nil)

	atomicBat.Append(types.NewPacker(), bat, 1, 0)
	assert.Equal(t, 1, len(atomicBat.Batches))
	assert.Equal(t, 1, atomicBat.Rows.Len())
}

func TestAtomicBatch_Close(t *testing.T) {
	bat := batch.New([]string{"attr1"})
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil)

	type fields struct {
		Mp      *mpool.MPool
		Batches []*batch.Batch
		Rows    *btree.BTreeG[AtomicBatchRow]
	}
	tests := []struct {
		name   string
		fields fields
	}{
		{
			fields: fields{
				Mp:      testutil.TestUtilMp,
				Batches: []*batch.Batch{bat},
				Rows:    btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64}),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bat := &AtomicBatch{
				Mp:      tt.fields.Mp,
				Batches: tt.fields.Batches,
				Rows:    tt.fields.Rows,
			}
			bat.Close()
		})
	}
}

func TestAtomicBatch_GetRowIterator(t *testing.T) {
	type fields struct {
		Mp      *mpool.MPool
		Batches []*batch.Batch
		Rows    *btree.BTreeG[AtomicBatchRow]
	}
	tests := []struct {
		name   string
		fields fields
		want   RowIterator
	}{
		{
			fields: fields{
				Rows: btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64}),
			},
			want: &atomicBatchRowIter{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bat := &AtomicBatch{
				Mp:      tt.fields.Mp,
				Batches: tt.fields.Batches,
				Rows:    tt.fields.Rows,
			}
			it := bat.GetRowIterator()
			it.Close()
		})
	}
}

func TestAtomicBatchRow_Less(t *testing.T) {
	t1 := types.BuildTS(1, 1)
	t2 := types.BuildTS(2, 1)

	type fields struct {
		Ts     types.TS
		Pk     []byte
		Offset int
		Src    *batch.Batch
	}
	type args struct {
		other AtomicBatchRow
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			fields: fields{Ts: t1},
			args:   args{other: AtomicBatchRow{Ts: t2}},
			want:   true,
		},
		{
			fields: fields{Ts: t2},
			args:   args{other: AtomicBatchRow{Ts: t1}},
			want:   false,
		},
		{
			fields: fields{Ts: t1, Pk: []byte{1}},
			args:   args{other: AtomicBatchRow{Ts: t1, Pk: []byte{2}}},
			want:   true,
		},
		{
			fields: fields{Ts: t1, Pk: []byte{2}},
			args:   args{other: AtomicBatchRow{Ts: t1, Pk: []byte{1}}},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			row := AtomicBatchRow{
				Ts:     tt.fields.Ts,
				Pk:     tt.fields.Pk,
				Offset: tt.fields.Offset,
				Src:    tt.fields.Src,
			}
			assert.Equalf(t, tt.want, row.Less(tt.args.other), "Less(%v)", tt.args.other)
		})
	}
}

func Test_atomicBatchRowIter(t *testing.T) {
	rows := btree.NewBTreeGOptions(AtomicBatchRow.Less, btree.Options{Degree: 64})
	row1 := AtomicBatchRow{Ts: types.BuildTS(1, 1), Pk: []byte{1}}
	row2 := AtomicBatchRow{Ts: types.BuildTS(2, 1), Pk: []byte{2}}
	row3 := AtomicBatchRow{Ts: types.BuildTS(3, 1), Pk: []byte{3}}
	rows.Set(row1)
	rows.Set(row2)
	rows.Set(row3)

	bat := batch.New([]string{"attr1"})
	bat.Vecs[0] = testutil.MakeInt32Vector([]int32{1}, nil)

	// at init position (before the first row)
	iter := &atomicBatchRowIter{
		iter: rows.Iter(),
	}

	// first row
	iter.Next()
	item := iter.Item()
	assert.Equal(t, row1, item)
	err := iter.Row(context.Background(), []any{})
	assert.NoError(t, err)

	// second row
	iter.Next()
	item = iter.Item()
	assert.Equal(t, row2, item)

	// third row
	iter.Next()
	item = iter.Item()
	assert.Equal(t, row3, item)

	assert.False(t, iter.Next())
	iter.Close()
}

func TestDbTableInfo_String(t *testing.T) {
	type fields struct {
		SourceDbName  string
		SourceTblName string
		SourceDbId    uint64
		SourceTblId   uint64
		SinkDbName    string
		SinkTblName   string
		IdChanged     bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			fields: fields{
				SourceDbName:  "source_db",
				SourceDbId:    1,
				SourceTblName: "source_tbl",
				SourceTblId:   1,
				SinkDbName:    "sink_db",
				SinkTblName:   "sink_tbl",
				IdChanged:     false,
			},
			want: "source_db(1).source_tbl(1) -> sink_db.sink_tbl, false",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := DbTableInfo{
				SourceDbName:  tt.fields.SourceDbName,
				SourceTblName: tt.fields.SourceTblName,
				SourceDbId:    tt.fields.SourceDbId,
				SourceTblId:   tt.fields.SourceTblId,
				SinkDbName:    tt.fields.SinkDbName,
				SinkTblName:   tt.fields.SinkTblName,
				IdChanged:     tt.fields.IdChanged,
			}
			assert.Equalf(t, tt.want, info.String(), "String()")
		})
	}
}

func TestDbTableInfo_Clone(t *testing.T) {
	type fields struct {
		SourceDbId      uint64
		SourceDbName    string
		SourceTblId     uint64
		SourceTblName   string
		SourceCreateSql string
		SinkDbName      string
		SinkTblName     string
	}
	tests := []struct {
		name   string
		fields fields
		want   *DbTableInfo
	}{
		{
			fields: fields{
				SourceDbId:      1,
				SourceDbName:    "source_db",
				SourceTblId:     1,
				SourceTblName:   "source_tbl",
				SourceCreateSql: "create table source_db.source_tbl (id int)",
				SinkDbName:      "sink_db",
				SinkTblName:     "sink_tbl",
			},
			want: &DbTableInfo{
				SourceDbId:      1,
				SourceDbName:    "source_db",
				SourceTblId:     1,
				SourceTblName:   "source_tbl",
				SourceCreateSql: "create table source_db.source_tbl (id int)",
				SinkDbName:      "sink_db",
				SinkTblName:     "sink_tbl",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := DbTableInfo{
				SourceDbId:      tt.fields.SourceDbId,
				SourceDbName:    tt.fields.SourceDbName,
				SourceTblId:     tt.fields.SourceTblId,
				SourceTblName:   tt.fields.SourceTblName,
				SourceCreateSql: tt.fields.SourceCreateSql,
				SinkDbName:      tt.fields.SinkDbName,
				SinkTblName:     tt.fields.SinkTblName,
			}
			assert.Equalf(t, tt.want, info.Clone(), "Clone()")
		})
	}
}

func TestDbTableInfo_OnlyDiffinTblId(t *testing.T) {
	type fields struct {
		SourceDbName  string
		SourceTblName string
		SourceDbId    uint64
		SourceTblId   uint64
		SinkDbName    string
		SinkTblName   string
		IdChanged     bool
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{
			fields: fields{
				SourceDbName:  "source_db",
				SourceDbId:    2,
				SourceTblName: "source_tbl",
				SourceTblId:   1,
				SinkDbName:    "sink_db",
				SinkTblName:   "sink_tbl",
				IdChanged:     false,
			},
			want: false,
		},
		{
			fields: fields{
				SourceDbName:  "source_db",
				SourceDbId:    1,
				SourceTblName: "source_tbl",
				SourceTblId:   2,
				SinkDbName:    "sink_db",
				SinkTblName:   "sink_tbl",
				IdChanged:     false,
			},
			want: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			base := DbTableInfo{
				SourceDbName:  tt.fields.SourceDbName,
				SourceTblName: tt.fields.SourceTblName,
				SourceDbId:    1,
				SourceTblId:   1,
				SinkDbName:    tt.fields.SinkDbName,
				SinkTblName:   tt.fields.SinkTblName,
				IdChanged:     tt.fields.IdChanged,
			}
			info := DbTableInfo{
				SourceDbName:  tt.fields.SourceDbName,
				SourceTblName: tt.fields.SourceTblName,
				SourceDbId:    tt.fields.SourceDbId,
				SourceTblId:   tt.fields.SourceTblId,
				SinkDbName:    tt.fields.SinkDbName,
				SinkTblName:   tt.fields.SinkTblName,
				IdChanged:     tt.fields.IdChanged,
			}
			assert.Equalf(t, tt.want, base.OnlyDiffinTblId(&info), "OnlyDiffinTblId()")
		})
	}
}

func TestJsonDecode(t *testing.T) {
	// TODO
	//type args struct {
	//	jbytes string
	//	value  any
	//}
	//tests := []struct {
	//	name      string
	//	args      args
	//	wantValue any
	//	wantErr   assert.ErrorAssertionFunc
	//}{
	//	{
	//		args:      args{jbytes: "7b2261223a317d"},
	//		wantValue: map[string]int{"a": 1},
	//		wantErr:   assert.NoError,
	//	},
	//}
	//for _, tt := range tests {
	//	t.Run(tt.name, func(t *testing.T) {
	//		tt.wantErr(t, JsonDecode(tt.args.jbytes, tt.args.value), fmt.Sprintf("JsonDecode(%v, %v)", tt.args.jbytes, tt.args.value))
	//		assert.Equal(t, tt.wantValue, tt.args.value)
	//	})
	//}
}

func TestJsonEncode(t *testing.T) {
	type args struct {
		value any
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			args:    args{value: map[string]int{"a": 1}},
			want:    "7b2261223a317d",
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := JsonEncode(tt.args.value)
			if !tt.wantErr(t, err, fmt.Sprintf("JsonEncode(%v)", tt.args.value)) {
				return
			}
			assert.Equalf(t, tt.want, got, "JsonEncode(%v)", tt.args.value)
		})
	}
}

func TestOutputType_String(t *testing.T) {
	tests := []struct {
		name string
		t    OutputType
		want string
	}{
		{
			t:    OutputTypeSnapshot,
			want: "Snapshot",
		},
		{
			t:    OutputTypeTail,
			want: "Tail",
		},
		{
			t:    100,
			want: "usp output type",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equalf(t, tt.want, tt.t.String(), "String()")
		})
	}
}

func TestPatternTable_String(t *testing.T) {
	type fields struct {
		Database string
		Table    string
		Reserved bool
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			fields: fields{
				Database: "database",
				Table:    "table",
			},
			want: "database.table",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			table := PatternTable{
				Database: tt.fields.Database,
				Table:    tt.fields.Table,
			}
			assert.Equalf(t, tt.want, table.String(), "String()")
		})
	}
}

func TestPatternTuple_String(t *testing.T) {
	type fields struct {
		Source       PatternTable
		Sink         PatternTable
		OriginString string
		Reserved     string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			fields: fields{
				Source: PatternTable{
					Database: "database1",
					Table:    "table1",
				},
				Sink: PatternTable{
					Database: "database2",
					Table:    "table2",
				},
			},
			want: "database1.table1,database2.table2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tuple := &PatternTuple{
				Source:       tt.fields.Source,
				Sink:         tt.fields.Sink,
				OriginString: tt.fields.OriginString,
				Reserved:     tt.fields.Reserved,
			}
			assert.Equalf(t, tt.want, tuple.String(), "String()")
		})
	}

	var tuple *PatternTuple
	assert.Equalf(t, "", tuple.String(), "String()")
}

func TestPatternTuples_Append(t *testing.T) {
	type fields struct {
		Pts      []*PatternTuple
		Reserved string
	}
	type args struct {
		pt *PatternTuple
	}
	tests := []struct {
		name   string
		fields fields
		args   args
	}{
		{
			fields: fields{
				Pts: []*PatternTuple{},
			},
			args: args{
				pt: &PatternTuple{
					Source: PatternTable{
						Database: "database1",
						Table:    "table1",
					},
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pts := &PatternTuples{
				Pts:      tt.fields.Pts,
				Reserved: tt.fields.Reserved,
			}
			pts.Append(tt.args.pt)
		})
	}
}

func TestPatternTuples_String(t *testing.T) {
	type fields struct {
		Pts      []*PatternTuple
		Reserved string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			fields: fields{
				Pts: nil,
			},
			want: "",
		},
		{
			fields: fields{
				Pts: []*PatternTuple{
					{
						Source: PatternTable{
							Database: "database1",
							Table:    "table1",
						},
						Sink: PatternTable{
							Database: "database2",
							Table:    "table2",
						},
					},
				},
			},
			want: "database1.table1,database2.table2",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			pts := &PatternTuples{
				Pts:      tt.fields.Pts,
				Reserved: tt.fields.Reserved,
			}
			assert.Equalf(t, tt.want, pts.String(), "String()")
		})
	}
}

func TestUriInfo_GetEncodedPassword(t *testing.T) {
	AesKey = "test-aes-key-not-use-it-in-cloud"
	defer func() { AesKey = "" }()

	type fields struct {
		SinkTyp       string
		User          string
		Password      string
		Ip            string
		Port          int
		PasswordStart int
		PasswordEnd   int
		Reserved      string
	}
	tests := []struct {
		name    string
		fields  fields
		want    string
		wantErr assert.ErrorAssertionFunc
	}{
		{
			fields:  fields{Password: "password"},
			want:    "6b66312c27142b570457556a5b11050bb2105255b3d96050",
			wantErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := &UriInfo{
				SinkTyp:       tt.fields.SinkTyp,
				User:          tt.fields.User,
				Password:      tt.fields.Password,
				Ip:            tt.fields.Ip,
				Port:          tt.fields.Port,
				PasswordStart: tt.fields.PasswordStart,
				PasswordEnd:   tt.fields.PasswordEnd,
				Reserved:      tt.fields.Reserved,
			}
			_, err := info.GetEncodedPassword()
			if !tt.wantErr(t, err, "GetEncodedPassword()") {
				return
			}
			// TODO assert equal
			//assert.Equalf(t, tt.want, got, "GetEncodedPassword()")
		})
	}
}

func TestUriInfo_String(t *testing.T) {
	type fields struct {
		SinkTyp       string
		User          string
		Password      string
		Ip            string
		Port          int
		PasswordStart int
		PasswordEnd   int
		Reserved      string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{
			fields: fields{
				User:     "user",
				Password: "password",
				Ip:       "127.0.0.1",
				Port:     3306,
			},
			want: "mysql://user:******@127.0.0.1:3306",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			info := &UriInfo{
				SinkTyp:       tt.fields.SinkTyp,
				User:          tt.fields.User,
				Password:      tt.fields.Password,
				Ip:            tt.fields.Ip,
				Port:          tt.fields.Port,
				PasswordStart: tt.fields.PasswordStart,
				PasswordEnd:   tt.fields.PasswordEnd,
				Reserved:      tt.fields.Reserved,
			}
			assert.Equalf(t, tt.want, info.String(), "String()")
		})
	}
}

func TestActiveRoutine_ClosePause(t *testing.T) {
	ar := NewCdcActiveRoutine()
	ar.ClosePause()
}

func TestActiveRoutine_CloseCancel(t *testing.T) {
	ar := NewCdcActiveRoutine()
	ar.CloseCancel()
}

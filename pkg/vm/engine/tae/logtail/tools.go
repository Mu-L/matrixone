// Copyright 2021 Matrix Origin
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package logtail

import (
	"bytes"
	"fmt"
	"hash/fnv"

	"github.com/matrixorigin/matrixone/pkg/container/batch"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/logutil"
	"github.com/matrixorigin/matrixone/pkg/pb/api"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/catalog"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/common"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/tae/containers"
	"go.uber.org/zap/zapcore"
)

func ToStringTemplate(vec containers.Vector, printN int, opts ...common.TypePrintOpt) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[%d]: ", vec.Length()))
	if printN < 0 || printN > vec.Length() {
		printN = vec.Length()
	}
	first := true
	typ := vec.GetType()
	for i := 0; i < printN; i++ {
		if !first {
			_ = w.WriteByte(',')
		}
		v := vec.Get(i)
		_, _ = w.WriteString(common.TypeStringValue(typ, v, opts...))
		first = false
	}

	return w.String()
}

func DebugBatchToString(name string, bat *containers.Batch, isSpecialRowID bool, lvl zapcore.Level) string {
	if logutil.GetSkip1Logger().Core().Enabled(lvl) {
		return BatchToString(name, bat, isSpecialRowID)
	}
	return "not required level"
}

func BatchToString(name string, bat *containers.Batch, isSpecialRowID bool) string {
	var w bytes.Buffer
	_, _ = w.WriteString(fmt.Sprintf("[BatchName=%s]\n", name))
	for i, vec := range bat.Vecs {
		_, _ = w.WriteString(fmt.Sprintf("(attr=%s)", bat.Attrs[i]))
		if bat.Attrs[i] == catalog.AttrRowID {
			if isSpecialRowID {
				_, _ = w.WriteString(ToStringTemplate(vec, common.PrintN, common.WithSpecialRowid{}))
			} else {
				_, _ = w.WriteString(ToStringTemplate(vec, common.PrintN))
			}
		} else {
			_, _ = w.WriteString(ToStringTemplate(vec, common.PrintN, common.WithDoNotPrintBin{}))
		}
		_ = w.WriteByte('\n')
	}
	return w.String()
}

func u64ToRowID(v uint64) types.Rowid {
	var rowid types.Rowid
	bs := types.EncodeUint64(&v)
	copy(rowid[0:], bs)
	return rowid
}

func blockid2rowid(bid *types.Blockid) types.Rowid {
	var rowid types.Rowid
	copy(rowid[:], bid[:])
	return rowid
}

func segid2rowid(sid *types.Uuid) types.Rowid {
	var rowid types.Rowid
	copy(rowid[:], sid[:])
	return rowid
}

func bytesToRowID(bs []byte) types.Rowid {
	var rowid types.Rowid
	if size := len(bs); size <= types.RowidSize {
		copy(rowid[:size], bs[:size])
	} else {
		hasher := fnv.New128()
		hasher.Write(bs)
		hasher.Sum(rowid[:0])
	}
	return rowid
}

// make batch, append necessary field like commit ts
func makeRespBatchFromSchema(schema *catalog.Schema) *containers.Batch {
	bat := containers.NewBatch()

	bat.AddVector(catalog.AttrRowID, containers.MakeVector(types.T_Rowid.ToType(), false))
	bat.AddVector(catalog.AttrCommitTs, containers.MakeVector(types.T_TS.ToType(), false))
	// Types() is not used, then empty schema can also be handled here
	typs := schema.AllTypes()
	attrs := schema.AllNames()
	nullables := schema.AllNullables()
	for i, attr := range attrs {
		if attr == catalog.PhyAddrColumnName {
			continue
		}
		bat.AddVector(attr, containers.MakeVector(typs[i], nullables[i]))
	}
	return bat
}

// consume containers.Batch to construct api batch
func containersBatchToProtoBatch(bat *containers.Batch) (*api.Batch, error) {
	mobat := containers.CopyToMoBatch(bat)
	return batch.BatchToProtoBatch(mobat)
}

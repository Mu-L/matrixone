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

package sort

import (
	"slices"
	"sort"
	"testing"

	"github.com/matrixorigin/matrixone/pkg/common/mpool"
	"github.com/matrixorigin/matrixone/pkg/container/types"
	"github.com/matrixorigin/matrixone/pkg/container/vector"
	"github.com/matrixorigin/matrixone/pkg/testutil"
	"github.com/matrixorigin/matrixone/pkg/vm/process"
	"github.com/stretchr/testify/require"
)

const (
	Rows          = 15
	BenchmarkRows = 100000
)

type testCase struct {
	desc bool
	vec  *vector.Vector
	proc *process.Process
}

func makeTestCases(t *testing.T) []testCase {
	mp := mpool.MustNewZero()
	return []testCase{
		newTestCase(t, true, mp, types.New(types.T_bool, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_bool, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_bit, 64, 0)),
		newTestCase(t, false, mp, types.New(types.T_bit, 64, 0)),

		newTestCase(t, true, mp, types.New(types.T_int8, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_int8, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_int16, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_int16, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_int32, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_int32, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_int64, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_int64, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_uint8, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_uint8, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_uint16, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_uint16, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_uint32, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_uint32, 0, 0)),
		newTestCase(t, true, mp, types.New(types.T_uint64, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_uint64, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_float32, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_float32, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_float64, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_float64, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_date, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_date, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_datetime, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_datetime, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_timestamp, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_timestamp, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_decimal64, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_decimal64, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_decimal128, 0, 0)),
		newTestCase(t, false, mp, types.New(types.T_decimal128, 0, 0)),

		newTestCase(t, true, mp, types.New(types.T_varchar, types.MaxVarcharLen, 0)),
		newTestCase(t, false, mp, types.New(types.T_varchar, types.MaxVarcharLen, 0)),

		newTestCase(t, true, mp, types.New(types.T_array_float32, types.MaxArrayDimension, 0)),
		newTestCase(t, false, mp, types.New(types.T_array_float32, types.MaxArrayDimension, 0)),

		newTestCase(t, true, mp, types.New(types.T_array_float64, types.MaxArrayDimension, 0)),
		newTestCase(t, false, mp, types.New(types.T_array_float64, types.MaxArrayDimension, 0)),

		newTestCase(t, true, mp, types.T_Blockid.ToType()),
		newTestCase(t, false, mp, types.T_Blockid.ToType()),

		newTestCase(t, true, mp, types.T_Rowid.ToType()),
		newTestCase(t, false, mp, types.T_Rowid.ToType()),
	}
}

func TestSort(t *testing.T) {
	for _, tc := range makeTestCases(t) {
		os := make([]int64, tc.vec.Length())
		for i := range os {
			os[i] = int64(i)
		}
		nb0 := tc.proc.Mp().CurrNB()
		Sort(tc.desc, false, false, os, tc.vec)
		checkResult(t, tc.desc, tc.vec, os)
		nb1 := tc.proc.Mp().CurrNB()
		require.Equal(t, nb0, nb1)
		tc.vec.Free(tc.proc.Mp())
	}
}

func BenchmarkSortInt(b *testing.B) {
	vs := make([]int, BenchmarkRows)
	for i := range vs {
		vs[i] = i
	}
	for i := 0; i < b.N; i++ {
		sort.Ints(vs)
	}
}

func BenchmarkSortIntVector(b *testing.B) {
	m := mpool.MustNewZero()
	vec := testutil.NewInt32Vector(BenchmarkRows, types.T_int32.ToType(), m, true, nil)
	os := make([]int64, vec.Length())
	for i := range os {
		os[i] = int64(i)
	}
	for i := 0; i < b.N; i++ {
		Sort(false, false, false, os, vec)
	}
}

func checkResult(t *testing.T, desc bool, vec *vector.Vector, os []int64) {
	switch vec.GetType().Oid {
	case types.T_bit:
		vs := make([]int, len(os))
		col := vector.MustFixedColWithTypeCheck[uint64](vec)
		for i := range vs {
			vs[i] = int(col[i])
		}
		sort.Ints(vs)
		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, int(col[os[j]]))
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, int(col[os[i]]))
			}
		}
	case types.T_int32:
		vs := make([]int, len(os))
		col := vector.MustFixedColWithTypeCheck[int32](vec)
		for i := range vs {
			vs[i] = int(col[i])
		}
		sort.Ints(vs)
		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, int(col[os[j]]))
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, int(col[os[i]]))
			}
		}
	case types.T_int64:
		vs := make([]int, len(os))
		col := vector.MustFixedColWithTypeCheck[int64](vec)
		for i := range vs {
			vs[i] = int(col[i])
		}
		sort.Ints(vs)
		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, int(col[os[j]]))
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, int(col[os[i]]))
			}
		}
	case types.T_float32:
		vs := make([]float64, len(os))
		col := vector.MustFixedColWithTypeCheck[float32](vec)
		for i := range vs {
			vs[i] = float64(col[i])
		}
		sort.Float64s(vs)
		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, float64(col[os[j]]))
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, float64(col[os[i]]))
			}
		}
	case types.T_float64:
		vs := make([]float64, len(os))
		col := vector.MustFixedColWithTypeCheck[float64](vec)
		for i := range vs {
			vs[i] = float64(col[i])
		}
		sort.Float64s(vs)
		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, float64(col[os[j]]))
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, float64(col[os[i]]))
			}
		}
	case types.T_Blockid:
		col := vector.MustFixedColWithTypeCheck[types.Blockid](vec)
		vs := make([]types.Blockid, len(os))

		for i := range vs {
			vs[i] = col[i]
		}

		slices.SortFunc(vs, func(a, b types.Blockid) int {
			return a.Compare(&b)
		})

		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, col[os[j]])
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, col[os[i]])
			}
		}

	case types.T_Rowid:
		col := vector.MustFixedColWithTypeCheck[types.Rowid](vec)
		vs := make([]types.Rowid, len(os))

		for i := range vs {
			vs[i] = col[i]
		}

		slices.SortFunc(vs, func(a, b types.Rowid) int {
			return a.Compare(&b)
		})

		if desc {
			j := len(vs) - 1
			for _, v := range vs {
				require.Equal(t, v, col[os[j]])
				j--
			}
		} else {
			for i, v := range vs {
				require.Equal(t, v, col[os[i]])
			}
		}
	}
}

func newTestCase(t *testing.T, desc bool, m *mpool.MPool, typ types.Type) testCase {
	return testCase{
		desc: desc,
		proc: testutil.NewProcessWithMPool(t, "", m),
		vec:  testutil.NewVector(Rows, typ, m, true, nil),
	}
}

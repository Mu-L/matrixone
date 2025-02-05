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

package objectio

import (
	"bytes"
	"github.com/matrixorigin/matrixone/pkg/common/moerr"
)

// Block is the organizational structure of a batch in objectio
// Write one batch at a time, and batch and block correspond one-to-one
type Block struct {
	// id is the serial number of the block in the object
	id uint32

	// header is the metadata of the block, such as tableid, blockid, column count...
	header BlockHeader

	// columns is the vector in the batch
	columns []*ColumnBlock

	// object is a container that can store multiple blocks,
	// using a fileservice file storage
	object *Object

	// extent is the location of the block's metadata on the fileservice
	extent Extent

	// name is the file name or object name of the block
	name string
}

func NewBlock(colCnt uint16, object *Object, name string) BlockObject {
	header := BlockHeader{
		columnCount: colCnt,
	}
	block := &Block{
		header:  header,
		object:  object,
		columns: make([]*ColumnBlock, colCnt),
		name:    name,
	}
	for i := range block.columns {
		block.columns[i] = NewColumnBlock(uint16(i), block.object)
	}
	return block
}

func (b *Block) GetExtent() Extent {
	return b.extent
}

func (b *Block) GetColumn(idx uint16) (*ColumnBlock, error) {
	if idx >= uint16(len(b.columns)) {
		return nil, moerr.NewInternalErrorNoCtx("ObjectIO: bad index: %d, "+
			"block: %v, column count: %d",
			idx, b.name,
			len(b.columns))
	}
	return b.columns[idx], nil
}

func (b *Block) GetRows() (uint32, error) {
	panic(any("implement me"))
}

func (b *Block) GetMeta() BlockMeta {
	return BlockMeta{
		header: b.header,
		name:   b.name,
	}
}

func (b *Block) GetID() uint32 {
	return b.id
}

func (b *Block) GetColumnCount() uint16 {
	return b.header.columnCount
}

func (b *Block) MarshalMeta() []byte {
	var (
		buffer bytes.Buffer
	)
	// write header
	buffer.Write(b.header.Marshal())
	// write columns meta
	for _, column := range b.columns {
		buffer.Write(column.MarshalMeta())
	}
	return buffer.Bytes()
}

func (b *Block) UnmarshalMeta(data []byte, ZMUnmarshalFunc ZoneMapUnmarshalFunc) (uint32, error) {
	var err error
	size := uint32(0)
	b.header = BlockHeader{}
	b.header.Unmarshal(data)
	size += HeaderSize
	data = data[HeaderSize:]
	b.columns = make([]*ColumnBlock, b.header.columnCount)
	for i := range b.columns {
		b.columns[i] = NewColumnBlock(uint16(i), b.object)
		b.columns[i].meta.zoneMap = ZoneMap{
			idx:           uint16(i),
			unmarshalFunc: ZMUnmarshalFunc,
		}
		err = b.columns[i].UnmarshalMate(data)
		if err != nil {
			return 0, err
		}
		size += ColumnMetaSize
		data = data[ColumnMetaSize:]
	}
	return size, err
}

package index

import (
	"simpledbgo/record"
	"simpledbgo/tx"
	"simpledbgo/types"
)

type IndexInfo struct {
	indexName   string
	fieldName   string
	tx          *tx.Transaction
	tableSchema *record.Schema
	indexLayout *record.Layout
	statInfo    *StatInfo
}

func NewIndexInfo(indexName, fieldName string, tableSchema *record.Schema, tx *tx.Transaction, si *StatInfo) *IndexInfo {
	indexInfo := &IndexInfo{
		indexName: indexName,
		fieldName: fieldName,
		tx:        tx,
		statInfo:  si,
	}
	indexInfo.indexLayout = indexInfo.createIndexLayout()
	return indexInfo
}

func (ii *IndexInfo) Open() types.Index {
	return NewHashIndex(ii.tx, ii.indexName, ii.indexLayout)
	// return NewBTreeIndex(ii.tx, ii.indexName, ii.indexLayout)
}

func (ii *IndexInfo) BlocksAccessed() int {
	rpb := ii.tx.BlockSize() / ii.indexLayout.SlotSize()
	numBlocks := ii.statInfo.RecordsOutput() / rpb

	return HashIndexSearchCost(numBlocks, rpb)
	// return BTreeIndexSearchCost(numBlocks, rpb)
}

func (ii *IndexInfo) RecordsOutput() int {
	return ii.statInfo.RecordsOutput() / ii.statInfo.DistinctValues(ii.fieldName)
}

func (ii *IndexInfo) DistinctValues(fieldName string) int {
	if ii.fieldName == fieldName {
		return 1
	}

	return ii.statInfo.DistinctValues(fieldName)
}

func (ii *IndexInfo) createIndexLayout() *record.Layout {
	schema := record.NewSchema()
	schema.AddIntField("block")
	schema.AddIntField("id")

	if ii.indexLayout.Schema().Type(ii.fieldName) == record.FieldTypeInteger {
		schema.AddIntField("dataval")
	} else {
		fieldLength := ii.indexLayout.Schema().Length(ii.fieldName)
		schema.AddStringFiled("dataval", fieldLength)
	}
	return record.NewLayout(schema)
}

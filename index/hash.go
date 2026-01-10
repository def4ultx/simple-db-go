package index

import (
	"fmt"
	"simpledbgo/operator"
	"simpledbgo/query"
	"simpledbgo/record"
	"simpledbgo/tx"
)

const (
	HashBucketSize = 100
)

type HashIndex struct {
	tx        *tx.Transaction
	indexName string
	layout    *record.Layout
	searchKey *query.Constant
	tableScan *operator.TableScan
}

func NewHashIndex(tx *tx.Transaction, indexName string, layout *record.Layout) *HashIndex {
	idx := &HashIndex{
		tx:        tx,
		indexName: indexName,
		layout:    layout,
	}
	return idx
}

func (idx *HashIndex) BeforeFirst(searchKey *query.Constant) {
	idx.Close()
	idx.searchKey = searchKey

	bucket := searchKey.HashKey() % HashBucketSize
	tableName := fmt.Sprintf("%s-%d", idx.indexName, bucket)

	idx.tableScan = operator.NewTableScan(idx.tx, tableName, idx.layout)
}

func (idx *HashIndex) Next() bool {
	for idx.tableScan.Next() {
		val := idx.tableScan.GetVal("dataval")
		if query.ConstantEqual(val, idx.searchKey) {
			return true
		}
	}
	return false
}

func (idx *HashIndex) GetDataRowID() record.RowID {
	blockNum := idx.tableScan.GetInt("block")
	id := idx.tableScan.GetInt("id")
	return record.NewRowID(blockNum, id)
}

func (idx *HashIndex) Insert(val *query.Constant, rowID record.RowID) {
	idx.BeforeFirst(val)
	idx.tableScan.Insert()
	idx.tableScan.SetInt("block", rowID.BlockNumber())
	idx.tableScan.SetInt("id", rowID.Slot())
	idx.tableScan.SetVal("dataval", val)
}

func (idx *HashIndex) Delete(val *query.Constant, rowID record.RowID) {
	idx.BeforeFirst(val)
	for idx.Next() {
		if idx.GetDataRowID() == rowID {
			idx.tableScan.Delete()
			return
		}
	}
}

func (idx *HashIndex) Close() {
	if idx.tableScan != nil {
		idx.tableScan.Close()
	}
}

func HashIndexSearchCost(numBlocks int, rpb int) int {
	return numBlocks / HashBucketSize
}

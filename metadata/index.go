package metadata

import (
	"simpledbgo/index"
	"simpledbgo/operator"
	"simpledbgo/record"
	"simpledbgo/tx"
)

type IndexManager struct {
	layout       *record.Layout
	tableManager *TableManager
	statManager  *StatManager
}

func NewIndexManager(isNew bool, tableManager *TableManager, statManager *StatManager, tx *tx.Transaction) *IndexManager {
	if isNew {
		schema := record.NewSchema()
		schema.AddStringFiled("indexname", MaxName)
		schema.AddStringFiled("tablename", MaxName)
		schema.AddStringFiled("fieldname", MaxName)
		tableManager.CreateTable("idxcat", schema, tx)
	}

	mgr := &IndexManager{
		layout:       tableManager.GetLayout("idxcat", tx),
		tableManager: tableManager,
		statManager:  statManager,
	}
	return mgr
}

func (mgr *IndexManager) CreateIndex(indexName, tableName, fieldName string, tx *tx.Transaction) {
	ts := operator.NewTableScan(tx, "idxcat", mgr.layout)

	ts.Insert()
	ts.SetString("indexname", indexName)
	ts.SetString("tablename", tableName)
	ts.SetString("fieldname", fieldName)
	ts.Close()
}

func (mgr *IndexManager) GetIndexInfo(tableName string, tx *tx.Transaction) map[string]*IndexInfo {
	result := make(map[string]*IndexInfo)

	ts := operator.NewTableScan(tx, "idxcat", mgr.layout)
	for ts.Next() {
		if ts.GetString("tablename") == tableName {

			indexName := ts.GetString("indexname")
			fieldName := ts.GetString("fieldname")
			layout := mgr.tableManager.GetLayout(tableName, tx)

			tableStatInfo := mgr.statManager.GetStatInfo(tableName, layout, tx)
			ii := NewIndexInfo(indexName, fieldName, layout.Schema(), tx, tableStatInfo)

			result[tableName] = ii
		}
	}
	ts.Close()
	return result
}

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

func (ii *IndexInfo) Open() index.Index {
	// schema := record.NewSchema()
	return index.NewHashIndex(ii.tx, ii.indexName, ii.indexLayout)
	// NewBTreeIndex(ii.tx, ii.indexName, ii.indexLayout)
}

func (ii *IndexInfo) BlocksAccessed() int {
	rpb := ii.tx.BlockSize() / ii.indexLayout.SlotSize()
	numBlocks := ii.statInfo.RecordsOutput() / rpb

	return index.HashIndexSearchCost(numBlocks, rpb)
	// return BTreeIndex.searchCost(numBlocks, rpb)
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

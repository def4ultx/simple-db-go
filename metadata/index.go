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

func (mgr *IndexManager) GetIndexInfo(tableName string, tx *tx.Transaction) map[string]*index.IndexInfo {
	result := make(map[string]*index.IndexInfo)

	ts := operator.NewTableScan(tx, "idxcat", mgr.layout)
	for ts.Next() {
		if ts.GetString("tablename") == tableName {

			indexName := ts.GetString("indexname")
			fieldName := ts.GetString("fieldname")
			layout := mgr.tableManager.GetLayout(tableName, tx)

			tableStatInfo := mgr.statManager.GetStatInfo(tableName, layout, tx)
			ii := index.NewIndexInfo(indexName, fieldName, layout.Schema(), tx, tableStatInfo)

			result[tableName] = ii
		}
	}
	ts.Close()
	return result
}

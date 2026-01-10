package metadata

import (
	"simpledbgo/record"
	"simpledbgo/tx"
)

type MetadataManager struct {
	tableManager *TableManager
	viewManager  *ViewManager
	statManager  *StatManager
	indexManager *IndexManager
}

func NewMetadataManager(isNew bool, tx *tx.Transaction) *MetadataManager {
	tm := NewTableManager(isNew, tx)
	vm := NewViewManager(isNew, tm, tx)
	sm := NewStatManager(tm, tx)
	im := NewIndexManager(isNew, tm, sm, tx)
	mm := &MetadataManager{
		tableManager: tm,
		viewManager:  vm,
		statManager:  sm,
		indexManager: im,
	}
	return mm
}

func (mgr *MetadataManager) CreateTable(tableName string, schema *record.Schema, tx *tx.Transaction) {
	mgr.tableManager.CreateTable(tableName, schema, tx)
}

func (mgr *MetadataManager) GetLayout(tableName string, tx *tx.Transaction) *record.Layout {
	return mgr.tableManager.GetLayout(tableName, tx)
}

func (mgr *MetadataManager) CreateView(viewName string, viewDef string, tx *tx.Transaction) {
	mgr.viewManager.CreateView(viewName, viewDef, tx)
}

func (mgr *MetadataManager) GetViewDef(viewName string, tx *tx.Transaction) string {
	return mgr.viewManager.GetViewDef(viewName, tx)
}

func (mgr *MetadataManager) CreateIndex(indexName, tableName, fieldName string, tx *tx.Transaction) {
	mgr.indexManager.CreateIndex(indexName, tableName, fieldName, tx)
}

func (mgr *MetadataManager) GetIndexInfo(tableName string, tx *tx.Transaction) map[string]*IndexInfo {
	return mgr.indexManager.GetIndexInfo(tableName, tx)
}

func (mgr *MetadataManager) GetStatInfo(tableName string, layout *record.Layout, tx *tx.Transaction) *StatInfo {
	return mgr.statManager.GetStatInfo(tableName, layout, tx)
}

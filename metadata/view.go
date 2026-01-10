package metadata

import (
	"simpledbgo/operator"
	"simpledbgo/record"
	"simpledbgo/tx"
)

const (
	MaxViewDef = 100
)

type ViewManager struct {
	tableManager *TableManager
}

func NewViewManager(isNew bool, tblMgr *TableManager, tx *tx.Transaction) *ViewManager {
	if isNew {
		schema := record.NewSchema()
		schema.AddStringFiled("viewname", MaxName)
		schema.AddStringFiled("viewdef", MaxViewDef)
		tblMgr.CreateTable("viewcat", schema, tx)
	}

	mgr := &ViewManager{
		tableManager: tblMgr,
	}
	return mgr
}

func (mgr *ViewManager) CreateView(viewName string, viewDef string, tx *tx.Transaction) {
	layout := mgr.tableManager.GetLayout("viewcat", tx)
	ts := operator.NewTableScan(tx, "viewcat", layout)

	ts.SetString("viewname", viewName)
	ts.SetString("viewdef", viewDef)
	ts.Close()
}

func (mgr *ViewManager) GetViewDef(viewName string, tx *tx.Transaction) string {
	layout := mgr.tableManager.GetLayout("viewcat", tx)
	ts := operator.NewTableScan(tx, "viewcat", layout)

	var result string
	for ts.Next() {
		if ts.GetString("viewname") == viewName {
			result = ts.GetString("viewdef")
			break
		}
	}
	ts.Close()
	return result
}

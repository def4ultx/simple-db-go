package metadata

import (
	"simpledbgo/operator"
	"simpledbgo/record"
	"simpledbgo/tx"
)

type StatManager struct {
	tableManager *TableManager
	tableStats   map[string]*StatInfo
	numCalls     int
}

func NewStatManager(tableManager *TableManager, tx *tx.Transaction) *StatManager {
	mgr := &StatManager{
		tableManager: tableManager,
		tableStats:   make(map[string]*StatInfo),
		numCalls:     0,
	}
	return mgr
}

// TODO: Synchronized
func (mgr *StatManager) GetStatInfo(tableName string, layout *record.Layout, tx *tx.Transaction) *StatInfo {
	mgr.numCalls++

	if mgr.numCalls > 100 {
		mgr.refreshStatistics(tx)
	}
	si, ok := mgr.tableStats[tableName]
	if !ok {
		si = mgr.calculateTableStats(tableName, layout, tx)
		mgr.tableStats[tableName] = si
	}
	return si
}

// TODO: Synchronized
func (mgr *StatManager) refreshStatistics(tx *tx.Transaction) {
	mgr.tableStats = make(map[string]*StatInfo)
	mgr.numCalls = 0

	tableCatalogLayout := mgr.tableManager.GetLayout("tblcat", tx)
	tableCatalog := operator.NewTableScan(tx, "tblcat", tableCatalogLayout)

	for tableCatalog.Next() {
		tableName := tableCatalog.GetString("tblname")
		layout := mgr.tableManager.GetLayout(tableName, tx)
		si := mgr.calculateTableStats(tableName, layout, tx)
		mgr.tableStats[tableName] = si
	}

	tableCatalog.Close()
}

// TODO: Synchronized
func (mgr *StatManager) calculateTableStats(tableName string, layout *record.Layout, tx *tx.Transaction) *StatInfo {
	numRec := 0
	numBlock := 0

	ts := operator.NewTableScan(tx, tableName, layout)
	for ts.Next() {
		numRec++
		numBlock = ts.GetRowID().BlockNumber() + 1
	}
	ts.Close()

	si := &StatInfo{
		numBlock: numBlock,
		numRec:   numRec,
	}
	return si
}

type StatInfo struct {
	numBlock int
	numRec   int
}

func (si StatInfo) BlocksAccessed() int {
	return si.numBlock
}

func (si StatInfo) RecordsOutput() int {
	return si.numRec
}

func (si StatInfo) DistinctValues(fieldName string) int {
	return 1 + (si.numRec / 3) // Not accurate
}

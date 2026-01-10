package plan

import (
	"log"
	"simpledbgo/index"
	"simpledbgo/metadata"
	"simpledbgo/parser"
	"simpledbgo/tx"
	"simpledbgo/types"
)

type IndexUpdatePlanner struct {
	mdm *metadata.MetadataManager
}

func NewIndexUpdatePlanner(mdm *metadata.MetadataManager) *IndexUpdatePlanner {
	return &IndexUpdatePlanner{mdm: mdm}
}

func (p *IndexUpdatePlanner) ExecuteInsert(data *parser.InsertData, tx *tx.Transaction) int {
	tableName := data.TableName

	plan := NewTablePlan(tx, tableName, p.mdm)
	// first, insert the record

	s := plan.Open().(types.UpdateScan)
	s.Insert()
	rowID := s.GetRowID()

	// then modify each field, inserting index records
	indexes := p.mdm.GetIndexInfo(tableName, tx)
	for idx, field := range data.Fields {
		val := data.Values[idx]

		log.Println("Modify field", field, val)
		s.SetVal(field, val)

		ii, ok := indexes[field]
		if !ok {
			continue
		}
		idx := ii.Open()
		idx.Insert(val, rowID)
		idx.Close()
	}

	s.Close()
	return 1
}

func (p *IndexUpdatePlanner) ExecuteDelete(data *parser.DeleteData, tx *tx.Transaction) int {
	tableName := data.TableName

	var pl types.Plan
	pl = NewTablePlan(tx, tableName, p.mdm)
	pl = NewSelectPlan(pl, data.Predicate)
	indexes := p.mdm.GetIndexInfo(tableName, tx)

	s := pl.Open().(types.UpdateScan)
	count := 0
	for s.Next() {
		// first, delete the record's RID from every index
		rid := s.GetRowID()
		for field := range indexes {
			val := s.GetVal(field)
			idx := indexes[field].Open()
			idx.Delete(val, rid)
			idx.Close()
		}

		// then delete the record
		s.Delete()
		count++
	}

	s.Close()
	return count
}

func (p *IndexUpdatePlanner) ExecuteUpdate(data *parser.UpdateData, tx *tx.Transaction) int {
	tableName := data.TableName
	fieldName := data.FieldName

	var pl types.Plan
	pl = NewTablePlan(tx, tableName, p.mdm)
	pl = NewSelectPlan(pl, data.Predicate)

	ii, ok := p.mdm.GetIndexInfo(tableName, tx)[fieldName]
	var index index.Index
	if ok {
		index = ii.Open()
	}

	s := pl.Open().(types.UpdateScan)
	count := 0
	for s.Next() {
		// first, update the record
		newVal := data.NewValue.Evaluate(s)
		oldVal := s.GetVal(fieldName)
		s.SetVal(data.FieldName, newVal)

		// then update the appropriate index, if it exists
		if index != nil {
			rid := s.GetRowID()
			index.Delete(oldVal, rid)
			index.Insert(newVal, rid)
		}
		count++
	}

	if index != nil {
		index.Close()
	}
	s.Close()
	return count
}

func (p *IndexUpdatePlanner) ExecuteCreateTable(data *parser.CreateTableData, tx *tx.Transaction) int {
	p.mdm.CreateTable(data.TableName, data.Schema, tx)
	return 0
}

func (p *IndexUpdatePlanner) ExecuteCreateView(data *parser.CreateViewData, tx *tx.Transaction) int {
	p.mdm.CreateView(data.ViewName, data.ViewDef(), tx)
	return 0
}

func (p *IndexUpdatePlanner) ExecuteCreateIndex(data *parser.CreateIndexData, tx *tx.Transaction) int {
	p.mdm.CreateIndex(data.IndexName, data.TableName, data.FieldName, tx)
	return 0
}

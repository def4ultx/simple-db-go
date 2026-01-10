package plan

import (
	"simpledbgo/metadata"
	"simpledbgo/parser"
	"simpledbgo/tx"
	"simpledbgo/types"
)

type Planner struct {
	QueryPlanner  *BasicQueryPlanner
	UpdatePlanner *BasicUpdatePlanner
}

func NewPlanner(qp *BasicQueryPlanner, up *BasicUpdatePlanner) *Planner {
	p := &Planner{
		QueryPlanner:  qp,
		UpdatePlanner: up,
	}
	return p
}

func (planner *Planner) CreateQueryPlan(cmd string, tx *tx.Transaction) types.Plan {
	p := parser.NewPredicateParser(cmd)
	data := p.Query()
	return planner.QueryPlanner.CreatePlan(data, tx)
}

func (planner *Planner) ExecuteUpdate(cmd string, tx *tx.Transaction) int {
	p := parser.NewPredicateParser(cmd)
	obj := p.Command()

	switch v := obj.(type) {
	case *parser.InsertData:
		return planner.UpdatePlanner.ExecuteInsert(v, tx)
	case *parser.UpdateData:
		return planner.UpdatePlanner.ExecuteUpdate(v, tx)
	case *parser.DeleteData:
		return planner.UpdatePlanner.ExecuteDelete(v, tx)
	case *parser.CreateTableData:
		return planner.UpdatePlanner.ExecuteCreateTable(v, tx)
	case *parser.CreateViewData:
		return planner.UpdatePlanner.ExecuteCreateView(v, tx)
	case *parser.CreateIndexData:
		return planner.UpdatePlanner.ExecuteCreateIndex(v, tx)
	default:
		return 0
	}
}

type BasicQueryPlanner struct {
	metadataManager *metadata.MetadataManager
}

func NewBasicQueryPlanner(mdm *metadata.MetadataManager) *BasicQueryPlanner {
	return &BasicQueryPlanner{metadataManager: mdm}
}

func (planner *BasicQueryPlanner) CreatePlan(data *parser.QueryData, tx *tx.Transaction) types.Plan {
	// Step 1: Create a plan for each mentioned table or view.
	plans := make([]types.Plan, 0)
	for _, table := range data.Tables {
		viewDef := planner.metadataManager.GetViewDef(table, tx)
		if viewDef != "" {
			parser := parser.NewPredicateParser(viewDef)
			viewData := parser.Query()
			plans = append(plans, planner.CreatePlan(viewData, tx))
		} else {
			plans = append(plans, NewTablePlan(tx, table, planner.metadataManager))
		}
	}

	// Step 2: Create the product of all table plans
	current := plans[0]
	remaining := plans[1:]

	for _, plan := range remaining {
		// Simple version
		// current = NewProductPlan(current, plan)

		// Optimize by BlocksAccessed
		p1 := NewProductPlan(current, plan)
		p2 := NewProductPlan(plan, current)

		if p1.BlocksAccessed() < p2.BlocksAccessed() {
			current = p1
		} else {
			current = p2
		}
	}

	// Step 3: Add a selection plan for the predicate
	current = NewSelectPlan(current, data.Predicate)

	// Step 4: Project on the field names
	return NewProjectPlan(current, data.Fields)
}

type BasicUpdatePlanner struct {
	metadataManager *metadata.MetadataManager
}

func NewBasicUpdatePlanner(mdm *metadata.MetadataManager) *BasicUpdatePlanner {
	return &BasicUpdatePlanner{metadataManager: mdm}
}

func (planner *BasicUpdatePlanner) ExecuteDelete(data *parser.DeleteData, tx *tx.Transaction) int {
	var plan types.Plan
	plan = NewTablePlan(tx, data.TableName, planner.metadataManager)
	plan = NewSelectPlan(plan, data.Predicate)

	us := plan.Open().(types.UpdateScan)

	count := 0
	for us.Next() {
		us.Delete()
		count++
	}
	us.Close()
	return count
}

func (planner *BasicUpdatePlanner) ExecuteUpdate(data *parser.UpdateData, tx *tx.Transaction) int {
	var plan types.Plan
	plan = NewTablePlan(tx, data.TableName, planner.metadataManager)
	plan = NewSelectPlan(plan, data.Predicate)

	us := plan.Open().(types.UpdateScan)

	count := 0
	for us.Next() {
		val := data.NewValue.Evaluate(us)
		us.SetVal(data.FieldName, val)
		count++
	}
	us.Close()
	return count
}

func (planner *BasicUpdatePlanner) ExecuteInsert(data *parser.InsertData, tx *tx.Transaction) int {
	var plan types.Plan
	plan = NewTablePlan(tx, data.TableName, planner.metadataManager)

	us := plan.Open().(types.UpdateScan)
	us.Insert()

	iter := data.Values
	for i, field := range data.Fields {
		us.SetVal(field, iter[i])
	}
	us.Close()
	return 1
}

func (planner *BasicUpdatePlanner) ExecuteCreateTable(data *parser.CreateTableData, tx *tx.Transaction) int {
	planner.metadataManager.CreateTable(data.TableName, data.Schema, tx)
	return 0
}

func (planner *BasicUpdatePlanner) ExecuteCreateView(data *parser.CreateViewData, tx *tx.Transaction) int {
	planner.metadataManager.CreateView(data.ViewName, data.ViewDef(), tx)
	return 0
}

func (planner *BasicUpdatePlanner) ExecuteCreateIndex(data *parser.CreateIndexData, tx *tx.Transaction) int {
	planner.metadataManager.CreateIndex(data.IndexName, data.TableName, data.FieldName, tx)
	return 0
}

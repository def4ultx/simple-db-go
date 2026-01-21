package plan

import (
	"simpledbgo/index"
	"simpledbgo/metadata"
	"simpledbgo/multibuffer"
	"simpledbgo/parser"
	"simpledbgo/query"
	"simpledbgo/record"
	"simpledbgo/tx"
	"simpledbgo/types"
	"slices"
)

type HeuristicQueryPlanner struct {
	tablePlanners []*TablePlanner
	mdm           *metadata.MetadataManager
}

func NewHeuristicQueryPlanner(mdm *metadata.MetadataManager) *HeuristicQueryPlanner {
	planner := &HeuristicQueryPlanner{
		tablePlanners: make([]*TablePlanner, 0),
		mdm:           mdm,
	}
	return planner
}

func (h *HeuristicQueryPlanner) CreatePlan(data *parser.QueryData, tx *tx.Transaction) types.Plan {
	// Step 1, Create a table planner object for each mention table
	for _, table := range data.Tables {
		tp := NewTablePlanner(table, data.Predicate, tx, h.mdm)
		h.tablePlanners = append(h.tablePlanners, tp)
	}

	// Step 2, Choose lowest-size plan to begin the join order
	currentPlan := h.getLowestSelectPlan()

	// Step 3, Repeatedly add a plan to the join order
	for len(h.tablePlanners) > 0 {
		p := h.getLowestJoinPlan(currentPlan)
		if p != nil {
			currentPlan = p
		} else {
			// no applicable join
			currentPlan = h.getLowestProductPlan(currentPlan)
		}
	}

	// Step 4, Project on the field name and returns
	return NewProjectPlan(currentPlan, data.Fields)
}

func (h *HeuristicQueryPlanner) getLowestSelectPlan() types.Plan {
	var bestTablePlannerIdx int
	var bestPlan types.Plan

	for idx, tp := range h.tablePlanners {
		plan := tp.makeSelectPlan()
		if bestPlan == nil || plan.RecordsOutput() < bestPlan.RecordsOutput() {
			bestTablePlannerIdx = idx
			bestPlan = plan
		}
	}

	h.tablePlanners = slices.Delete(h.tablePlanners, bestTablePlannerIdx, bestTablePlannerIdx+1)
	return bestPlan
}

func (h *HeuristicQueryPlanner) getLowestJoinPlan(current types.Plan) types.Plan {
	var bestTablePlannerIdx int
	var bestPlan types.Plan

	for idx, tp := range h.tablePlanners {
		plan := tp.makeJoinPlan(current)
		if plan != nil && (bestPlan == nil || plan.RecordsOutput() < bestPlan.RecordsOutput()) {
			bestTablePlannerIdx = idx
			bestPlan = plan
		}
	}

	if bestPlan != nil {
		h.tablePlanners = slices.Delete(h.tablePlanners, bestTablePlannerIdx, bestTablePlannerIdx+1)
	}
	return bestPlan
}

func (h *HeuristicQueryPlanner) getLowestProductPlan(current types.Plan) types.Plan {
	var bestTablePlannerIdx int
	var bestPlan types.Plan

	for idx, tp := range h.tablePlanners {
		plan := tp.makeProductPlan(current)
		if bestPlan == nil || plan.RecordsOutput() < bestPlan.RecordsOutput() {
			bestTablePlannerIdx = idx
			bestPlan = plan
		}
	}

	h.tablePlanners = slices.Delete(h.tablePlanners, bestTablePlannerIdx, bestTablePlannerIdx+1)
	return bestPlan
}

type TablePlanner struct {
	plan      *TablePlan
	predicate *query.Predicate
	schema    *record.Schema
	indexes   map[string]*index.IndexInfo
	tx        *tx.Transaction
}

func NewTablePlanner(tableName string, pred *query.Predicate, tx *tx.Transaction, mdm *metadata.MetadataManager) *TablePlanner {
	plan := NewTablePlan(tx, tableName, mdm)
	planner := &TablePlanner{
		plan:      plan,
		predicate: pred,
		schema:    plan.Schema(),
		indexes:   mdm.GetIndexInfo(tableName, tx),
		tx:        tx,
	}
	return planner
}

func (tp *TablePlanner) makeSelectPlan() types.Plan {
	p := tp.makeIndexSelect()
	if p == nil {
		p = tp.plan
	}
	return tp.addSelectPred(p)
}

func (tp *TablePlanner) makeJoinPlan(current types.Plan) types.Plan {
	currentSchema := current.Schema()
	joinPredicate := tp.predicate.JoinSubPred(tp.schema, currentSchema)

	if joinPredicate == nil {
		return nil
	}

	p := tp.makeIndexJoin(current, currentSchema)
	if p == nil {
		p = tp.makeProductJoin(current, currentSchema)
	}
	return p
}

func (tp *TablePlanner) makeProductPlan(current types.Plan) types.Plan {
	p := tp.addSelectPred(tp.plan)
	return multibuffer.NewProductPlan(tp.tx, current, p)
}

func (tp *TablePlanner) makeIndexSelect() types.Plan {
	for field, ii := range tp.indexes {
		val := tp.predicate.EquatesWithConstant(field)
		if val != nil {
			return index.NewIndexSelectPlan(tp.plan, ii, val)
		}
	}
	return nil
}

func (tp *TablePlanner) makeIndexJoin(current types.Plan, currentSchema *record.Schema) types.Plan {
	for field, ii := range tp.indexes {
		outerField := tp.predicate.EquatesWithField(field)
		if outerField != nil && currentSchema.HasField(*outerField) {
			var p types.Plan
			p = index.NewIndexJoinPlan(current, tp.plan, ii, *outerField)
			p = tp.addSelectPred(p)
			return tp.addJoinPred(p, currentSchema)
		}
	}
	return nil
}

func (tp *TablePlanner) makeProductJoin(current types.Plan, currentSchema *record.Schema) types.Plan {
	p := tp.makeProductPlan(current)
	return tp.addJoinPred(p, currentSchema)
}

func (tp *TablePlanner) addSelectPred(current types.Plan) types.Plan {
	selectPred := tp.predicate.SelectSubPredicate(tp.schema)
	if selectPred != nil {
		return NewSelectPlan(current, selectPred)
	}
	return current
}

func (tp *TablePlanner) addJoinPred(current types.Plan, currentSchema *record.Schema) types.Plan {
	joinPred := tp.predicate.JoinSubPred(currentSchema, tp.schema)
	if joinPred != nil {
		return NewSelectPlan(current, joinPred)
	} else {
		return current
	}
}

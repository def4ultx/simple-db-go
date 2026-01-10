package plan

import (
	"simpledbgo/metadata"
	"simpledbgo/operator"
	"simpledbgo/query"
	"simpledbgo/record"
	"simpledbgo/tx"
	"simpledbgo/types"
)

type TablePlan struct {
	tx        *tx.Transaction
	tableName string
	layout    *record.Layout
	statInfo  *metadata.StatInfo
}

func NewTablePlan(tx *tx.Transaction, tableName string, mdm *metadata.MetadataManager) *TablePlan {
	layout := mdm.GetLayout(tableName, tx)
	statInfo := mdm.GetStatInfo(tableName, layout, tx)

	tp := &TablePlan{
		tx:        tx,
		tableName: tableName,
		layout:    layout,
		statInfo:  statInfo,
	}
	return tp
}

func (tp *TablePlan) Open() types.Scan {
	return operator.NewTableScan(tp.tx, tp.tableName, tp.layout)
}

func (tp *TablePlan) BlocksAccessed() int {
	return tp.statInfo.BlocksAccessed()
}

func (tp *TablePlan) RecordsOutput() int {
	return tp.statInfo.RecordsOutput()
}

func (tp *TablePlan) DistinctValues(fieldName string) int {
	return tp.statInfo.DistinctValues(fieldName)
}

func (tp *TablePlan) Schema() *record.Schema {
	return tp.layout.Schema()
}

type SelectPlan struct {
	plan types.Plan
	pred *query.Predicate
}

func NewSelectPlan(plan types.Plan, pred *query.Predicate) *SelectPlan {
	tp := &SelectPlan{
		plan: plan,
		pred: pred,
	}
	return tp
}

func (tp *SelectPlan) Open() types.Scan {
	s := tp.plan.Open()
	return operator.NewSelectScan(s, tp.pred)
}

func (tp *SelectPlan) BlocksAccessed() int {
	return tp.plan.BlocksAccessed()
}

func (tp *SelectPlan) RecordsOutput() int {
	return tp.plan.RecordsOutput() / tp.pred.ReductionFactor(tp.plan)
}

func (tp *SelectPlan) DistinctValues(fieldName string) int {
	if tp.pred.EquatesWithConstant(fieldName) != nil {
		return 1
	}

	f := tp.pred.EquatesWithField(fieldName)
	if f != nil {
		return min(tp.plan.DistinctValues(fieldName), tp.plan.DistinctValues(*f))
	}

	return tp.plan.DistinctValues(fieldName)
}

func (tp *SelectPlan) Schema() *record.Schema {
	return tp.plan.Schema()
}

type ProjectPlan struct {
	plan   types.Plan
	schema *record.Schema
}

func NewProjectPlan(plan types.Plan, fields []string) *ProjectPlan {
	schema := record.NewSchema()
	for _, v := range fields {
		schema.Add(v, plan.Schema())
	}

	tp := &ProjectPlan{
		plan:   plan,
		schema: schema,
	}
	return tp
}

func (tp *ProjectPlan) Open() types.Scan {
	s := tp.plan.Open()
	return operator.NewProjectScan(s, tp.schema.Fields())
}

func (tp *ProjectPlan) BlocksAccessed() int {
	return tp.plan.BlocksAccessed()
}

func (tp *ProjectPlan) RecordsOutput() int {
	return tp.plan.RecordsOutput()
}

func (tp *ProjectPlan) DistinctValues(fieldName string) int {
	return tp.plan.DistinctValues(fieldName)
}

func (tp *ProjectPlan) Schema() *record.Schema {
	return tp.plan.Schema()
}

type ProductPlan struct {
	p1, p2 types.Plan
	schema *record.Schema
}

func NewProductPlan(p1, p2 types.Plan) *ProductPlan {
	schema := record.NewSchema()
	schema.AddAll(p1.Schema())
	schema.AddAll(p2.Schema())

	tp := &ProductPlan{
		p1:     p1,
		p2:     p2,
		schema: schema,
	}
	return tp
}

func (tp *ProductPlan) Open() types.Scan {
	s1 := tp.p1.Open()
	s2 := tp.p2.Open()
	return operator.NewProductScan(s1, s2)
}

func (tp *ProductPlan) BlocksAccessed() int {
	return tp.p1.BlocksAccessed() + (tp.p1.RecordsOutput() * tp.p2.BlocksAccessed())
}

func (tp *ProductPlan) RecordsOutput() int {
	return tp.p1.RecordsOutput() * tp.p2.RecordsOutput()
}

func (tp *ProductPlan) DistinctValues(fieldName string) int {
	if tp.p1.Schema().HasField(fieldName) {
		return tp.p1.DistinctValues(fieldName)
	}
	return tp.p2.DistinctValues(fieldName)
}

func (tp *ProductPlan) Schema() *record.Schema {
	return tp.schema
}

package materialize

import (
	"math"
	"simpledbgo/record"
	"simpledbgo/tx"
	"simpledbgo/types"
)

type MaterializePlan struct {
	sourcePlan types.Plan
	tx         *tx.Transaction
}

func NewMaterializePlan(tx *tx.Transaction, src types.Plan) *MaterializePlan {
	return &MaterializePlan{
		sourcePlan: src,
		tx:         tx,
	}
}

func (p *MaterializePlan) Open() types.Scan {
	schema := p.sourcePlan.Schema()
	temp := NewTempTable(p.tx, schema)

	src := p.sourcePlan.Open()
	dst := temp.Open()

	for src.Next() {
		dst.Insert()
		for _, field := range schema.Fields() {
			dst.SetVal(field, src.GetVal(field))
		}
	}
	src.Close()
	dst.BeforeFirst()
	return dst
}

func (p *MaterializePlan) BlocksAccessed() int {
	// create a dummy layout object to calculate slot size
	l := record.NewLayout(p.sourcePlan.Schema())
	rpb := float64(p.tx.BlockSize() / l.SlotSize())
	return int(math.Ceil(float64(p.sourcePlan.RecordsOutput())) / rpb)
}

func (p *MaterializePlan) RecordsOutput() int {
	return p.sourcePlan.RecordsOutput()
}

func (p *MaterializePlan) DistinctValues(fieldName string) int {
	return p.sourcePlan.DistinctValues(fieldName)
}

func (p *MaterializePlan) Schema() *record.Schema {
	return p.sourcePlan.Schema()
}

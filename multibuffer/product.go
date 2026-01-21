package multibuffer

import (
	"simpledbgo/materialize"
	"simpledbgo/operator"
	"simpledbgo/record"
	"simpledbgo/tx"
	"simpledbgo/types"
)

type ProductPlan struct {
	tx       *tx.Transaction
	lhs, rhs types.Plan
	schema   *record.Schema
}

func NewProductPlan(tx *tx.Transaction, lhs, rhs types.Plan) *ProductPlan {
	p := &ProductPlan{
		tx:     tx,
		lhs:    lhs,
		rhs:    rhs,
		schema: record.NewSchema(),
	}
	p.schema.AddAll(lhs.Schema())
	p.schema.AddAll(rhs.Schema())

	return p
}

func (p *ProductPlan) Open() types.Scan {
	leftScan := p.lhs.Open()
	temp := p.copyRecordsFrom(p.rhs)

	return NewProductScan(p.tx, leftScan, temp.TableName(), temp.GetLayout())
}

func (p *ProductPlan) BlocksAccessed() int {
	// this guesses at the # of chunks
	avail := p.tx.AvailableBuffer()
	size := materialize.NewMaterializePlan(p.tx, p.rhs).BlocksAccessed()
	numChunks := size / avail
	return p.rhs.BlocksAccessed() + (p.lhs.BlocksAccessed() * numChunks)
}

func (p *ProductPlan) RecordsOutput() int {
	return p.lhs.RecordsOutput() * p.rhs.RecordsOutput()
}

func (p *ProductPlan) DistinctValues(fieldName string) int {
	if p.lhs.Schema().HasField(fieldName) {
		return p.lhs.DistinctValues(fieldName)
	} else {
		return p.rhs.DistinctValues(fieldName)
	}
}

func (p *ProductPlan) Schema() *record.Schema {
	return p.schema
}

func (p *ProductPlan) copyRecordsFrom(plan types.Plan) *materialize.TempTable {
	src := plan.Open()
	schema := plan.Schema()

	tt := materialize.NewTempTable(p.tx, schema)
	dst := tt.Open().(types.UpdateScan)

	for src.Next() {
		dst.Insert()
		for _, field := range schema.Fields() {
			dst.SetVal(field, src.GetVal(field))
		}
	}

	src.Close()
	dst.Close()

	return tt
}

type ProductScan struct {
	tx              *tx.Transaction
	lhsScan         types.Scan
	rhsScan         types.Scan
	prodScan        types.Scan
	filename        string
	layout          *record.Layout
	chunkSize       int
	nextBlockNumber int
	fileSize        int
}

func NewProductScan(tx *tx.Transaction, lhs types.Scan, filename string, layout *record.Layout) *ProductScan {
	avail := tx.AvailableBuffer()
	fileSize := tx.Size(filename)
	p := &ProductScan{
		tx:              tx,
		lhsScan:         lhs,
		filename:        filename,
		layout:          layout,
		chunkSize:       BufferNeeds.BestFactor(avail, fileSize),
		nextBlockNumber: 0,
		fileSize:        fileSize,
	}
	p.BeforeFirst()

	return p
}

func (p *ProductScan) BeforeFirst() {
	p.nextBlockNumber = 0
	p.useNextChunk()
}

func (p *ProductScan) Next() bool {
	for !p.prodScan.Next() {
		if !p.useNextChunk() {
			return false
		}
	}
	return true
}

func (p *ProductScan) Close() {
	p.prodScan.Close()
}

func (p *ProductScan) GetInt(fieldName string) int {
	return p.prodScan.GetInt(fieldName)
}

func (p *ProductScan) GetString(fieldName string) string {
	return p.prodScan.GetString(fieldName)
}

func (p *ProductScan) GetVal(fieldName string) *types.Constant {
	return p.prodScan.GetVal(fieldName)
}

func (p *ProductScan) HasField(fieldName string) bool {
	return p.prodScan.HasField(fieldName)
}

func (p *ProductScan) useNextChunk() bool {
	if p.rhsScan != nil {
		p.rhsScan.Close()
	}

	if p.nextBlockNumber >= p.fileSize {
		return false
	}

	end := p.nextBlockNumber + p.chunkSize - 1
	if end >= p.fileSize {
		end = p.fileSize - 1
	}

	p.rhsScan = NewChunkScan(p.tx, p.filename, p.layout, p.nextBlockNumber, end)
	p.lhsScan.BeforeFirst()
	p.prodScan = operator.NewProductScan(p.lhsScan, p.rhsScan)
	p.nextBlockNumber = end + 1
	return true
}

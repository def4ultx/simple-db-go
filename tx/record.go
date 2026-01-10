package tx

import (
	"fmt"
	"simpledbgo/file"
	"simpledbgo/log"
	"strconv"
)

type RecordType int

const (
	RecordTypeCheckpoint RecordType = iota
	RecordTypeStart
	RecordTypeCommit
	RecordTypeRollback
	RecordTypeSetInt
	RecordTypeSetString
)

type Record interface {
	Op() RecordType
	TxNumber() int
	Undo(tx *Transaction)
}

func CreateRecord(records []byte) Record {
	p := file.NewPageFromBytes(records)
	switch RecordType(p.GetInt(0)) {
	case RecordTypeCheckpoint:
		return NewRecordCheckpoint()
	case RecordTypeStart:
		return NewRecordStart(p)
	case RecordTypeCommit:
		return NewRecordCommit(p)
	case RecordTypeRollback:
		return NewRecordRollback(p)
	case RecordTypeSetInt:
		return NewRecordSetInt(p)
	case RecordTypeSetString:
		return NewRecordSetString(p)
	default:
		panic("invalid record type")
	}
}

const IntegerSize = 32 / 8

type RecordCheckpoint struct{}

func (r *RecordCheckpoint) Op() RecordType       { return RecordTypeCheckpoint }
func (r *RecordCheckpoint) TxNumber() int        { return -1 }
func (r *RecordCheckpoint) Undo(tx *Transaction) { return }
func (r *RecordCheckpoint) String() string       { return "<CHECKPOINT>" }

func NewRecordCheckpoint() Record {
	return &RecordCheckpoint{}
}

func RecordCheckpointWriteToLog(lm *log.Manager) int {
	rec := make([]byte, IntegerSize)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int(RecordTypeCheckpoint))
	return lm.Append(rec)
}

type RecordStart struct {
	txNum int
}

func (r *RecordStart) Op() RecordType       { return RecordTypeStart }
func (r *RecordStart) TxNumber() int        { return r.txNum }
func (r *RecordStart) Undo(tx *Transaction) { return }
func (r *RecordStart) String() string       { return "<START " + strconv.Itoa(r.txNum) + ">" }

func NewRecordStart(p *file.Page) Record {
	return &RecordStart{txNum: p.GetInt(IntegerSize)}
}

func RecordStartWriteToLog(lm *log.Manager, txNum int) int {
	rec := make([]byte, IntegerSize*2)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int(RecordTypeStart))
	p.SetInt(IntegerSize, txNum)
	return lm.Append(rec)
}

type RecordCommit struct {
	txNum int
}

func (r *RecordCommit) Op() RecordType       { return RecordTypeCommit }
func (r *RecordCommit) TxNumber() int        { return r.txNum }
func (r *RecordCommit) Undo(tx *Transaction) { return }
func (r *RecordCommit) String() string       { return "<COMMIT " + strconv.Itoa(r.txNum) + ">" }

func NewRecordCommit(p *file.Page) Record {
	return &RecordCommit{txNum: p.GetInt(IntegerSize)}
}

func RecordCommitWriteToLog(lm *log.Manager, txNum int) int {
	rec := make([]byte, IntegerSize*2)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int(RecordTypeCommit))
	p.SetInt(IntegerSize, txNum)
	return lm.Append(rec)
}

type RecordRollback struct {
	txNum int
}

func (r *RecordRollback) Op() RecordType       { return RecordTypeRollback }
func (r *RecordRollback) TxNumber() int        { return r.txNum }
func (r *RecordRollback) Undo(tx *Transaction) { return }
func (r *RecordRollback) String() string       { return "<ROLLBACK " + strconv.Itoa(r.txNum) + ">" }

func NewRecordRollback(p *file.Page) Record {
	return &RecordRollback{txNum: p.GetInt(IntegerSize)}
}

func RecordRollbackWriteToLog(lm *log.Manager, txNum int) int {
	rec := make([]byte, IntegerSize*2)
	p := file.NewPageFromBytes(rec)
	p.SetInt(0, int(RecordTypeRollback))
	p.SetInt(IntegerSize, txNum)
	return lm.Append(rec)
}

type RecordSetInt struct {
	txNum  int
	offset int
	val    int
	block  *file.BlockID
}

func (r *RecordSetInt) Op() RecordType { return RecordTypeSetInt }
func (r *RecordSetInt) TxNumber() int  { return r.txNum }
func (r *RecordSetInt) Undo(tx *Transaction) {
	tx.Pin(r.block)
	tx.SetInt(r.block, r.offset, r.val, false)
	tx.Unpin(r.block)
	return
}
func (r *RecordSetInt) String() string {
	return fmt.Sprintf("<SETINT %d %v %d %d>", r.txNum, r.block, r.offset, r.val)
}

func NewRecordSetInt(p *file.Page) Record {
	tpos := IntegerSize
	txNum := p.GetInt(tpos)

	fpos := tpos + IntegerSize
	filename := p.GetString(fpos)

	bpos := fpos + len(filename) + IntegerSize
	blockNumber := p.GetInt(bpos)
	blockID := &file.BlockID{Filename: filename, BlockNumber: blockNumber}

	opos := bpos + IntegerSize
	offset := p.GetInt(opos)

	vpos := opos + IntegerSize
	val := p.GetInt(vpos)

	r := &RecordSetInt{
		txNum:  txNum,
		offset: offset,
		val:    val,
		block:  blockID,
	}
	return r
}

func RecordSetIntWriteToLog(lm *log.Manager, txNum int, blockID *file.BlockID, offset int, val int) int {
	tpos := IntegerSize
	fpos := tpos + IntegerSize
	bpos := fpos + len(blockID.Filename) + IntegerSize
	opos := bpos + IntegerSize
	vpos := opos + IntegerSize

	rec := make([]byte, vpos+IntegerSize)
	p := file.NewPageFromBytes(rec)

	p.SetInt(0, int(RecordTypeSetInt))
	p.SetInt(tpos, txNum)
	p.SetString(fpos, blockID.Filename)
	p.SetInt(bpos, blockID.BlockNumber)
	p.SetInt(opos, offset)
	p.SetInt(vpos, val)

	return lm.Append(rec)
}

type RecordSetString struct {
	txNum  int
	offset int
	val    string
	block  *file.BlockID
}

func (r *RecordSetString) Op() RecordType { return RecordTypeSetString }
func (r *RecordSetString) TxNumber() int  { return r.txNum }
func (r *RecordSetString) Undo(tx *Transaction) {
	tx.Pin(r.block)
	tx.SetString(r.block, r.offset, r.val, false)
	tx.Unpin(r.block)
	return
}
func (r *RecordSetString) String() string {
	return fmt.Sprintf("<SETSTRING %d %v %d %s>", r.txNum, r.block, r.offset, r.val)
}

func NewRecordSetString(p *file.Page) Record {
	tpos := IntegerSize
	txNum := p.GetInt(tpos)

	fpos := tpos + IntegerSize
	filename := p.GetString(fpos)

	bpos := fpos + len(filename) + IntegerSize
	blockNumber := p.GetInt(bpos)
	blockID := &file.BlockID{Filename: filename, BlockNumber: blockNumber}

	opos := bpos + IntegerSize
	offset := p.GetInt(opos)

	vpos := opos + IntegerSize
	val := p.GetString(vpos)

	r := &RecordSetString{
		txNum:  txNum,
		offset: offset,
		val:    val,
		block:  blockID,
	}
	return r
}

func RecordSetStringWriteToLog(lm *log.Manager, txNum int, blockID *file.BlockID, offset int, val string) int {
	tpos := IntegerSize
	fpos := tpos + IntegerSize
	bpos := fpos + len(blockID.Filename) + IntegerSize
	opos := bpos + IntegerSize
	vpos := opos + len(val) + IntegerSize

	rec := make([]byte, vpos+IntegerSize)
	p := file.NewPageFromBytes(rec)

	p.SetInt(0, int(RecordTypeSetString))
	p.SetInt(tpos, txNum)
	p.SetString(fpos, blockID.Filename)
	p.SetInt(bpos, blockID.BlockNumber)
	p.SetInt(opos, offset)
	p.SetString(vpos, val)

	return lm.Append(rec)
}

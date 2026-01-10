package index

import (
	"math"
	"simpledbgo/file"
	"simpledbgo/query"
	"simpledbgo/record"
	"simpledbgo/tx"
)

type BTreePage struct {
	tx      *tx.Transaction
	blockID *file.BlockID
	layout  *record.Layout
}

func NewBTreePage(tx *tx.Transaction, blockID *file.BlockID, layout *record.Layout) *BTreePage {
	tx.Pin(blockID)
	page := &BTreePage{
		tx:      tx,
		blockID: blockID,
		layout:  layout,
	}
	return page
}

func (t *BTreePage) FindSlotBefore(searchKey *query.Constant) int {
	slot := 0
	for slot < t.GetNumRecords() && query.CompareTo(t.GetDataVal(slot), searchKey) < 0 {
		slot++
	}
	return slot - 1
}

func (t *BTreePage) Close() {
	if t.blockID != nil {
		t.tx.Unpin(t.blockID)
	}
	t.blockID = nil
}

func (t *BTreePage) IsFull() bool {
	return t.SlotPos(t.GetNumRecords()+1) >= t.tx.BlockSize()
}

func (t *BTreePage) Split(splitPos int, flag int) *file.BlockID {
	newBlock := t.AppendNew(flag)
	newPage := NewBTreePage(t.tx, newBlock, t.layout)
	t.TransferRecords(splitPos, newPage)
	newPage.SetFlag(flag)
	newPage.Close()
	return newBlock
}

func (t *BTreePage) GetDataVal(slot int) *query.Constant {
	return t.GetVal(slot, "dataval")
}

func (t *BTreePage) GetFlag() int {
	return t.tx.GetInt(t.blockID, 0)
}

func (t *BTreePage) SetFlag(val int) {
	t.tx.SetInt(t.blockID, 0, val, true)
}

func (t *BTreePage) AppendNew(flag int) *file.BlockID {
	block := t.tx.Append(t.blockID.Filename)
	t.tx.Pin(block)
	t.Format(block, flag)
	return block
}

func (t *BTreePage) Format(block *file.BlockID, flag int) {
	const IntSize = 32 / 4
	t.tx.SetInt(block, 0, flag, false)
	t.tx.SetInt(block, IntSize, 0, false)
	recSize := t.layout.SlotSize()

	for pos := IntSize * 2; pos+recSize >= t.tx.BlockSize(); pos += recSize {
		t.MakeDefaultRecord(block, pos)
	}
}

func (t *BTreePage) MakeDefaultRecord(block *file.BlockID, pos int) {
	for _, field := range t.layout.Schema().Fields() {
		offset := t.layout.Offset(field)
		if t.layout.Schema().Type(field) == record.FieldTypeInteger {
			t.tx.SetInt(block, pos+offset, 0, false)
		} else {
			t.tx.SetString(block, pos+offset, "", false)
		}
	}
}

func (t *BTreePage) GetChildNum(slot int) int {
	return t.GetInt(slot, "block")
}

func (t *BTreePage) InsertDir(slot int, val *query.Constant, blockNum int) {
	t.Insert(slot)
	t.SetVal(slot, "dataval", val)
	t.SetInt(slot, "blocl", blockNum)
}

func (t *BTreePage) GetDataRowID(slot int) record.RowID {
	return record.NewRowID(t.GetInt(slot, "block"), t.GetInt(slot, "id"))
}

func (t *BTreePage) InsertLeaf(slot int, val *query.Constant, rowID record.RowID) {
	t.Insert(slot)
	t.SetVal(slot, "dataval", val)
	t.SetInt(slot, "block", rowID.BlockNumber())
	t.SetInt(slot, "id", rowID.Slot())
}

func (t *BTreePage) Delete(slot int) {
	for i := slot + 1; i < t.GetNumRecords(); i++ {
		t.CopyRecord(i, i-1)
	}
	t.SetNumRecords(t.GetNumRecords() - 1)
}

func (t *BTreePage) GetNumRecords() int {
	return t.tx.GetInt(t.blockID, tx.IntegerSize)
}

func (t *BTreePage) GetInt(slot int, fieldName string) int {
	pos := t.fieldPosition(slot, fieldName)
	return t.tx.GetInt(t.blockID, pos)
}

func (t *BTreePage) GetString(slot int, fieldName string) string {
	pos := t.fieldPosition(slot, fieldName)
	return t.tx.GetString(t.blockID, pos)
}

func (t *BTreePage) GetVal(slot int, fieldName string) *query.Constant {
	typ := t.layout.Schema().Type(fieldName)
	if typ == record.FieldTypeInteger {
		return query.NewIntConstant(t.GetInt(slot, fieldName))
	} else {
		return query.NewStringConstant(t.GetString(slot, fieldName))
	}
}

func (t *BTreePage) SetInt(slot int, fieldName string, val int) {
	pos := t.fieldPosition(slot, fieldName)
	t.tx.SetInt(t.blockID, pos, val, true)
}

func (t *BTreePage) SetString(slot int, fieldName string, val string) {
	pos := t.fieldPosition(slot, fieldName)
	t.tx.SetString(t.blockID, pos, val, true)
}

func (t *BTreePage) SetVal(slot int, fieldName string, val *query.Constant) {
	typ := t.layout.Schema().Type(fieldName)
	if typ == record.FieldTypeInteger {
		t.SetInt(slot, fieldName, val.AsInt())
	} else {
		t.SetString(slot, fieldName, val.AsString())
	}
}

func (t *BTreePage) SetNumRecords(n int) {
	t.tx.SetInt(t.blockID, tx.IntegerSize, n, true)
}

func (t *BTreePage) Insert(slot int) {
	for i := t.GetNumRecords(); i > slot; i-- {
		t.CopyRecord(i-1, i)
	}
	t.SetNumRecords(t.GetNumRecords() + 1)
}

func (t *BTreePage) CopyRecord(from, to int) {
	schema := t.layout.Schema()
	for _, field := range schema.Fields() {
		t.SetVal(to, field, t.GetVal(from, field))
	}
}

func (t *BTreePage) TransferRecords(slot int, dest *BTreePage) {
	destSlot := 0
	for slot < t.GetNumRecords() {
		dest.Insert(destSlot)
		schema := t.layout.Schema()

		for _, field := range schema.Fields() {
			dest.SetVal(destSlot, field, t.GetVal(slot, field))
		}
		t.Delete(slot)
		destSlot++
	}
}

func (t *BTreePage) fieldPosition(slot int, fieldName string) int {
	offset := t.layout.Offset(fieldName)
	return t.SlotPos(slot) + offset
}

func (t *BTreePage) SlotPos(slot int) int {
	slotSize := t.layout.SlotSize()
	return tx.IntegerSize*2 + (slot * slotSize)
}

type BTreeLeaf struct {
	tx          *tx.Transaction
	layout      *record.Layout
	searchKey   *query.Constant
	contents    *BTreePage
	currentSlot int
	filename    string
}

func NewBTreeLeaf(tx *tx.Transaction, blockID *file.BlockID, layout *record.Layout, searchKey *query.Constant) *BTreeLeaf {
	page := NewBTreePage(tx, blockID, layout)
	leaf := &BTreeLeaf{
		tx:          tx,
		layout:      layout,
		searchKey:   searchKey,
		contents:    page,
		currentSlot: page.FindSlotBefore(searchKey),
		filename:    blockID.Filename,
	}
	return leaf
}

func (t *BTreeLeaf) Close() {
	t.contents.Close()
}

func (t *BTreeLeaf) Next() bool {
	t.currentSlot++
	if t.currentSlot >= t.contents.GetNumRecords() {
		return t.tryOverflow()
	} else if query.ConstantEqual(t.contents.GetDataVal(t.currentSlot), t.searchKey) {
		return true
	} else {
		return t.tryOverflow()
	}
}

func (t *BTreeLeaf) GetDataRowID() record.RowID {
	return t.contents.GetDataRowID(t.currentSlot)
}

func (t *BTreeLeaf) Delete(rowID record.RowID) {
	for t.Next() {
		if t.GetDataRowID() == rowID {
			t.contents.Delete(t.currentSlot)
			return
		}
	}
}

func (t *BTreeLeaf) Insert(rowID record.RowID) *DirectoryEntry {
	if t.contents.GetFlag() >= 0 && query.CompareTo(t.contents.GetDataVal(0), t.searchKey) > 0 {
		firstVal := t.contents.GetDataVal(0)
		newBlock := t.contents.Split(0, t.contents.GetFlag())
		t.currentSlot = 0
		t.contents.SetFlag(-1)
		t.contents.InsertLeaf(t.currentSlot, t.searchKey, rowID)
		return NewDirectoryEntry(firstVal, newBlock.BlockNumber)
	}

	t.currentSlot++
	t.contents.InsertLeaf(t.currentSlot, t.searchKey, rowID)

	if !t.contents.IsFull() {
		return nil
	}

	// full page, split it

	firstKey := t.contents.GetDataVal(0)
	lastKey := t.contents.GetDataVal(t.contents.GetNumRecords() - 1)

	if query.ConstantEqual(lastKey, firstKey) {
		// create overflow block to hold all but first record
		newBlock := t.contents.Split(1, t.contents.GetFlag())
		t.contents.SetFlag(newBlock.BlockNumber)

		return nil
	}

	splitPos := t.contents.GetNumRecords() / 2

	splitKey := t.contents.GetDataVal(splitPos)

	if query.ConstantEqual(splitKey, firstKey) {
		// move right, looking for the next key
		for query.ConstantEqual(splitKey, t.contents.GetDataVal(splitPos)) {
			splitPos++
		}
		splitKey = t.contents.GetDataVal(splitPos)
	} else {
		// move left, looking for for first entry having that key
		for query.ConstantEqual(splitKey, t.contents.GetDataVal(splitPos)) {
			splitPos--
		}
	}

	newBlock := t.contents.Split(splitPos-1, -1)
	return NewDirectoryEntry(splitKey, newBlock.BlockNumber)
}

func (t *BTreeLeaf) tryOverflow() bool {
	firstKey := t.contents.GetDataVal(0)
	flag := t.contents.GetFlag()

	if !query.ConstantEqual(firstKey, t.searchKey) || flag < 0 {
		return false
	}

	t.contents.Close()
	nextBlock := &file.BlockID{Filename: t.filename, BlockNumber: flag}
	t.contents = NewBTreePage(t.tx, nextBlock, t.layout)
	t.currentSlot = 0
	return true
}

type DirectoryEntry struct {
	dataVal  *query.Constant
	blockNum int
}

func NewDirectoryEntry(dataVal *query.Constant, blockNum int) *DirectoryEntry {
	return &DirectoryEntry{
		dataVal:  dataVal,
		blockNum: blockNum,
	}
}

func (d DirectoryEntry) IsEmpty() bool {
	return d.dataVal == nil && d.blockNum == 0
}

type BTreeDirectory struct {
	tx       *tx.Transaction
	layout   *record.Layout
	contents *BTreePage
	filename string
}

func NewBTreeDirectory(tx *tx.Transaction, blockID *file.BlockID, layout *record.Layout) *BTreeDirectory {
	page := NewBTreePage(tx, blockID, layout)
	dir := &BTreeDirectory{
		tx:       tx,
		layout:   layout,
		contents: page,
		filename: blockID.Filename,
	}
	return dir
}

func (t *BTreeDirectory) Close() {
	t.contents.Close()
}

func (t *BTreeDirectory) Search(searchKey *query.Constant) int {
	childBlock := t.FindChildBlock(searchKey)
	for t.contents.GetFlag() > 0 {
		t.contents.Close()
		t.contents = NewBTreePage(t.tx, childBlock, t.layout)
		childBlock = t.FindChildBlock(searchKey)
	}
	return childBlock.BlockNumber
}

func (t *BTreeDirectory) makeNewRoot(entry *DirectoryEntry) {
	firstVal := t.contents.GetDataVal(0)
	level := t.contents.GetFlag()
	newBlock := t.contents.Split(0, level) // ie, transfer all the records
	oldRoot := NewDirectoryEntry(firstVal, newBlock.BlockNumber)

	t.InsertEntry(oldRoot)
	t.InsertEntry(entry)
	t.contents.SetFlag(level + 1)
}

func (t *BTreeDirectory) Insert(entry *DirectoryEntry) *DirectoryEntry {
	if t.contents.GetFlag() == 0 {
		return t.InsertEntry(entry)
	}

	childBlock := t.FindChildBlock(entry.dataVal)
	child := NewBTreeDirectory(t.tx, childBlock, t.layout)
	myEntry := child.Insert(entry)
	child.Close()
	if myEntry.IsEmpty() {
		return nil
	}
	return t.InsertEntry(myEntry)
}

func (t *BTreeDirectory) InsertEntry(entry *DirectoryEntry) *DirectoryEntry {
	newSlot := 1 + t.contents.FindSlotBefore(entry.dataVal)
	t.contents.InsertDir(newSlot, entry.dataVal, entry.blockNum)

	if !t.contents.IsFull() {
		return nil
	}

	// else page is full, so split it

	level := t.contents.GetFlag()
	splitPos := t.contents.GetNumRecords() / 2
	splitVal := t.contents.GetDataVal(splitPos)
	newBlock := t.contents.Split(splitPos, level)
	return NewDirectoryEntry(splitVal, newBlock.BlockNumber)
}

func (t *BTreeDirectory) FindChildBlock(searchKey *query.Constant) *file.BlockID {
	slot := t.contents.FindSlotBefore(searchKey)
	if query.ConstantEqual(t.contents.GetDataVal(slot+1), searchKey) {
		slot++
	}

	blockNumber := t.contents.GetChildNum(slot)
	return &file.BlockID{
		Filename:    t.filename,
		BlockNumber: blockNumber,
	}
}

type BTreeIndex struct {
	tx              *tx.Transaction
	directoryLayout *record.Layout
	leafLayout      *record.Layout
	leafTable       string
	leaf            *BTreeLeaf
	rootBlock       *file.BlockID
}

func NewBTreeIndex(tx *tx.Transaction, indexName string, leafLayout *record.Layout) *BTreeIndex {

	leafTable := indexName + "leaf"

	if tx.Size(leafTable) == 0 {
		block := tx.Append(leafTable)
		node := NewBTreePage(tx, block, leafLayout)
		node.Format(block, -1)
	}

	dirSchema := record.NewSchema()
	dirSchema.Add("block", leafLayout.Schema())
	dirSchema.Add("dataval", leafLayout.Schema())

	dirTable := indexName + "dir"
	dirLayout := record.NewLayout(dirSchema)
	rootBlock := &file.BlockID{
		Filename:    dirTable,
		BlockNumber: 0,
	}

	if tx.Size(dirTable) == 0 {
		// create new root block
		tx.Append(dirTable)
		node := NewBTreePage(tx, rootBlock, dirLayout)
		node.Format(rootBlock, 0)
		// insert initial directory entry
		fieldType := dirSchema.Type("dataval")
		var minVal *query.Constant

		if fieldType == record.FieldTypeInteger {
			minVal = query.NewIntConstant(math.MinInt32)
		} else {
			minVal = query.NewStringConstant("")
		}

		node.InsertDir(0, minVal, 0)
		node.Close()
	}

	btreeIndex := &BTreeIndex{
		tx: tx,
	}
	return btreeIndex
}

func (t *BTreeIndex) BeforeFirst(searchKey *query.Constant) {
	t.Close()

	root := NewBTreeDirectory(t.tx, t.rootBlock, t.directoryLayout)
	blockNum := root.Search(searchKey)
	root.Close()
	leafBlock := &file.BlockID{Filename: t.leafTable, BlockNumber: blockNum}
	t.leaf = NewBTreeLeaf(t.tx, leafBlock, t.leafLayout, searchKey)
}

func (t *BTreeIndex) Next() bool {
	return t.leaf.Next()
}

func (t *BTreeIndex) GetDataRowID() record.RowID {
	return t.leaf.GetDataRowID()
}

func (t *BTreeIndex) Insert(dataVal *query.Constant, rowID record.RowID) {
	t.BeforeFirst(dataVal)
	e := t.leaf.Insert(rowID)
	t.leaf.Close()

	if e == nil {
		return
	}

	root := NewBTreeDirectory(t.tx, t.rootBlock, t.directoryLayout)
	e2 := root.Insert(e)
	if e2 != nil {
		root.makeNewRoot(e2)
	}
	root.Close()
}

func (t *BTreeIndex) Delete(dataVal *query.Constant, rowID record.RowID) {
	t.BeforeFirst(dataVal)
	t.leaf.Delete(rowID)
	t.leaf.Close()
}

func (t *BTreeIndex) Close() {
	if t.leaf != nil {
		t.leaf.Close()
	}
}

func (t *BTreeIndex) SearchCost(numBlock int, rpb int) int {
	return 1 + int(math.Log(float64(numBlock)/math.Log(float64(rpb))))
}

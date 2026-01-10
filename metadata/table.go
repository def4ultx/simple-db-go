package metadata

import (
	"simpledbgo/operator"
	"simpledbgo/record"
	"simpledbgo/tx"
)

const MaxName = 16

type TableManager struct {
	tableCatalogLayout *record.Layout
	fieldCatalogLayout *record.Layout
}

func NewTableManager(isNew bool, tx *tx.Transaction) *TableManager {
	tableCatalogSchema := record.NewSchema()
	fieldCatalogSchema := record.NewSchema()

	tableCatalogSchema.AddStringFiled("tblname", MaxName)
	tableCatalogSchema.AddIntField("slotsize")

	fieldCatalogSchema.AddStringFiled("tblname", MaxName)
	fieldCatalogSchema.AddStringFiled("fldname", MaxName)

	fieldCatalogSchema.AddIntField("type")
	fieldCatalogSchema.AddIntField("length")
	fieldCatalogSchema.AddIntField("offset")

	tm := &TableManager{
		tableCatalogLayout: record.NewLayout(tableCatalogSchema),
		fieldCatalogLayout: record.NewLayout(fieldCatalogSchema),
	}

	if isNew {
		tm.CreateTable("tblcat", tableCatalogSchema, tx)
		tm.CreateTable("fldcat", fieldCatalogSchema, tx)
	}
	return tm
}

func (t *TableManager) CreateTable(tableName string, schema *record.Schema, tx *tx.Transaction) {
	layout := record.NewLayout(schema)

	// insert 1 record into tblcat
	tableCatalog := operator.NewTableScan(tx, "tblcat", t.tableCatalogLayout)
	tableCatalog.Insert()
	tableCatalog.SetString("tblname", tableName)
	tableCatalog.SetInt("slotsize", layout.SlotSize())
	tableCatalog.Close()

	// insert a record into fldcat for each field
	fieldCatalog := operator.NewTableScan(tx, "fldcat", t.fieldCatalogLayout)
	for _, fieldName := range schema.Fields() {
		fieldCatalog.Insert()
		fieldCatalog.SetString("tblname", tableName)
		fieldCatalog.SetString("fldname", fieldName)

		fieldCatalog.SetInt("type", schema.Type(fieldName))
		fieldCatalog.SetInt("length", schema.Length(fieldName))
		fieldCatalog.SetInt("offset", layout.Offset(fieldName))
	}
	fieldCatalog.Close()
}

func (t *TableManager) GetLayout(tableName string, tx *tx.Transaction) *record.Layout {
	size := -1

	tableCatalog := operator.NewTableScan(tx, "tblcat", t.tableCatalogLayout)
	for tableCatalog.Next() {
		if tableCatalog.GetString("tblname") == tableName {
			size = tableCatalog.GetInt("slotsize")
			break
		}
	}
	tableCatalog.Close()

	schema := record.NewSchema()
	offsets := make(map[string]int)

	fieldCatalog := operator.NewTableScan(tx, "fldcat", t.fieldCatalogLayout)
	for fieldCatalog.Next() {
		if fieldCatalog.GetString("tblname") == tableName {
			fieldName := fieldCatalog.GetString("fldname")

			fieldType := fieldCatalog.GetInt("type")
			fieldLength := fieldCatalog.GetInt("length")
			fieldOffset := fieldCatalog.GetInt("offset")

			offsets[fieldName] = fieldOffset
			schema.AddField(fieldName, fieldType, fieldLength)
		}
	}
	fieldCatalog.Close()

	return record.NewLayoutWithOffset(schema, offsets, size)
}

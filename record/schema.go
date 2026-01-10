package record

import "slices"

const (
	FieldTypeInteger = 0
	FieldTypeString  = 1
)

type FieldInfo struct {
	typ    int
	length int
}

type Schema struct {
	fields []string
	infos  map[string]FieldInfo
}

func NewSchema() *Schema {
	sch := &Schema{
		fields: make([]string, 0),
		infos:  make(map[string]FieldInfo),
	}
	return sch
}

func (s *Schema) AddField(fieldName string, typ int, length int) {
	s.fields = append(s.fields, fieldName)
	s.infos[fieldName] = FieldInfo{typ: typ, length: length}
}

func (s *Schema) AddIntField(fieldName string) {
	s.AddField(fieldName, FieldTypeInteger, 0)
}

func (s *Schema) AddStringFiled(fieldName string, length int) {
	s.AddField(fieldName, FieldTypeString, length)
}

func (s *Schema) Add(fieldName string, schema *Schema) {
	typ := schema.Type(fieldName)
	len := schema.Length(fieldName)
	s.AddField(fieldName, typ, len)
}

func (s *Schema) AddAll(schema *Schema) {
	for _, f := range schema.Fields() {
		s.Add(f, schema)
	}
}

func (s *Schema) Fields() []string {
	return s.fields
}

func (s *Schema) HasField(fieldName string) bool {
	return slices.Contains(s.fields, fieldName)
}

func (s *Schema) Type(fieldName string) int {
	return s.infos[fieldName].typ
}

func (s *Schema) Length(fieldName string) int {
	return s.infos[fieldName].length
}

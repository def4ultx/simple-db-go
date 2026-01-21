package index

type StatInfo struct {
	NumBlock int
	NumRec   int
}

func (si StatInfo) BlocksAccessed() int {
	return si.NumBlock
}

func (si StatInfo) RecordsOutput() int {
	return si.NumRec
}

func (si StatInfo) DistinctValues(fieldName string) int {
	return 1 + (si.NumRec / 3) // Not accurate
}

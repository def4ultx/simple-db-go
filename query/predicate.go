package query

import (
	"fmt"
	"simpledbgo/record"
	"simpledbgo/types"
)

type Predicate struct {
	terms []*Term
}

func NewPredicate() *Predicate {
	return &Predicate{terms: make([]*Term, 0)}
}

func NewPredicateWithTerm(t *Term) *Predicate {
	return &Predicate{terms: []*Term{t}}
}

func (p *Predicate) ConjoinWith(predicate *Predicate) {
	for _, pred := range predicate.terms {
		p.terms = append(p.terms, pred)
	}
}

func (p *Predicate) IsSatisfied(scan types.Scan) bool {
	for _, t := range p.terms {
		if !t.IsSatisfied(scan) {
			return false
		}
	}
	return true
}

func (p *Predicate) ReductionFactor(pl types.Plan) int {
	factor := 1
	for _, t := range p.terms {
		factor *= t.ReductionFactor(pl)
	}
	return factor
}

func (p *Predicate) SelectSubPredicate(schema *record.Schema) *Predicate {
	result := NewPredicate()

	for _, t := range p.terms {
		if t.AppliedTo(schema) {
			result.terms = append(result.terms, t)
		}
	}

	if len(result.terms) == 0 {
		return nil
	}
	return result
}

func (p *Predicate) JoinSubPred(sch1, sch2 *record.Schema) *Predicate {
	result := NewPredicate()
	schema := record.NewSchema()
	schema.AddAll(sch1)
	schema.AddAll(sch2)

	for _, t := range p.terms {
		if !t.AppliedTo(sch1) && !t.AppliedTo(sch2) && t.AppliedTo(schema) {
			result.terms = append(result.terms, t)
		}
	}

	if len(result.terms) == 0 {
		return nil
	}
	return result
}

func (p *Predicate) EquatesWithConstant(fieldName string) *Constant {
	for _, t := range p.terms {
		c := t.EquatesWithConstant(fieldName)
		if c != nil {
			return c
		}
	}
	return nil
}

func (p *Predicate) EquatesWithField(fieldName string) *string {
	for _, t := range p.terms {
		c := t.EquatesWithField(fieldName)
		if c != nil {
			return c
		}
	}
	return nil
}

//   Iterator<Term> iter = terms.iterator();
//   if (!iter.hasNext())
//      return "";
//   String result = iter.next().toString();
//   while (iter.hasNext())
//      result += " and " + iter.next().toString();
//   return result;

func (p *Predicate) AsString() string {
	if len(p.terms) == 0 {
		return ""
	}

	result := p.terms[0].AsString()
	for _, t := range p.terms[1:] {
		result += " and " + t.AsString()
	}
	return result
}

func (p Predicate) String() string {
	result := "<TERM: "

	for _, t := range p.terms {
		result += fmt.Sprintf("%v,", *t)
	}
	return result + ">"
}

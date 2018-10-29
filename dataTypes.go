package main

import (
	"fmt"
	"sort"
	"sync/atomic"
)

// Class statistics
// - a string IRI (str) and
// - its support, i.e. its total number of occurrences (totalCount)
type iType struct {
	Str        *string
	TotalCount uint32
}

func (t *iType) increment() {
	atomic.AddUint32(&t.TotalCount, 1)
}

type typeMap map[string]*iType

func (m *typeMap) get(iri string) *iType {
	item, ok := (*m)[iri]
	if !ok {
		item = &iType{&iri, 0}
		(*m)[iri] = item
	}
	return item
}

// A struct capturing
// - a string IRI (str) and
// - its support, i.e. its total number of occurrences (totalCount)
// - an integer indicating sort order
type iItem struct {
	Str              *string
	TotalCount       uint32
	SortOrder        uint16
	traversalPointer *schemaNode // node traversal pointer
}

func (p *iItem) increment() {
	atomic.AddUint32(&p.TotalCount, 1)
}

func (p iItem) String() string {
	return fmt.Sprint(p.TotalCount, "x\t", *p.Str, " (", p.SortOrder, ")")
}

type propMap map[string]*iItem

func (m *propMap) get(iri string) (item *iItem) { // TODO: Implement sameas Mapping/Resolution to single group identifier upon insert!
	item, ok := (*m)[iri]
	if !ok {
		item = &iItem{&iri, 0, uint16(len(*m)), nil}
		(*m)[iri] = item
	}
	return
}

// An array of pointers to IRI structs
type iList []*iItem

// sort the list according to the current iList sort order
func (l *iList) sort() {
	// sort the properties according to the current iList sort order
	sort.Slice(*l, func(i, j int) bool { return (*l)[i].SortOrder < (*l)[j].SortOrder })
}

// inplace sorting and deduplication.
func (l *iList) sortAndDeduplicate() {
	l.sort()

	// inplace deduplication
	j := 0
	for i := 1; i < len(*l); i++ {
		if (*l)[j] == (*l)[i] {
			continue
		}
		j++
		(*l)[i], (*l)[j] = (*l)[j], (*l)[i]
	}
	*l = (*l)[:j+1]
}

func (l *iList) toSet() map[*iItem]bool {
	pSet := make(map[*iItem]bool, len(*l))
	for _, p := range *l {
		pSet[p] = true
	}
	return pSet
}

func (l iList) String() string {
	//// list representation (includes duplicates)
	o := "[ "
	for i := 0; i < len(l); i++ {
		o += *(l[i].Str) + " "
	}
	return o + "]"

	//// couter presentation (loses order)
	// ctr := make(map[string]int)
	// for i := 0; i < len(p); i++ {
	// 	ctr[p[i].str]++
	// }
	// return fmt.Sprint(ctr)
}

// struct to rank suggestions
type rankedPropertyCandidate struct {
	Property    *iItem
	Probability float64
}

type propertyRecommendations []rankedPropertyCandidate

func (ps propertyRecommendations) String() string {
	s := ""
	for _, p := range ps {
		s += fmt.Sprintf("%v: %v\n", *p.Property.Str, p.Probability)
	}
	return s
}

type rankedTypeCandidate struct {
	Class       *iType
	Probability float64
}
type typeRecommendations []rankedTypeCandidate

func (ts typeRecommendations) String() string {
	s := ""
	for _, t := range ts {
		s += fmt.Sprintf("%v: %v\n", *t.Class.Str, t.Probability)
	}
	return s
}

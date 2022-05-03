package schematree

import (
	"encoding/gob"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/lgleim/SchemaTreeRecommender/schematree/serialization"
	"google.golang.org/protobuf/proto"

	gzip "github.com/klauspost/pgzip"
)

// TypedSchemaTree is a schematree that includes type information as property nodes
type SchemaTree struct {
	PropMap propMap    // PropMap maps the string representations of properties to the corresponding IItem
	Root    SchemaNode // Root is the root node of the schematree. All further nodes are descendants of this node.
	MinSup  uint32     // TODO (not used)
	Typed   bool       // Typed indicates if this schematree includes type information as properties
}

// Create creates a new schema tree from given dataset with given first n subjects, typed and minSup
func Create(filename string, firstNsubjects uint64, typed bool, minSup uint32) *SchemaTree {

	schema := New(typed, minSup)
	schema.TwoPass(filename, uint64(firstNsubjects))

	PrintMemUsage()
	return schema
}

// New returns a newly allocated and initialized schema tree
func New(typed bool, minSup uint32) (tree *SchemaTree) {
	if minSup < 1 {
		minSup = 1
	}

	pMap := make(propMap)
	tree = &SchemaTree{
		PropMap: pMap,
		Root:    newRootNode(pMap),
		MinSup:  minSup,
		Typed:   typed,
	}
	tree.init()
	return
}

// Init initializes the datastructure for usage
func (tree *SchemaTree) init() {
	for i := range globalItemLocks {
		globalItemLocks[i] = &sync.Mutex{}
		globalNodeLocks[i] = &sync.RWMutex{}
	}
}

// Insert inserts all properties of a new subject into the schematree
// The subject is given by
// thread-safe
func (tree *SchemaTree) Insert(e *SubjectSummary) {

	// transform into iList of properties
	properties := make(IList, len(e.Properties), len(e.Properties))
	i := 0
	for p := range e.Properties {
		properties[i] = p
		i++
	}

	// sort the properties descending by support
	properties.Sort()

	// insert sorted item list into the schemaTree
	node := &tree.Root
	node.incrementSupport()
	for _, prop := range properties {
		node = node.getOrCreateChild(prop) // recurse, i.e., node.getOrCreateChild(prop).insert(properties[1:], types)
		node.incrementSupport()
	}

}

// updateSortOrder updates iList according to actual frequencies
// calling this directly WILL BREAK non-empty schema trees
// Runtime: O(n*log(n)), Memory: O(n)
func (tree *SchemaTree) updateSortOrder() {
	// make a list of all known properties
	// Runtime: O(n), Memory: O(n)
	iList := make(IList, len(tree.PropMap))
	i := 0
	for _, v := range tree.PropMap {
		iList[i] = v
		i++
	}

	// sort by descending support. In case of equal support, lexicographically
	// Runtime: O(n*log(n)), Memory: -
	sort.Slice(
		iList,
		func(i, j int) bool {
			if iList[i].TotalCount != iList[j].TotalCount {
				return iList[i].TotalCount > iList[j].TotalCount
			}
			return *(iList[i].Str) < *(iList[j].Str)
		})

	// update term's internal sortOrder
	// Runtime: O(n), Memory: -
	for i, v := range iList {
		v.SortOrder = uint32(i)
	}
}

// Support returns the total cooccurrence-frequency of the given property list
func (tree *SchemaTree) Support(properties IList) uint32 {
	var support uint32

	if len(properties) == 0 {
		return tree.Root.Support // empty set occured in all transactions
	}

	properties.Sort() // descending by support

	// check all branches that include least frequent term
	for term := properties[len(properties)-1].traversalPointer; term != nil; term = term.nextSameID {
		if term.prefixContains(properties) {
			support += term.Support
		}
	}

	return support
}

func (tree *SchemaTree) SaveProtocolBuffer(filePath string) error {
	t1 := time.Now()
	fmt.Printf("Writing schema to protocol buffer file %v... ", filePath)

	pb_tree := &serialization.SchemaTree{}

	// encode propMap
	pb_propmap := &serialization.PropMap{}
	// first get them in order
	props := make([]*IItem, len(tree.PropMap), len(tree.PropMap))
	for _, p := range tree.PropMap {
		props[int(p.SortOrder)] = p
	}
	//then store them in order
	for _, p := range props {
		pb_propmap_item := &serialization.PropMapItem{
			Str:        *p.Str,
			TotalCount: p.TotalCount,
			SortOrder:  p.SortOrder,
		}
		pb_propmap.Items = append(pb_propmap.Items, pb_propmap_item)
	}

	pb_tree.PropMap = pb_propmap
	// encode MinSup

	pb_tree.MinSup = tree.MinSup

	// encode root
	var root *serialization.SchemaNode = tree.Root.AsProtoSchemaNode()

	pb_tree.Root = root

	// encode Typed
	if tree.Typed {
		pb_tree.Options = []serialization.Options{serialization.Options_TYPED}
	} else {
		//no action needed. The default is an empty option list, wich is fine
	}

	out, err := proto.Marshal(pb_tree)
	if err != nil {
		return err
	}
	// TODO check whether gzip compression helps

	if err := os.WriteFile(filePath, out, 0644); err != nil {
		log.Fatalln("Failed to write address book:", err)
	}

	if err == nil {
		fmt.Printf("done (%v)\n", time.Since(t1))
	} else {
		fmt.Printf("Saving schema failed with error: %v\n", err)
	}
	return err
}

// SaveGob stores a binarized version of the schematree to the given filepath
func (tree *SchemaTree) SaveGob(filePath string) error {
	t1 := time.Now()
	fmt.Printf("Writing schema to file %v... ", filePath)

	f, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer f.Close()

	w := gzip.NewWriter(f)
	defer w.Close()

	e := gob.NewEncoder(w)

	// encode propMap
	props := make([]*IItem, len(tree.PropMap), len(tree.PropMap))
	for _, p := range tree.PropMap {
		props[int(p.SortOrder)] = p
	}
	err = e.Encode(props)
	if err != nil {
		return err
	}

	// encode MinSup

	err = e.Encode(tree.MinSup)
	if err != nil {
		return err
	}

	// encode root
	err = tree.Root.writeGob(e)

	// encode Typed
	if tree.Typed {
		err = e.Encode(1)
	} else {
		err = e.Encode(2)
	}
	err = e.Encode(tree.Typed)
	if err != nil {
		return err
	}

	if err == nil {
		fmt.Printf("done (%v)\n", time.Since(t1))
	} else {
		fmt.Printf("Saving schema failed with error: %v\n", err)
	}

	return err
}

func LoadProtocolBuffer(filePath string) (*SchemaTree, error) {
	fmt.Printf("Loading schema (from file %v): ", filePath)
	in, err := os.ReadFile(filePath)
	if err != nil {
		log.Fatalln("Error reading file:", err)
	}
	return loadProtocolBuffer(in)
}

func loadProtocolBuffer(in []byte) (*SchemaTree, error) {
	t1 := time.Now()
	pb_tree := &serialization.SchemaTree{}
	if err := proto.Unmarshal(in, pb_tree); err != nil {
		return nil, err
	}

	tree := New(false, 1)

	// decode propMap
	var props []*IItem

	for _, pb_item := range pb_tree.PropMap.Items {
		// This sortorder was overwritten in the gob implementation, but that seems unnecesary.
		// sortOrder was the index in the items array, but that is already set in the item anyway
		// item.SortOrder = uint32(sortOrder)
		tree.PropMap[pb_item.Str] = &IItem{
			Str:        &pb_item.Str,
			TotalCount: pb_item.TotalCount,
			SortOrder:  pb_item.SortOrder,
			// TODO: check whether it is okay to not have the traverselpointer here
			traversalPointer: nil,
		}
	}
	fmt.Printf("%v properties... ", len(props))

	// decode MinSup
	tree.MinSup = pb_tree.MinSup

	// decode Root
	fmt.Printf("decoding tree...")
	tree.Root = *FromProtoSchemaNode(pb_tree.Root, props)

	//decode Typed
	for _, option := range pb_tree.Options {
		switch option {
		case serialization.Options_TYPED:
			tree.Typed = true
		default:
			log.Fatal("Unknown option in protocol buffer tree")
		}
	}

	fmt.Println("Time for decoding ", time.Since(t1), " seconds")
	return tree, nil

}

// LoadGob loads a binarized SchemaTree from disk
func LoadGob(filePath string) (*SchemaTree, error) {
	// Alternatively via GobDecoder(...): https://stackoverflow.com/a/12854659

	fmt.Printf("Loading schema (from file %v): ", filePath)
	t1 := time.Now()

	/// file handling
	f, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Encountered error while trying to open the file: %v\n", err)
		return nil, err
	}

	r, err := gzip.NewReader(f)
	if err != nil {
		fmt.Printf("Encountered error while trying to decompress the file: %v\n", err)
		return nil, err
	}
	defer r.Close()

	/// decoding
	tree := New(false, 1)
	d := gob.NewDecoder(r)

	// decode propMap
	var props []*IItem
	err = d.Decode(&props)
	if err != nil {
		return nil, err
	}
	for sortOrder, item := range props {
		item.SortOrder = uint32(sortOrder)
		tree.PropMap[*item.Str] = item
	}
	fmt.Printf("%v properties... ", len(props))

	// decode MinSup
	err = d.Decode(&tree.MinSup)
	if err != nil {
		return nil, err
	}

	// decode Root
	fmt.Printf("decoding tree...")
	err = tree.Root.decodeGob(d, props)

	// legacy import bug workaround
	if *tree.Root.ID.Str != "root" {
		fmt.Println("WARNING!!! Encountered legacy root node import bug - root node counts will be incorrect!")
		tree.Root.ID = tree.PropMap.get("root")
	}

	//decode Typed
	var i int
	err = d.Decode(&i)
	if i == 1 {
		tree.Typed = true
	}
	if err != nil {
		return nil, err
	}

	if err != nil {
		fmt.Printf("Encountered error while decoding the file: %v\n", err)
		return nil, err
	}

	fmt.Println(time.Since(t1))
	return tree, err
}

// first pass: collect I-List and statistics
func (tree *SchemaTree) firstPass(fileName string, firstN uint64) {
	//	if _, err := os.Stat(fileName + ".firstPass.bin"); os.IsNotExist(err) {
	counter := func(s *SubjectSummary) {
		for prop := range s.Properties {
			prop.increment()
		}
	}

	t1 := time.Now()
	subjectCount := SubjectSummaryReader(fileName, tree.PropMap, counter, firstN, tree.Typed)
	propCount, typeCount := tree.PropMap.count()

	fmt.Printf("%v subjects, %v properties, %v types\n", subjectCount, propCount, typeCount)

	// f, _ := os.Create(fileName + ".propMap")
	// gob.NewEncoder(f).Encode(schema.propMap)
	// f.Close()
	// f, _ = os.Create(fileName + ".typeMap")
	// gob.NewEncoder(f).Encode(schema.typeMap)
	// f.Close()

	tree.updateSortOrder()

	fmt.Println("First Pass:", time.Since(t1))
	PrintMemUsage()

	const MaxUint32 = uint64(^uint32(0))
	if subjectCount > MaxUint32 {
		fmt.Print("\n#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#\n\n")
		fmt.Printf("WARNING: uint32 OVERFLOW - Processed %v subjects but tree can only track support up to %v!\n", subjectCount, MaxUint32)
		fmt.Print("\n#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#!#\n\n")
	}

	// Disabled saving the firstPass.bin for now, because using it between untyped and typed
	// trees can possibly lead to unexpected errors.
	//
	// err := tree.Save(fileName + ".firstPass.bin")
	// if err != nil {
	// 	log.Fatalln(err)
	// }

	//} else {
	// f1, err1 := os.Open(fileName + ".propMap")
	// f2, err2 := os.Open(fileName + ".typeMap")

	// if err1 == nil && err2 == nil {
	// 	fmt.Print("Loading type- and propertyMap directly from corresponding gobs: ")
	// 	tmp := New(false, 1)
	// 	gob.NewDecoder(f1).Decode(&tmp.propMap)
	// 	gob.NewDecoder(f2).Decode(&tmp.typeMap)
	// 	tmp.updateSortOrder()
	// 	*schema = *tmp
	// 	fmt.Printf("%v properties, %v types\n", len(tmp.propMap), len(tmp.typeMap))
	// } else {

	// TODO: Think whether its OK to re-use existing files on build step (maybe with optional arg)
	//	tmp, err := Load(fileName + ".firstPass.bin")
	//	if err != nil {
	//		log.Fatalln(err)
	//	}
	//*tree = *tmp
	// }
	//	}
}

// build schema tree
func (tree *SchemaTree) secondPass(fileName string, firstN uint64) {
	tree.updateSortOrder() // duplicate -- legacy compatability

	inserter := func(s *SubjectSummary) {
		tree.Insert(s)
	}

	// go countTreeNodes(schema)

	t1 := time.Now()
	SubjectSummaryReader(fileName, tree.PropMap, inserter, firstN, tree.Typed)

	fmt.Println("Second Pass:", time.Since(t1))
	PrintMemUsage()
	// PrintLockStats()
}

// TwoPass constructs a SchemaTree from the firstN subjects of the given NTriples file using a two-pass approach
func (tree *SchemaTree) TwoPass(fileName string, firstN uint64) {
	// go func() {
	// 	for true {
	// 		time.Sleep(10 * time.Second)
	// 		PrintMemUsage()
	// 	}
	// }()
	tree.firstPass(fileName, firstN)
	tree.secondPass(fileName, firstN)
}

// WritePropFreqs writes all Properties together with their Support to the given File as CSV
func (tree SchemaTree) WritePropFreqs(file string) {
	f, err := os.Create(file)
	if err != nil {
		log.Fatalln("Could not open file to writePropFreqs!")
	}
	defer f.Close()

	f.WriteString("URI;Frequency\n")
	for uri, item := range tree.PropMap {
		if item.IsProp() {
			f.WriteString(fmt.Sprintf("%v;%v\n", uri, item.TotalCount))
		}
	}
}

// WriteTypeFreqs writes all Types together with their Support to the given File as CSV
func (tree SchemaTree) WriteTypeFreqs(file string) {
	f, err := os.Create(file)
	if err != nil {
		log.Fatalln("Could not open file to writeTypeFreqs!")
	}
	defer f.Close()

	f.WriteString("URI;Frequency\n")
	for uri, item := range tree.PropMap {
		if item.IsType() {
			f.WriteString(fmt.Sprintf("%v;%v\n", strings.TrimPrefix(uri, "t#"), item.TotalCount))
		}
	}
}

// String returns the string represantation of the schema tree
func (tree SchemaTree) String() string {
	var minSupport uint32 = 100000
	s := "digraph schematree { newrank=true; labelloc=b; color=blue; fontcolor=blue; style=dotted;\n"

	s += tree.Root.graphViz(minSupport)

	cluster := ""

	for _, prop := range tree.PropMap {
		cluster = ""
		for node := prop.traversalPointer; node != nil; node = node.nextSameID {
			if node.Support >= minSupport {
				cluster += fmt.Sprintf("\"%p\"; ", node)
			}
		}
		if cluster != "" {
			s += fmt.Sprintf("subgraph \"cluster_%v\" { rank=same; label=\"%v\"; %v}\n", prop.Str, *prop.Str, cluster)
		}
	}

	s += "\n"

	return s + "}"
}

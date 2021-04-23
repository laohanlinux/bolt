package bolt

import (
	"container/list"
	"fmt"
	"github.com/awalterschulze/gographviz"
	"io/ioutil"
	"os"
	"strings"
	"testing"
	"unsafe"
)

// Ensure that a node can insert a key/value.
func TestNode_put(t *testing.T) {
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{meta: &meta{pgid: 1}}}}
	n.put([]byte("baz"), []byte("baz"), []byte("2"), 0, 0)
	n.put([]byte("foo"), []byte("foo"), []byte("0"), 0, 0)
	n.put([]byte("bar"), []byte("bar"), []byte("1"), 0, 0)
	n.put([]byte("foo"), []byte("foo"), []byte("3"), 0, leafPageFlag)

	if len(n.inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(n.inodes))
	}
	if k, v := n.inodes[0].key, n.inodes[0].value; string(k) != "bar" || string(v) != "1" {
		t.Fatalf("exp=<bar,1>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[1].key, n.inodes[1].value; string(k) != "baz" || string(v) != "2" {
		t.Fatalf("exp=<baz,2>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[2].key, n.inodes[2].value; string(k) != "foo" || string(v) != "3" {
		t.Fatalf("exp=<foo,3>; got=<%s,%s>", k, v)
	}
	if n.inodes[2].flags != uint32(leafPageFlag) {
		t.Fatalf("not a leaf: %d", n.inodes[2].flags)
	}
}

// Ensure that a node can deserialize from a leaf page.
func TestNode_read_LeafPage(t *testing.T) {
	// Create a page.
	var buf [4096]byte
	page := (*page)(unsafe.Pointer(&buf[0]))
	page.flags = leafPageFlag
	page.count = 2

	// Insert 2 elements at the beginning. sizeof(leafPageElement) == 16
	nodes := (*[3]leafPageElement)(unsafe.Pointer(&page.ptr))
	nodes[0] = leafPageElement{flags: 0, pos: 32, ksize: 3, vsize: 4}  // pos = sizeof(leafPageElement) * 2
	nodes[1] = leafPageElement{flags: 0, pos: 23, ksize: 10, vsize: 3} // pos = sizeof(leafPageElement) + 3 + 4

	// Write data for the nodes at the end.
	data := (*[4096]byte)(unsafe.Pointer(&nodes[2]))
	copy(data[:], []byte("barfooz"))
	copy(data[7:], []byte("helloworldbye"))

	// Deserialize page into a leaf.
	n := &node{}
	n.read(page)

	// Check that there are two inodes with correct data.
	if !n.isLeaf {
		t.Fatal("expected leaf")
	}
	if len(n.inodes) != 2 {
		t.Fatalf("exp=2; got=%d", len(n.inodes))
	}
	if k, v := n.inodes[0].key, n.inodes[0].value; string(k) != "bar" || string(v) != "fooz" {
		t.Fatalf("exp=<bar,fooz>; got=<%s,%s>", k, v)
	}
	if k, v := n.inodes[1].key, n.inodes[1].value; string(k) != "helloworld" || string(v) != "bye" {
		t.Fatalf("exp=<helloworld,bye>; got=<%s,%s>", k, v)
	}
}

// Ensure that a node can serialize into a leaf page.
func TestNode_write_LeafPage(t *testing.T) {
	// Create a node.
	n := &node{isLeaf: true, inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("susy"), []byte("susy"), []byte("que"), 0, 0)
	n.put([]byte("ricki"), []byte("ricki"), []byte("lake"), 0, 0)
	n.put([]byte("john"), []byte("john"), []byte("johnson"), 0, 0)

	// Write it to a page.
	var buf [4096]byte
	p := (*page)(unsafe.Pointer(&buf[0]))
	n.write(p)

	// Read the page back in.
	n2 := &node{}
	n2.read(p)

	// Check that the two pages are the same.
	if len(n2.inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(n2.inodes))
	}
	if k, v := n2.inodes[0].key, n2.inodes[0].value; string(k) != "john" || string(v) != "johnson" {
		t.Fatalf("exp=<john,johnson>; got=<%s,%s>", k, v)
	}
	if k, v := n2.inodes[1].key, n2.inodes[1].value; string(k) != "ricki" || string(v) != "lake" {
		t.Fatalf("exp=<ricki,lake>; got=<%s,%s>", k, v)
	}
	if k, v := n2.inodes[2].key, n2.inodes[2].value; string(k) != "susy" || string(v) != "que" {
		t.Fatalf("exp=<susy,que>; got=<%s,%s>", k, v)
	}
}

func TestNode_split1(t *testing.T) {
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	for i:=0 ; i < 100; i ++ {
		n.put([]byte(fmt.Sprintf("%0x", i)), []byte(fmt.Sprintf("%0x", i)), []byte(fmt.Sprintf("%0x", i)), 0, 0)
	}
	n.split(100)
	err := n.spill()
	if err != nil {
		t.Fatalf("expected nil, actual %v", err)
	}
	PrintDag(n.root(), "TestSpill.dot")
}

// Ensure that a node can split into appropriate subgroups.
func TestNode_split(t *testing.T) {
	// Create a node.
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	t.Log(n.isLeaf)

	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000003"), []byte("00000003"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000004"), []byte("00000004"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000005"), []byte("00000005"), []byte("0123456701234567"), 0, 0)
	// Split between 2 & 3.
	n.split(100)
	t.Logf("%#v", n)

	var parent = n.parent
	if len(parent.children) != 2 {
		t.Fatalf("exp=2; got=%d", len(parent.children))
	}
	if len(parent.children[0].inodes) != 2 {
		t.Fatalf("exp=2; got=%d", len(parent.children[0].inodes))
	}
	if len(parent.children[1].inodes) != 3 {
		t.Fatalf("exp=3; got=%d", len(parent.children[1].inodes))
	}
	PrintDag(n, "g.dot")
}

// Ensure that a page with the minimum number of inodes just returns a single node.
func TestNode_split_MinKeys(t *testing.T) {
	// Create a node.
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), 0, 0)

	// Split.
	n.split(20)
	if n.parent != nil {
		t.Fatalf("expected nil parent")
	}
}

// Ensure that a node that has keys that all fit on a page just returns one leaf.
func TestNode_split_SinglePage(t *testing.T) {
	// Create a node.
	n := &node{inodes: make(inodes, 0), bucket: &Bucket{tx: &Tx{db: &DB{}, meta: &meta{pgid: 1}}}}
	n.put([]byte("00000001"), []byte("00000001"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000002"), []byte("00000002"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000003"), []byte("00000003"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000004"), []byte("00000004"), []byte("0123456701234567"), 0, 0)
	n.put([]byte("00000005"), []byte("00000005"), []byte("0123456701234567"), 0, 0)

	// Split.
	n.split(4096)
	if n.parent != nil {
		t.Fatalf("expected nil parent")
	}
}


func PrintDag(n *node, fs string) {
	graphAst, _ := gographviz.ParseString(`digraph G {}`)
	g := gographviz.NewGraph()
	if err := gographviz.Analyse(graphAst, g); err != nil {
		panic(err)
	}

	nodeID := func(inodes inodes) string {
		var str []string
		for _, inode := range inodes {
			str = append(str, fmt.Sprintf("%s", inode.key))
		}
		if len(inodes) == 0 {
			return  fmt.Sprintf("\"%s\"", "Root")
		}
		return fmt.Sprintf("\"%s\"", strings.Join(str, ", "))
	}

	if n.root() == nil {
		return
	}

	var stack = list.New()
	stack.PushBack(n.root())
	for stack.Len() > 0 {
		n := stack.Front()
		stack.Remove(n)
		id1 := nodeID(n.Value.(*node).inodes)
		_ = g.AddNode("", id1, nil)
		for _, child := range n.Value.(*node).children {
			id2 := nodeID(child.inodes)
			_ = g.AddNode("", id2, nil)
			_ = g.AddEdge(id1, id2, true, nil)
			stack.PushBack(child)
		}
	}
	_ = os.Remove(fs)
	_ = ioutil.WriteFile(fs, []byte(g.String()), os.ModePerm)
}

package kvdb

import (
	"bytes"
	"sort"
)

type Node struct {
	bucket   *Bucket
	pgid     uint64
	parent   uint64   // pgid of parent node
	typ      uint8    // 0: internal, 1: leaf
	Keys     [][]byte // keys of internal nodes
	children []uint64 // pgid of children nodes
	values   [][]byte // values of leaf nodes
}

func (n Node) findKey(key []byte) (int, bool) {
	for i, k := range n.Keys {
		if bytes.Compare(k, key) == 0 {
			return i, true
		}
	}

	return -1, false
}

func (n *Node) insert(key []byte, value []byte) {
	// find index where key should be inserted
	i := sort.Search(len(n.Keys), func(i int) bool { return bytes.Compare(n.Keys[i], key) != -1 })

	// create new keys array with space for new key
	keys := make([][]byte, len(n.Keys)+1)
	// copy keys before index
	copy(keys, n.Keys[:i])
	// insert new key
	keys[i] = key
	// copy keys after index
	copy(keys[i+1:], n.Keys[i:])
	// set keys
	n.Keys = keys

	// create new values array with space for new value
	values := make([][]byte, len(n.values)+1)
	// copy values before index
	copy(values, n.values[:i])
	// insert new value
	values[i] = value
	// copy values after index
	copy(values[i+1:], n.values[i:])
	// set values
	n.values = values
}

func (n *Node) split() {
	if len(n.Keys) <= MAX_KEYS_PER_NODE {
		return
	}

	if n.typ == NODE_TYPE_LEAF {
		n.splitLeaf()
	} else {
		n.splitInternal()
	}

	n.bucket.node(n.parent).split()
}

func (n *Node) splitLeaf() {
	if n.typ != NODE_TYPE_LEAF {
		panic("cannot split non-leaf node")
	}
	// if parent is nil, create new parent
	if n.parent == 0 {
		n.parent = n.bucket.newRootNode().pgid
		n.bucket.node(n.parent).children = append(n.bucket.node(n.parent).children, n.pgid)
	}

	midIndex := MAX_KEYS_PER_NODE
	midKey := n.Keys[midIndex]

	// create new sibling node
	sibling := n.bucket.newNode(n.parent, n.typ)
	sibling.parent = n.parent

	n.bucket.node(n.parent).children = append(n.bucket.node(n.parent).children, sibling.pgid)
	n.bucket.node(n.parent).Keys = append(n.bucket.node(n.parent).Keys, midKey)

	// set sibling keys and values and children

	// create new keys array with space for new key
	sibling.Keys = make([][]byte, len(n.Keys[midIndex:]))
	// copy keys before index
	copy(sibling.Keys, n.Keys[midIndex:])

	// set new node keys and values
	newKeys := make([][]byte, len(n.Keys[:midIndex]))
	copy(newKeys, n.Keys[:midIndex])
	n.Keys = newKeys

	// create new values array with space for new value
	sibling.values = make([][]byte, len(n.values[midIndex:]))
	// copy values before index
	copy(sibling.values, n.values[midIndex:])

	newValues := make([][]byte, len(n.values[:midIndex]))
	copy(newValues, n.values[:midIndex])
	n.values = newValues
}

func (n *Node) splitInternal() {
	if n.typ != NODE_TYPE_INTERNAL {
		panic("cannot split non-internal node")
	}

	// if parent is nil, create new parent
	if n.parent == 0 {
		n.parent = n.bucket.newRootNode().pgid
		n.bucket.node(n.parent).children = append(n.bucket.node(n.parent).children, n.pgid)
	}

	midIndex := MAX_KEYS_PER_NODE
	midKey := n.Keys[midIndex]

	// create new sibling node
	sibling := n.bucket.newNode(n.parent, n.typ)
	sibling.parent = n.parent

	// find the index to insert midKey in the parent
	parent := n.bucket.node(n.parent)
	i := sort.Search(len(parent.Keys), func(i int) bool { return bytes.Compare(parent.Keys[i], midKey) != -1 })

	// insert midKey and sibling.pgid in the parent node
	parent.Keys = append(parent.Keys, nil) // make space for the new key
	copy(parent.Keys[i+1:], parent.Keys[i:])
	parent.Keys[i] = midKey

	parent.children = append(parent.children, 0) // make space for the new child
	copy(parent.children[i+2:], parent.children[i+1:])
	parent.children[i+1] = sibling.pgid

	// set sibling keys and children
	sibling.Keys = make([][]byte, len(n.Keys[midIndex+1:]))
	copy(sibling.Keys, n.Keys[midIndex+1:])

	sibling.children = make([]uint64, len(n.children[midIndex+1:]))
	copy(sibling.children, n.children[midIndex+1:])
	for _, child := range sibling.children {
		n.bucket.node(child).parent = sibling.pgid
	}

	// set new node keys and children
	newKeys := make([][]byte, len(n.Keys[:midIndex]))
	copy(newKeys, n.Keys[:midIndex])
	n.Keys = newKeys

	newChildren := make([]uint64, len(n.children[:midIndex+1]))
	copy(newChildren, n.children[:midIndex+1])
	n.children = newChildren
}

func newNode(b *Bucket, pgid uint64, typ uint8) *Node {
	return &Node{
		bucket:   b,
		pgid:     pgid,
		typ:      typ,
		Keys:     make([][]byte, 0),
		children: make([]uint64, 0),
		values:   make([][]byte, 0),
	}
}

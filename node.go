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

	// insert new key
	n.Keys = append(n.Keys[:i], append([][]byte{key}, n.Keys[i:]...)...)

	// insert new value
	n.values = append(n.values[:i], append([][]byte{value}, n.values[i:]...)...)
}

func (n *Node) split() {
	if len(n.Keys) <= n.bucket.db.config.maxKeysPerNode {
		return
	}

	if n.typ == NODE_TYPE_LEAF {
		n.splitLeaf()
	} else {
		n.splitInternal()
	}

	//parent := n.bucket.node(n.parent)
	//if len(parent.Keys) > MAX_KEYS_PER_NODE {
	//	parent.split()
	//}
}

func (n *Node) splitLeaf() {
	if n.typ != NODE_TYPE_LEAF {
		panic("cannot split non-leaf node")
	}

	// as we are splitting, we must check the existence of parent node
	// if parent node does not exist, create it
	var parent *Node
	if n.parent == 0 {
		n.parent = n.bucket.newRootNode().pgid
		// as the parent node is new, we must update the root node
		n.bucket.root = n.parent
		// and we must attach the current node to the parent node
		n.bucket.node(n.parent).addChild(n.pgid)
	}

	parent = n.bucket.node(n.parent)

	// now we split the current into 2 halves. and second half will be the new node
	// the first half will be the current node

	sibling := n.bucket.newLeafNode()

	// now we split the keys, values and children between the current node and the sibling node
	n.Keys, sibling.Keys = n.splitTwoKeys()
	n.values, sibling.values = n.splitTwoValues()
	n.children, sibling.children = n.splitTwoChildren()

	// we must update the parent of the sibling node
	sibling.parent = parent.pgid
	// the parent node must have the sibling node as a child
	parent.addChild(sibling.pgid)

	// as we have split the keys, sibling node children must be updated
	//to have the sibling node as a parent
	for _, child := range sibling.children {
		n.bucket.node(child).parent = sibling.pgid
	}
	// and also parent node to have the sibling node's key as a key
	parent.addKey(sibling.Keys[0])

}

func (n *Node) splitInternal() {
	if n.typ != NODE_TYPE_INTERNAL {
		panic("cannot split non-internal node")
	}

	// as we are splitting, we must check the existence of parent node
	// if parent node does not exist, create it
	var parent *Node
	if n.parent == 0 {
		n.parent = n.bucket.newRootNode().pgid
		// as the parent node is new, we must update the root node
		n.bucket.root = n.parent
		// and we must attach the current node to the parent node
		n.bucket.node(n.parent).addChild(n.pgid)
	}

	parent = n.bucket.node(n.parent)

	// now we split the current into 2 halves. and second half will be the new node
	// the first half will be the current node

	// pick the middle key and promote it to the parent node
	midKey := n.Keys[len(n.Keys)/2]
	parent.addKey(midKey)

	sibling := n.bucket.newInternalNode()

	// now we split the keys, values and children between the current node and the sibling node
	n.Keys, sibling.Keys = n.splitTwoKeys()

	// splitting internal node is a bit different
	// we take one key out of the sibling node and put it in the parent node
	if len(sibling.Keys) > 1 {
		sibling.Keys = sibling.Keys[1:]
	}

	n.children, sibling.children = n.splitTwoChildren()
	// internal node does not have values
	sibling.values = make([][]byte, 0)
	// we must update the parent of the sibling node
	sibling.parent = n.parent
	// the parent node must have the sibling node as a child
	parent.addChild(sibling.pgid)

	// as we have split the keys, sibling node children must be updated
	//to have the sibling node as a parent
	for _, child := range sibling.children {
		n.bucket.node(child).parent = sibling.pgid
	}

}

func (n *Node) addChild(pgid uint64) {
	// find index where child should be inserted based on the key
	newNode := n.bucket.node(pgid)

	i := sort.Search(len(n.children), func(i int) bool {
		currentNode := n.bucket.node(n.children[i])

		return bytes.Compare(currentNode.Keys[0], newNode.Keys[0]) != -1
	})

	newChildren := make([]uint64, len(n.children)+1)
	copy(newChildren, n.children[:i])
	newChildren[i] = pgid
	copy(newChildren[i+1:], n.children[i:])

	n.children = newChildren
}

func (n *Node) addKey(key []byte) {
	// find index where key should be inserted
	i := sort.Search(len(n.Keys), func(i int) bool { return bytes.Compare(n.Keys[i], key) != -1 })

	// insert new key
	n.Keys = append(n.Keys[:i], append([][]byte{key}, n.Keys[i:]...)...)
}

// splitTwoKeys splits the keys into two halves and return 2 new copies of keys
func (n *Node) splitTwoKeys() ([][]byte, [][]byte) {
	mid := len(n.Keys) / 2
	left := make([][]byte, mid)
	right := make([][]byte, len(n.Keys)-mid)

	copy(left, n.Keys[:mid])
	copy(right, n.Keys[mid:])

	return left, right
}

func (n *Node) splitTwoValues() ([][]byte, [][]byte) {
	mid := len(n.values) / 2
	left := make([][]byte, mid)
	right := make([][]byte, len(n.values)-mid)

	copy(left, n.values[:mid])
	copy(right, n.values[mid:])

	return left, right
}

func (n *Node) splitTwoChildren() ([]uint64, []uint64) {
	mid := len(n.children) / 2
	left := make([]uint64, mid)
	right := make([]uint64, len(n.children)-mid)

	copy(left, n.children[:mid])
	copy(right, n.children[mid:])

	return left, right
}

func (n *Node) scan(f func(key []byte, value []byte) bool) {
	if n.typ == NODE_TYPE_LEAF {
		for i := 0; i < len(n.Keys); i++ {
			if !f(n.Keys[i], n.values[i]) {
				return
			}
		}

		return
	}

	// scan all children of the internal node
	for i := 0; i < len(n.children); i++ {
		n.bucket.node(n.children[i]).scan(f)
	}
}

func (n *Node) delete(i int) {
	newKeys := make([][]byte, len(n.Keys)-1)
	copy(newKeys, n.Keys[:i])
	copy(newKeys[i:], n.Keys[i+1:])
	n.Keys = newKeys

	newValues := make([][]byte, len(n.values)-1)
	copy(newValues, n.values[:i])
	copy(newValues[i:], n.values[i+1:])
	n.values = newValues
}

func (n *Node) possibleFree(pgid uint64) {
	if n.typ != NODE_TYPE_INTERNAL {
		panic("cannot free node from non-internal node")
	}

	node := n.bucket.node(pgid)

	// if the node is not empty, we cannot free it
	if len(node.Keys) > 0 {
		return
	}

	// if the node is empty, we must remove it from the parent node
	n.removeChild(pgid)

	// @todo: add the node to the freelist
}

func (n *Node) removeChild(pgid uint64) {
	var newChildren []uint64
	for _, child := range n.children {
		if child == pgid {
			continue
		}

		newChildren = append(newChildren, child)
	}

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

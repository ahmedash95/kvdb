package kvdb

import (
	"bytes"
	"fmt"
)

type Cursor struct {
	bucket *Bucket
	stack  []*Node
}

func newCursor(b *Bucket) Cursor {
	return Cursor{bucket: b, stack: make([]*Node, 0)}
}

func (c *Cursor) seek(seek []byte) *Node {
	return c.search(c.bucket.root, seek)
}

func (c *Cursor) search(pgid uint64, seek []byte) *Node {
	node := c.bucket.node(pgid)
	c.stack = append(c.stack, node)

	// if node is leaf, return it
	if node.typ == NODE_TYPE_LEAF {
		return node
	}

	// if node is internal, search for the child node
	for i, key := range node.Keys {
		if bytes.Compare(key, seek) > 0 {
			return c.search(node.children[i], seek)
		}
	}

	if len(node.children) == 0 {
		keysString := ""
		for _, key := range node.Keys {
			keysString += fmt.Sprintf("%s, ", key)
		}

		panic(fmt.Sprintf("node %d has no children. node keys %v", node.pgid, keysString))
	}

	// if seek is greater than all keys, return the last child node
	return c.search(node.children[len(node.children)-1], seek)
}

func (c Cursor) freeStack() {
	c.stack = make([]*Node, 0)
}

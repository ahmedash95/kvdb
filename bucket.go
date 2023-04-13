package kvdb

import "fmt"

type Bucket struct {
	db    *DB
	root  uint64
	nodes map[uint64]*Node // in-memory nodes
}

func newBucket(db *DB, pgid uint64) *Bucket {
	return &Bucket{
		db:    db,
		root:  pgid,
		nodes: make(map[uint64]*Node),
	}
}

func (b *Bucket) Put(key []byte, value []byte) error {
	cursor := b.Cursor()

	// get node where key should be inserted
	node := cursor.seek(key)

	// if key already exists, update value
	// if key does not exist, the value is -1
	if i, ok := node.findKey(key); ok {
		node.values[i] = value
		return nil
	}

	// insert key and value
	node.insert(key, value)

	// if node is full, split it
	// @todo: this logic should be moved to commit transaction function
	// but for now, we will keep it here
	for i := len(cursor.stack) - 1; i >= 0; i-- {
		// split every node that is full in the stack
		if b.db.config.callOnSplit != nil {
			b.db.config.callOnSplit()
		}
		cursor.stack[i].split()
	}
	//node.split()

	return nil
}

func (b *Bucket) Update(key []byte, value []byte) error {
	cursor := b.Cursor()

	// get node where key should be
	node := cursor.seek(key)

	// if key  exists, update value
	if i, ok := node.findKey(key); ok {
		node.values[i] = value
		return nil
	}

	return fmt.Errorf("key not found")
}

func (b *Bucket) Get(key []byte) ([]byte, error) {
	cursor := b.Cursor()

	// get node where key should be
	node := cursor.seek(key)

	// if key exists, return value
	if i, ok := node.findKey(key); ok {
		return node.values[i], nil
	}

	return nil, fmt.Errorf("key not found")
}

func (b *Bucket) Delete(key []byte) error {
	cursor := b.Cursor()

	// get node where key should be
	node := cursor.seek(key)

	// if key exists, delete it
	if i, ok := node.findKey(key); ok {
		node.delete(i)
		b.node(node.parent).possibleFree(node.pgid)
		return nil
	}

	return fmt.Errorf("key not found")
}

func (b *Bucket) Cursor() Cursor {
	return newCursor(b)
}

// node returns the in-memory node for a given page id
// if node is not found in cache, it is loaded from disk
// if node is not found on disk, it is created in memory
// and will be persisted to disk when the bucket finishes writing
func (b *Bucket) node(pgid uint64) *Node {
	if node, ok := b.nodes[pgid]; ok {
		return node
	}

	// @todo: load node from disk and cache it in memory if found

	node := newNode(b, pgid, NODE_TYPE_LEAF)

	b.nodes[pgid] = node

	return node
}

func (b *Bucket) newRootNode() *Node {
	node := newNode(b, b.db.meta.getNewPageID(), NODE_TYPE_INTERNAL)

	b.nodes[node.pgid] = node
	b.root = node.pgid

	return node
}

func (b *Bucket) newInternalNode() *Node {
	node := newNode(b, b.db.meta.getNewPageID(), NODE_TYPE_INTERNAL)

	b.nodes[node.pgid] = node

	return node
}

func (b Bucket) newLeafNode() *Node {
	node := newNode(&b, b.db.meta.getNewPageID(), NODE_TYPE_LEAF)

	b.nodes[node.pgid] = node

	return node
}

func (b *Bucket) newNode(parent uint64, typ uint8) *Node {
	node := newNode(b, b.db.meta.getNewPageID(), typ)

	node.parent = parent

	b.nodes[node.pgid] = node

	return node
}

func (b *Bucket) Scan(f func(key []byte, value []byte) bool) {
	b.node(b.root).scan(f)
}

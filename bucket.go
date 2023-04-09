package kvdb

import (
	"fmt"
)

type Bucket struct {
	name     string
	rootpage uint64
	db       *DB
}

func (db *DB) Bucket(name string) (*Bucket, error) {
	// check if bucket exists
	for _, bucket := range db.meta.buckets {
		if bucket.name == name {
			return &Bucket{
				name:     bucket.name,
				rootpage: bucket.rootpage,
				db:       db,
			}, nil
		}
	}

	return newBucket(db, name)
}

func newBucket(db *DB, name string) (*Bucket, error) {
	b := &MetaBucket{
		name:     name,
		rootpage: db.meta.getNewPageID(),
	}

	err := db.writeNode(NewNode(b.rootpage))
	if err != nil {
		return nil, err
	}

	db.meta.buckets = append(db.meta.buckets, b)
	db.writeMeta()

	return &Bucket{
		name:     b.name,
		rootpage: b.rootpage,
		db:       db,
	}, nil
}

func (b *Bucket) Put(key, value []byte) error {
	node := b.db.readNode(b.rootpage)
	node.insert(key, value)

	return b.db.writeNode(node)
}

func (b *Bucket) Get(key []byte) ([]byte, error) {
	node := b.db.readNode(b.rootpage)
	val, ok := node.get(key)
	if !ok {
		return nil, fmt.Errorf("Key not found")
	}

	return val, nil
}

func (b *Bucket) Scan(call func([]byte, []byte)) error {
	node := b.db.readNode(b.rootpage)
	node.scan(call)

	return nil
}

package kvdb

import (
	"fmt"
	"io"
	"strings"
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

	// write new empty page
	err := db.writePage(Page{pgid: b.rootpage})
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
	p, err := b.db.readPage(b.rootpage)
	if err != nil {
		return err
	}

	k := p.keys

	if k > MAX_KEYS {
		return fmt.Errorf("max keys reached: %d. current key: %d", MAX_KEYS, k)
	}

	// update rows count
	k, err = p.updateRows(b.db)
	if err != nil {
		return err
	}

	// write key
	bytes := p.MakeRow(key, value)

	// write new key to page
	offset := b.db.pageOffset(b.rootpage) + p.keyPos(k)
	b.db.file.Seek(int64(offset), io.SeekStart)

	_, err = b.db.file.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}

func (b *Bucket) Get(key []byte) ([]byte, error) {
	p, err := b.db.readPage(b.rootpage)
	if err != nil {
		return nil, err
	}

	size := p.keys

	b.db.file.Seek(int64(b.db.pageOffset(b.rootpage)+PAGE_HEADER), io.SeekStart)

	for i := 0; i < int(size); i++ {
		// read key
		keybytes := make([]byte, KEY_SIZE)
		_, err := b.db.file.Read(keybytes)
		if err != nil {
			return nil, err
		}

		// read value
		valbytes := make([]byte, VALUE_SIZE)
		_, err = b.db.file.Read(valbytes)
		if err != nil {
			return nil, err
		}

		keystr := strings.TrimRight(string(keybytes), "\x00")
		if string(key) == keystr {
			return []byte(strings.TrimRight(string(valbytes), "\x00")), nil
		}

	}

	return nil, nil
}

func (b *Bucket) Scan(call func([]byte, []byte)) error {
	p, err := b.db.readPage(b.rootpage)
	if err != nil {
		return err
	}

	size := p.keys

	b.db.file.Seek(int64(b.db.pageOffset(b.rootpage)+PAGE_HEADER), io.SeekStart)

	for i := 0; i < int(size); i++ {
		// read key
		keybytes := make([]byte, KEY_SIZE)
		_, err := b.db.file.Read(keybytes)
		if err != nil {
			return err
		}

		// read value
		valbytes := make([]byte, VALUE_SIZE)
		_, err = b.db.file.Read(valbytes)
		if err != nil {
			return err
		}

		keystr := strings.TrimRight(string(keybytes), "\x00")
		valstr := strings.TrimRight(string(valbytes), "\x00")

		call([]byte(keystr), []byte(valstr))
	}

	return nil
}

package kvdb

import (
	"encoding/binary"
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

func (b *Bucket) keys() (uint64, error) {
	var bytes [8]byte
	var keys uint64

	_, err := b.db.file.Read(bytes[:])
	if err != nil {
		panic(err)
	}

	keys = binary.LittleEndian.Uint64(bytes[:])

	return keys, nil
}

func (b *Bucket) Put(key, value []byte) error {
	p, err := b.db.readPage(b.rootpage)
	if err != nil {
		return err
	}

	k, err := b.keys()
	if err != nil {
		return err
	}

	if k > MAX_KEYS {
		panic("Max keys per page reached")
	}

	// update rows count
	p.rows++
	rowBytes := make([]byte, 8)
	binary.LittleEndian.PutUint32(rowBytes, p.rows)
	_, err = b.db.file.WriteAt(rowBytes[:], int64(b.db.pageOffset(b.rootpage))+9)
	if err != nil {
		return err
	}

	// write key
	bytes := p.MakeRow(key, value)

	// write new key to page
	offset := b.db.pageOffset(b.rootpage) + uint64(PAGE_HEADER+(KEY_SIZE+VALUE_SIZE)*int(k-1))
	_, err = b.db.file.WriteAt(bytes[:], int64(offset))
	if err != nil {
		return err
	}

	return nil
}

func (b *Bucket) Get(key []byte) ([]byte, error) {
	_, err := b.db.readPage(b.rootpage)
	if err != nil {
		return nil, err
	}

	size, err := b.keys()
	if err != nil {
		return nil, err
	}

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

package kvdb

import (
	"encoding/binary"
	"io"
	"os"
	"strings"
	"sync"
)

const (
	PAGE_SIZE      = 4096 // 4KB
	META_PAGE_SIZE = 4096 // 4KB

	// node types
	NODE_TYPE_INTERNAL = 0x01
	NODE_TYPE_LEAF     = 0x02

	// key/value length
	KEY_SIZE   = 100  // 100 bytes
	VALUE_SIZE = 3000 // 3000 bytes

	// HEADER SIZE
	HEADER = 0 + META_PAGE_SIZE

	// First page after meta page
	PAGES_OFFSET = HEADER
	PAGE_HEADER  = 13 // 8 bytes for page id, 1 byte for type, 4 bytes for rows

	// Page max keys
	MAX_KEYS = 3 // 2 keys per page (for now)
)

type DB struct {
	file *os.File
	path string

	meta *Meta
}

func Open(path string) (*DB, error) {
	return newDB(path)
}

func (db *DB) Close() error {
	return db.file.Close()
}

func newDB(path string) (*DB, error) {
	// create file if not exists
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	// if file is empty, write meta
	fi, err := file.Stat()
	if err != nil {
		return nil, err
	}

	db := &DB{
		file: file,
		path: path,
	}

	if fi.Size() == 0 {
		db.newMeta()
	} else {
		db.meta, err = db.readMeta()
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

type Meta struct {
	buckets []*MetaBucket
	pgid    uint64
	mu      sync.Mutex
}

type MetaBucket struct {
	name     string
	rootpage uint64
}

func (db *DB) newMeta() {
	db.meta = &Meta{
		buckets: make([]*MetaBucket, 0),
		pgid:    0,
	}

	db.writeMeta()
}

// meta page is always the first page
func (db *DB) writeMeta() error {
	bytes := make([]byte, META_PAGE_SIZE)

	// write meta
	_, err := db.file.Seek(0, 0)
	if err != nil {
		return err
	}

	// append meta page id
	binary.LittleEndian.PutUint64(bytes[0:8], db.meta.pgid)

	// append length of meta
	size := len(db.meta.buckets)
	binary.LittleEndian.PutUint64(bytes[8:16], uint64(size))
	offset := 16
	// append meta buckets
	for _, bucket := range db.meta.buckets {
		// append name
		copy(bytes[offset:offset+100], []byte(bucket.name))
		offset += 100
		// append pageroot
		binary.LittleEndian.PutUint64(bytes[offset:offset+8], uint64(bucket.rootpage))
		offset += 8
	}

	_, err = db.file.Write(bytes)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) readMeta() (*Meta, error) {
	// read meta
	_, err := db.file.Seek(0, 0)
	if err != nil {
		return nil, err
	}

	bytes := make([]byte, META_PAGE_SIZE)
	_, err = db.file.Read(bytes)
	if err != nil {
		return nil, err
	}

	m := &Meta{}

	// read meta page id
	m.pgid = binary.LittleEndian.Uint64(bytes[0:8])

	// read length of meta
	size := binary.LittleEndian.Uint64(bytes[8:16])
	offset := 16
	// read meta buckets
	var i uint64
	for i = 0; i < size; i++ {
		b := &MetaBucket{}
		// read name
		b.name = string(bytes[offset : offset+100])
		offset += 100
		// read pageroot
		b.rootpage = binary.LittleEndian.Uint64(bytes[offset : offset+8])
		offset += 8

		// trim null bytes so the name has the correct length
		b.name = strings.TrimRight(string(b.name), "\x00")

		m.buckets = append(m.buckets, b)
	}

	return m, nil
}

func (m *Meta) getNewPageID() uint64 {
	m.mu.Lock()
	m.pgid++
	m.mu.Unlock()

	return m.pgid
}

func (db *DB) pageOffset(pgid uint64) uint64 {
	return uint64(PAGES_OFFSET + (int(pgid-1) * PAGE_SIZE))
}

func (db *DB) readPage(pgid uint64) (*Page, error) {
	_, err := db.file.Seek(int64(db.pageOffset(pgid)), io.SeekStart)
	if err != nil {
		return nil, err
	}

	p := &Page{}
	p.read()

	return p, nil
}

func (db *DB) writePage(page Page) error {
	// write page
	_, err := db.file.Seek(int64(db.pageOffset(page.pgid)), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = db.file.Write(page.write())
	if err != nil {
		return err
	}

	return nil
}
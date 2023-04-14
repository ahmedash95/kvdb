package kvdb

import (
	"encoding/binary"
	"strings"
	"sync"
)

// @todo persist freelist to disk
type Meta struct {
	buckets  []*MetaRecord
	pgid     uint64
	freeList []uint64 // list of free pages
	mu       sync.Mutex
}

type MetaRecord struct {
	name     string
	rootpage uint64
}

func (m *Meta) addFreePage(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.freeList = append(m.freeList, id)
}

func (m *Meta) getNewPageID() uint64 {
	m.mu.Lock()

	if len(m.freeList) > 0 {
		id := m.freeList[0]
		m.freeList = m.freeList[1:]
		m.mu.Unlock()
		return id
	}

	m.pgid++
	m.mu.Unlock()

	// @todo: persist meta page to disk to reflect last page id

	return m.pgid
}

// newBucket should be called only from DB.Bucket()
func (m *Meta) newBucket(db *DB, s string) *Bucket {
	// create new bucket
	record := &MetaRecord{
		name:     s,
		rootpage: m.getNewPageID(),
	}

	m.mu.Lock()
	m.buckets = append(m.buckets, record)
	m.mu.Unlock()

	return newBucket(db, record.rootpage)
}

func (db *DB) newMeta() error {
	db.meta = &Meta{
		buckets: make([]*MetaRecord, 0),
		pgid:    0,
	}

	return db.writeMeta()
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
		b := &MetaRecord{}
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

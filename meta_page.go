package kvdb

import (
	"encoding/binary"
	"strings"
	"sync"
)

type Meta struct {
	buckets []*MetaBucket
	pgid    uint64
	mu      sync.Mutex
}

type MetaBucket struct {
	name     string
	rootpage uint64
}

func (m *Meta) getNewPageID() uint64 {
	m.mu.Lock()
	m.pgid++
	m.mu.Unlock()

	return m.pgid
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

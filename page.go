package kvdb

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
)

type Record struct {
	Key   []byte
	Value []byte
}

type Page struct {
	pgid    uint64
	_type   uint8 // leaf or internal
	nkeys   uint32
	keys    [][]byte
	Records []Record
}

func NewPage(pgid uint64) Page {
	return Page{
		pgid:    pgid,
		_type:   NODE_TYPE_LEAF,
		nkeys:   0,
		keys:    make([][]byte, 0),
		Records: make([]Record, 0),
	}
}

func (m *Meta) getNewPageID() uint64 {
	m.mu.Lock()
	m.pgid++
	m.mu.Unlock()

	return m.pgid
}

func (db *DB) pageOffset(pgid uint64) int64 {
	return int64(PAGES_OFFSET + (int(pgid-1) * PAGE_SIZE))
}

func (db *DB) seekPage(pgid uint64) error {
	_, err := db.file.Seek(int64(db.pageOffset(pgid)), io.SeekStart)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) readPage(pgid uint64) (*Page, error) {
	err := db.seekPage(pgid)
	if err != nil {
		return nil, err
	}

	var p Page

	headerBytes := make([]byte, PAGE_HEADER)

	_, err = db.file.Read(headerBytes)
	if err != nil {
		return nil, err
	}

	// read page id
	p.pgid = binary.LittleEndian.Uint64(headerBytes[0:8])

	// read page type
	p._type = headerBytes[8]

	// read number of keys
	p.nkeys = binary.LittleEndian.Uint32(headerBytes[9:13])

	// read keys
	p.keys = make([][]byte, p.nkeys)
	for i := 0; i < int(p.nkeys); i++ {
		keyBytes := make([]byte, KEY_SIZE)
		_, err := db.file.Read(keyBytes)
		if err != nil {
			return nil, err
		}

		p.keys[i] = keyBytes
	}

	// read records
	p.Records = make([]Record, p.nkeys)
	for i := 0; i < int(p.nkeys); i++ {
		// read key
		keyBytes := make([]byte, KEY_SIZE)
		_, err := db.file.Read(keyBytes)
		if err != nil {
			return nil, err
		}

		// read value
		valueBytes := make([]byte, VALUE_SIZE)
		_, err = db.file.Read(valueBytes)
		if err != nil {
			return nil, err
		}

		p.Records[i] = Record{
			Key:   keyBytes,
			Value: valueBytes,
		}
	}

	return &p, nil
}

func (db *DB) writePage(page Page) error {
	// write page
	err := db.seekPage(page.pgid)
	if err != nil {
		return err
	}

	_, err = db.file.Write(page.write())
	if err != nil {
		return err
	}

	return nil
}

func (p *Page) keyPos(k uint32) int64 {
	return int64(PAGE_HEADER + uint64((KEY_SIZE+VALUE_SIZE)*int(k-1)))
}

func (p *Page) write() []byte {
	bytes := make([]byte, PAGE_SIZE)

	// write page id
	binary.LittleEndian.PutUint64(bytes[0:8], p.pgid)

	// write page type
	bytes[8] = p._type

	// write number of keys
	binary.LittleEndian.PutUint32(bytes[9:13], p.nkeys)

	keysbytes := make([]byte, KEY_SIZE*p.nkeys)
	// write keys list
	for i := 0; i < int(p.nkeys); i++ {
		keybytes := make([]byte, KEY_SIZE)
		copy(keybytes, p.keys[i])
		copy(keysbytes[i*KEY_SIZE:], keybytes)
	}

	copy(bytes[PAGE_HEADER:], keysbytes)

	// write records
	offset := PAGE_HEADER + len(keysbytes)
	for i := 0; i < int(p.nkeys); i++ {
		recordBytes := p.MakeRow(p.keys[i], p.Records[i].Value)
		copy(bytes[offset:], recordBytes)
		offset += len(recordBytes)
	}

	return bytes
}
func (p *Page) findKey(key []byte) (uint32, bool) {
	for i, k := range p.keys {
		if bytes.Equal(k, key) {
			return uint32(i), true
		}
	}

	return 0, false
}

func (p *Page) lookupKeyPos(key []byte) uint32 {
	for i, k := range p.keys {
		fmt.Println("Comparing", string(k), "with", string(key))
		if bytes.Compare(k, key) >= 0 {
			return uint32(i)
		}
	}

	return uint32(len(p.keys))
}

func (p *Page) appendKey(db *DB, key []byte) {
	p.nkeys += 1
	p.keys = append(p.keys, key)
}

func (p *Page) MakeRow(key, value []byte) []byte {
	bytes := make([]byte, KEY_SIZE+VALUE_SIZE)

	// write key
	copy(bytes[0:KEY_SIZE], key)

	// write value
	copy(bytes[KEY_SIZE:], value)

	return bytes
}

func (p *Page) writeKeyAt(db *DB, key, val []byte, pos uint32) error {
	return nil
}

func (p *Page) insertKey(db *DB, key, val []byte, pos uint32) error {
	p.Records = append(p.Records, Record{
		Key:   key,
		Value: val,
	})

	p.appendKey(db, key)

	return db.writePage(*p)
}

func (p *Page) recordsOffset() int64 {
	return int64(PAGE_HEADER + (KEY_SIZE * p.nkeys))
}

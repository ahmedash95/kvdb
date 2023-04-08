package kvdb

import (
	"encoding/binary"
	"io"
)

type Page struct {
	pgid  uint64
	_type uint8 // leaf or internal
	rows  uint32
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

	bytes := make([]byte, PAGE_HEADER)

	_, err = db.file.Read(bytes)
	if err != nil {
		return nil, err
	}

	// read page id
	p.pgid = binary.LittleEndian.Uint64(bytes[0:8])

	// read page type
	p._type = bytes[8]

	// read number of rows
	p.rows = binary.LittleEndian.Uint32(bytes[9:13])

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
	bytes := make([]byte, PAGE_HEADER)

	// write page id
	binary.LittleEndian.PutUint64(bytes[0:8], p.pgid)

	// write page type
	bytes[8] = p._type

	// write number of rows
	binary.LittleEndian.PutUint32(bytes[9:13], p.rows)

	return bytes
}

func (p *Page) updateRows(db *DB) (uint32, error) {
	err := db.seekPage(p.pgid)
	if err != nil {
		return 0, err
	}

	p.rows += 1

	err = db.writePage(*p)
	if err != nil {
		return 0, err
	}

	return p.rows, nil
}

func (p *Page) MakeRow(key, value []byte) []byte {
	bytes := make([]byte, KEY_SIZE+VALUE_SIZE)

	// write key
	copy(bytes[0:KEY_SIZE], key)

	// write value
	copy(bytes[KEY_SIZE:], value)

	return bytes
}

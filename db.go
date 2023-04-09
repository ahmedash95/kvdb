package kvdb

import (
	"os"
)

const (
	PAGE_SIZE      = 4096 // 4KB
	META_PAGE_SIZE = 4096 // 4KB

	// node types
	NODE_TYPE_INTERNAL = 0x01
	NODE_TYPE_LEAF     = 0x02

	// key/value length
	KEY_SIZE   = 100 // 100 bytes
	VALUE_SIZE = 100 // 3000 bytes

	// DB_HEADER SIZE
	DB_HEADER = 0 + META_PAGE_SIZE

	// First page after meta page
	PAGES_OFFSET = DB_HEADER

	// Page header
	PAGE_ID_BYTES   = 8
	PAGE_TYPE_BYTES = 1
	PAGE_KEYS_BYTES = 4

	PAGE_HEADER = PAGE_ID_BYTES + PAGE_TYPE_BYTES + PAGE_KEYS_BYTES

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

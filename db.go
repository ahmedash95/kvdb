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
	MAX_KEYS_PER_NODE  = 5

	// key/value length
	KEY_SIZE   = 100 // 100 bytes
	VALUE_SIZE = 100 // 3000 bytes

	// DB_HEADER SIZE
	DB_HEADER = 0 + META_PAGE_SIZE

	FreeListSize = 32
)

type DB struct {
	file   *os.File
	path   string
	config Config

	meta *Meta
}

type Config struct {
	maxKeysPerNode int
	callOnSplit    func()
}

func Open(path string, config *Config) (*DB, error) {
	if config == nil {
		config = &Config{maxKeysPerNode: 3}
	}
	return newDB(path, *config)
}

func (db *DB) Close() error {
	return db.file.Close()
}

func newDB(path string, config Config) (*DB, error) {
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
		file:   file,
		path:   path,
		config: config,
	}

	if fi.Size() == 0 {
		err := db.newMeta()
		if err != nil {
			return nil, err
		}
	} else {
		db.meta, err = db.readMeta()
		if err != nil {
			return nil, err
		}
	}

	return db, nil
}

func (db *DB) Bucket(s string) *Bucket {
	// find bucket in meta page
	for _, record := range db.meta.buckets {
		if record.name == s {
			return newBucket(db, record.rootpage)
		}
	}

	// if bucket not found, create new bucket
	return db.meta.newBucket(db, s)
}

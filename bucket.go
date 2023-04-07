package kvdb

type Bucket struct {
	name     string
	rootpage uint64
	db       *DB
}

func (db *DB) Bucket(name string) (*Bucket, error) {
	return newBucket(db, name)
}

func newBucket(db *DB, name string) (*Bucket, error) {
	// check if bucket exists
	for _, bucket := range db.meta.buckets {
		if bucket.name == name {
			return &Bucket{
				name:     bucket.name,
				rootpage: bucket.pageroot,
				db:       db,
			}, nil
		}
	}

	// create new bucket
	b := &MetaBucket{
		name:     name,
		pageroot: db.meta.getNewPageID(),
	}

	db.meta.buckets = append(db.meta.buckets, b)
	db.writeMeta()

	return &Bucket{
		name:     b.name,
		rootpage: b.pageroot,
		db:       db,
	}, nil
}

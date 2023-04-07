package kvdb

import (
	"os"
	"testing"
)

func setupTest() func() {
	os.Remove("test.db")

	return func() {
		os.Remove("test.db")
	}
}

func TestOpen(t *testing.T) {
	defer setupTest()
	db, err := Open("test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
}

// creating buckets should update meta page
// and create a new page for the bucket
func TestCreateBucket(t *testing.T) {
	defer setupTest()
	db, err := Open("test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Bucket("users")
	if err != nil {
		t.Fatal(err)
	}

	if len(db.meta.buckets) != 1 {
		t.Fatalf("bucket not created, found %d buckets\n", len(db.meta.buckets))
	}

	if db.meta.buckets[0].name != "users" {
		t.Fatal("bucket name not correct")
	}

	if db.meta.buckets[0].pageroot != 1 {
		t.Fatalf("page id not correct: %d\n", db.meta.buckets[0].pageroot)
	}
}

func TestExsistingBucket(t *testing.T) {
	defer setupTest()
	db, err := Open("test.db")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Bucket("users")
	if err != nil {
		t.Fatal(err)
	}

	db.Close()

	db, err = Open("test.db")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	_, err = db.Bucket("users")
	if err != nil {
		t.Fatal(err)
	}

	if len(db.meta.buckets) != 1 {
		t.Fatalf("bucket not created, found %d buckets\n", len(db.meta.buckets))
	}

	if db.meta.buckets[0].name != "users" {
		t.Fatal("bucket name not correct")
	}

	if db.meta.buckets[0].pageroot != 1 {
		t.Fatalf("page id not correct: %d\n", db.meta.buckets[0].pageroot)
	}
}

func TestCreateMultipleBuckets(t *testing.T) {
	defer setupTest()
	db, err := Open("test.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	_, err = db.Bucket("users")
	if err != nil {
		t.Fatal(err)
	}

	_, err = db.Bucket("posts")
	if err != nil {
		t.Fatal(err)
	}

	if len(db.meta.buckets) != 2 {
		t.Fatalf("bucket not created, found %d buckets\n", len(db.meta.buckets))
	}

	if db.meta.buckets[0].name != "users" {
		t.Fatal("bucket name not correct")
	}

	if db.meta.buckets[0].pageroot != 1 {
		t.Fatalf("page id not correct: %d\n", db.meta.buckets[0].pageroot)
	}

	if db.meta.buckets[1].name != "posts" {
		t.Fatal("bucket name not correct")
	}

	if db.meta.buckets[1].pageroot != 2 {
		t.Fatalf("page id not correct: %d\n", db.meta.buckets[1].pageroot)
	}
}

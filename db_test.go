package kvdb

import (
	"fmt"
	"testing"
)

func TestDB(t *testing.T) {
	db, err := Open("test.db")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	bucket := db.Bucket("user_emails")

	err = bucket.Put([]byte("user1"), []byte("user1@email.com"))
	if err != nil {
		t.Fatal(err)
	}

	printBucket(bucket)
}

func TestDBInsertMultiple(t *testing.T) {
	db, err := Open("test.db")
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	bucket := db.Bucket("user_emails")

	names := []string{"Ahmed", "Basem", "Camal", "Dawood", "Emad", "Fady", "Gamal", "Hassan", "Ibrahim", "Jack", "Khaled"}

	for _, name := range names {
		err = bucket.Put([]byte(name), []byte(fmt.Sprintf("%s@email.com", name)))
		if err != nil {
			t.Fatal(err)
		}
	}

	printBucket(bucket)
}

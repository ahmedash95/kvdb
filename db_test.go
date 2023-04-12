package kvdb

import (
	"fmt"
	"math/rand"
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

	names := []string{"Ahmed", "Basem", "Hassan", "Ibrahim", "Camal", "Emad", "Fady", "Dawood", "Jack", "Khaled"}

	// shuffle names
	for i := range names {
		j := rand.Intn(i + 1)
		names[i], names[j] = names[j], names[i]
	}

	for _, name := range names {
		err = bucket.Put([]byte(name), []byte(fmt.Sprintf("%s@email.com", name)))
		if err != nil {
			t.Fatal(err)
		}
	}

	printBucket(bucket)
}

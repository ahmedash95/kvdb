package kvdb

import (
	"fmt"
	"testing"
)

func injectAndPrintMermaid(db *DB, bucket *Bucket) func() {
	var mermaidDevs []string
	db.config.callOnSplit = func() {
		newMermaid := MermaidHtml(bucket)
		// check if the new mermaid is not the same as the previous one
		if len(mermaidDevs) > 0 && mermaidDevs[len(mermaidDevs)-1] == newMermaid {
			return
		}

		mermaidDevs = append(mermaidDevs, newMermaid)
	}

	return func() {
		mermaidDevs = append(mermaidDevs, MermaidHtml(bucket))
		mermaidToHtml(mermaidDevs)
	}
}

func TestDB(t *testing.T) {
	db, err := Open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	bucket := db.Bucket("user_emails")

	err = bucket.Put([]byte("user1"), []byte("user1@email.com"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestDBInsertMultiple(t *testing.T) {
	db, err := Open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()
	bucket := db.Bucket("user_emails")

	// names are sorted in a way to catch sorting issues if splitting is picking different keys
	names := []string{"Ibrahim", "Gamal", "Hassan", "Camal", "Basem", "Dawood", "Emad", "Ahmed", "Fady"}

	var mermaidDevs []string
	db.config.callOnSplit = func() {
		newMermaid := MermaidHtml(bucket)
		// check if the new mermaid is not the same as the previous one
		if len(mermaidDevs) > 0 && mermaidDevs[len(mermaidDevs)-1] == newMermaid {
			return
		}

		mermaidDevs = append(mermaidDevs, newMermaid)
	}

	for _, name := range names {
		err = bucket.Put([]byte(name), []byte(fmt.Sprintf("%s@email.com", name)))
		if err != nil {
			t.Fatal(err)
		}
	}

	mermaidDevs = append(mermaidDevs, MermaidHtml(bucket))
	mermaidToHtml(mermaidDevs)
}

func TestDBScanRecords(t *testing.T) {
	db, err := Open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	bucket := db.Bucket("user_emails")

	err = bucket.Put([]byte("Zanzibar"), []byte("zanzibar@gmail.com"))
	if err != nil {
		t.Fatal(err)
	}
	err = bucket.Put([]byte("Algeria"), []byte("algeria@gmail.com"))
	if err != nil {
		t.Fatal(err)
	}
	err = bucket.Put([]byte("Egypt"), []byte("egypt@gmail.com"))
	if err != nil {
		t.Fatal(err)
	}

	expectedKeys := []string{"Algeria", "Egypt", "Zanzibar"}
	actualKeys := []string{}
	bucket.Scan(func(key, value []byte) bool {
		actualKeys = append(actualKeys, string(key))
		return true
	})

	if len(expectedKeys) != len(actualKeys) {
		t.Fatalf("expected %d keys but got %d", len(expectedKeys), len(actualKeys))
	}

	for i, expectedKey := range expectedKeys {
		if expectedKey != actualKeys[i] {
			t.Fatalf("expected key %s but got %s", expectedKey, actualKeys[i])
		}
	}
}

func TestDBGet(t *testing.T) {
	db, err := Open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	bucket := db.Bucket("user_emails")

	countryEmails := map[string]string{
		"Zanzibar": "zanzibar@gmail.com",
		"Algeria":  "algeria@gmail.com",
		"Egypt":    "egypt@gmail.com",
	}

	for country, email := range countryEmails {
		err = bucket.Put([]byte(country), []byte(email))
		if err != nil {
			t.Fatal(err)
		}
	}

	for country, email := range countryEmails {
		value, err := bucket.Get([]byte(country))
		if err != nil {
			t.Fatal(err)
		}

		if string(value) != email {
			t.Fatalf("expected email %s but got %s", email, value)
		}
	}

	// test for non existing key
	_, err = bucket.Get([]byte("non-existing-key"))
	if err == nil {
		t.Fatal("expected error but got nil")
	}
}

func TestDBUpdate(t *testing.T) {
	db, err := Open("test.db", nil)
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	bucket := db.Bucket("user_emails")

	err = bucket.Put([]byte("ahmed"), []byte("ahmed@gmail.com"))
	if err != nil {
		t.Fatal(err)
	}

	err = bucket.Put([]byte("ahmed"), []byte("new@gmail.com"))
	if err != nil {
		t.Fatal(err)
	}

	value, err := bucket.Get([]byte("ahmed"))
	if err != nil {
		t.Fatal(err)
	}

	if string(value) != "new@gmail.com" {
		t.Fatalf("expected email %s but got %s", "new@gmail.com", string(value))
	}
}

func TestDBDelete(t *testing.T) {
	db, err := Open("test.db", &Config{maxKeysPerNode: 2})
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	bucket := db.Bucket("user_emails")

	defer injectAndPrintMermaid(db, bucket)()

	countryEmails := map[string]string{
		"Zanzibar": "zanzibar@gmail.com",
		"Algeria":  "algeria@gmail.com",
		"Egypt":    "egypt@gmail.com",
	}

	for country, email := range countryEmails {
		err = bucket.Put([]byte(country), []byte(email))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = bucket.Delete([]byte("Algeria"))
	if err != nil {
		t.Fatal(err)
	}
}

func TestFreeList(t *testing.T) {
	db, err := Open("test.db", &Config{maxKeysPerNode: 2})
	if err != nil {
		t.Fatal(err)
	}

	defer db.Close()

	bucket := db.Bucket("user_emails")

	defer injectAndPrintMermaid(db, bucket)()

	countryZipCodes := map[string]string{
		"Zanzibar": "12345",
		"Algeria":  "12346",
		"Egypt":    "12347",
		"Germany":  "12348",
		"France":   "12349",
		"Spain":    "12350",
		"Poland":   "12351",
	}

	for country, zipCode := range countryZipCodes {
		err = bucket.Put([]byte(country), []byte(zipCode))
		if err != nil {
			t.Fatal(err)
		}
	}

	err = bucket.Delete([]byte("Algeria"))
	if err != nil {
		t.Fatal(err)
	}
	err = bucket.Delete([]byte("Spain"))
	if err != nil {
		t.Fatal(err)
	}
	err = bucket.Delete([]byte("Zanzibar"))
	if err != nil {
		t.Fatal(err)
	}
	err = bucket.Delete([]byte("Poland"))
	if err != nil {
		t.Fatal(err)
	}

	err = bucket.Put([]byte("Germany"), []byte("duetchland@gmail.com"))
	if err != nil {
		t.Fatal(err)
	}
}

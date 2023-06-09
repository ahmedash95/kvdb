# KVDB

Key value database. again for learning purposes.


## Roadmap

### Basic features
- [x] DB file create
- [x] DB file open
- [x] DB file close
- [x] Create buckets
- [x] Create keys
- [x] Read/Get keys
- [x] Scan keys

### Memory B+tree
- [x] B+tree
- [x] Internal pages
- [x] Leaf pages
- [x] Update keys
- [x] Delete keys
- [ ] Free list pages

### Persistence
- [ ] Write pages to disk
- [ ] Read pages from disk
- [ ] Write Free list pages in meta to disk
- [ ] Read Free list pages from disk


## Usage

```go
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


// Update record
err = bucket.Put([]byte("Egypt"), []byte("cairo@gmail.com")
if err != nil {
    t.Fatal(err)
}

// Fetch record
email, err := bucket.Get([]byte("Egypt"))
if err != nil {
    t.Fatal(err)
}

fmt.Println(string(email))
```
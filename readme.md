# KVDB

Key value database. again for learning purposes.


## Roadmap

- [x] DB file create
- [x] DB file open
- [x] DB file close
- [x] Create buckets
- [x] Create keys
- [x] Read/Get keys
- [x] Scan keys
- [x] B+tree
- [x] Internal pages
- [x] Leaf pages
- [ ] Update keys
- [ ] Delete keys
- [ ] Free list pages


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

// Fetch record
email, err := bucket.Get([]byte("Egypt"))
if err != nil {
    t.Fatal(err)
}

fmt.Println(string(email))
```
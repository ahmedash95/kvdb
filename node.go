package kvdb

import (
	"bytes"
	"encoding/binary"
	"io"
	"strings"
)

// Node represents a page in the B+ tree
// the page is stored entirely in the data field
// the user of the node is responsible for persisting
// the data to disk at any time
type Node struct {
	data []byte
}

func NewNode(pgid uint64, typ int) Node {
	n := Node{
		data: make([]byte, PAGE_SIZE),
	}

	n.setPgid(pgid)
	n.setType(byte(typ))

	return n
}

func NodeFromBytes(data []byte) Node {
	return Node{
		data: data,
	}
}

func (n *Node) setPgid(pgid uint64) {
	binary.LittleEndian.PutUint64(n.data[0:8], pgid)
}

func (n *Node) pgid() uint64 {
	return binary.LittleEndian.Uint64(n.data[0:8])
}

// setType sets the type of the node either leaf or internal
func (n *Node) setType(_type byte) {
	n.data[8] = _type
}

func (n *Node) _type() byte {
	return n.data[8]
}

func (n *Node) setKeys(keys [][]byte) {
	nKeys := len(keys)
	binary.LittleEndian.PutUint32(n.data[9:13], uint32(nKeys))

	offset := 13
	for _, key := range keys {
		copy(n.data[offset:offset+KEY_SIZE], key)
		offset += KEY_SIZE
	}
}

func (n *Node) nkeys() uint32 {
	return binary.LittleEndian.Uint32(n.data[9:13])
}

func (n *Node) keys() [][]byte {
	nKeys := n.nkeys()

	keys := make([][]byte, nKeys)
	offset := 13
	for i := 0; i < int(nKeys); i++ {
		keys[i] = make([]byte, KEY_SIZE)
		copy(keys[i], n.data[offset:offset+KEY_SIZE])
		offset += KEY_SIZE
	}

	return keys
}

func (n *Node) setValues(values [][]byte) {
	offset := 13 + (len(n.keys()) * KEY_SIZE)
	for _, value := range values {
		copy(n.data[offset:offset+VALUE_SIZE], value)
		offset += VALUE_SIZE
	}
}

func (n *Node) values() [][]byte {
	nKeys := binary.LittleEndian.Uint32(n.data[9:13])

	values := make([][]byte, nKeys)
	offset := 13 + (int(nKeys) * KEY_SIZE)
	for i := 0; i < int(nKeys); i++ {
		values[i] = make([]byte, VALUE_SIZE)
		copy(values[i], n.data[offset:offset+VALUE_SIZE])
		offset += VALUE_SIZE
	}

	return values
}

func (n *Node) insert(key, value []byte) {
	keys := n.keys()
	values := n.values()

	// find the index to insert the key
	i := 0
	for i < len(keys) && bytes.Compare(keys[i], key) < 0 {
		i++
	}

	// insert the key and value
	keys = append(keys, nil)
	copy(keys[i+1:], keys[i:])
	keys[i] = key

	values = append(values, nil)
	copy(values[i+1:], values[i:])
	values[i] = value

	n.setKeys(keys)
	n.setValues(values)
}

func (n *Node) get(key []byte) ([]byte, bool) {
	keyByte := make([]byte, KEY_SIZE)
	copy(keyByte, key)

	keys := n.keys()
	for i, k := range keys {
		if bytes.Equal(k, keyByte) {
			val := n.values()[i]
			return []byte(strings.Trim(string(val), "\x00")), true
		}
	}

	return nil, false
}

func (n *Node) scan(call func([]byte, []byte)) {
	keys := n.keys()
	values := n.values()

	for i, key := range keys {
		key = []byte(strings.Trim(string(key), "\x00"))
		value := []byte(strings.Trim(string(values[i]), "\x00"))
		call(key, value)
	}
}

func (db *DB) writeNode(n Node) error {
	// write node
	_, err := db.file.Seek(DB_HEADER+int64((n.pgid()-1)*PAGE_SIZE), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = db.file.Write(n.data)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) readNode(pgid uint64) Node {
	// read node
	_, err := db.file.Seek(DB_HEADER+int64((pgid-1)*PAGE_SIZE), io.SeekStart)
	if err != nil {
		panic(err)
	}

	bytes := make([]byte, PAGE_SIZE)
	_, err = db.file.Read(bytes)
	if err != nil {
		panic(err)
	}

	return NodeFromBytes(bytes)
}

package kvdb

import (
	"bytes"
	"testing"
)

func TestNewNode(t *testing.T) {
	n := NewNode(1, NODE_TYPE_LEAF)

	if n.pgid() != 1 {
		t.Fatal("page id not correct")
	}
}

func TestNodeInsert(t *testing.T) {
	n := NewNode(1, NODE_TYPE_LEAF)

	n.insert([]byte("key"), []byte("value"))

	if n.nkeys() != 1 {
		t.Fatal("key not inserted")
	}

	val, ok := n.get([]byte("key"))

	if !ok {
		t.Fatal("value not inserted")
	}

	if !bytes.Equal(val, []byte("value")) {
		t.Fatal("value not correct")
	}
}

func TestNodeInsertMultiple(t *testing.T) {
	n := NewNode(1, NODE_TYPE_LEAF)

	n.insert([]byte("name"), []byte("ahmed"))
	n.insert([]byte("company"), []byte("shopify"))

	if n.nkeys() != 2 {
		t.Fatal("key not inserted")
	}

	val, ok := n.get([]byte("name"))
	if !ok {
		t.Fatal("value not inserted")
	}
	if !bytes.Equal(val, []byte("ahmed")) {
		t.Fatal("value not correct")
	}

	val, ok = n.get([]byte("company"))
	if !ok {
		t.Fatal("value not inserted")
	}

	if !bytes.Equal(val, []byte("shopify")) {
		t.Fatal("value not correct")
	}
}

func TestNodeScan(t *testing.T) {
	n := NewNode(1, NODE_TYPE_LEAF)

	n.insert([]byte("name"), []byte("ahmed"))
	n.insert([]byte("company"), []byte("shopify"))

	actualKeys := [][]byte{}
	n.scan(func(k, v []byte) {
		actualKeys = append(actualKeys, k)
	})

	expectedKeys := [][]byte{
		[]byte("company"),
		[]byte("name"),
	}

	for i, k := range expectedKeys {
		if !bytes.Equal(actualKeys[i], k) {
			t.Fatalf("key not correct. expected: %s, got: %s", k, actualKeys[i])
		}
	}
}

package cassandra

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/wrble/flock/index/store"
)

type Reader struct {
	store *Store
}

func (r *Reader) Get(table string, key []byte) ([]byte, error) {
	var value []byte
	if r.store.debug {
		fmt.Println("GET: "+table, key)
	}
	err := r.store.Session.Query(`SELECT value FROM `+r.store.tableName+` WHERE type = ? AND key = ?`, table, key).Scan(&value)
	if err != nil {
		if err.Error() == "not found" {
			return nil, nil
		}
		return nil, err
	}
	if r.store.debug {
		fmt.Println("GET RESPONSE: " + string(value))
	}
	return value, err
}

func (r *Reader) GetCounter(table string, key []byte) (int64, error) {
	var value int64
	if r.store.debug {
		fmt.Println("GET COUNTER: "+table, key)
	}
	err := r.store.Session.Query(`SELECT value FROM d WHERE type = ? AND key = ?`, table, key).Scan(&value)
	if err != nil {
		if err.Error() == "not found" {
			return -1, nil
		}
		return -1, err
	}
	if r.store.debug {
		fmt.Println("GET COUNTER RESPONSE: " + string(value))
	}
	return value, err
}

func (r *Reader) DocCount() (uint64, error) {
	c := uint64(0)
	err := r.store.Session.Query(`SELECT count(*) FROM `+r.store.tableName+` WHERE type = ?`, "b").Scan(&c)
	return c, err
}

func (r *Reader) MultiGet(table string, keys [][]byte) ([][]byte, error) {
	if r.store.debug {
		fmt.Println("multiget", len(keys))
	}
	return store.MultiGet(r, table, keys)
}

func (r *Reader) PrefixIterator(table string, prefix []byte) store.KVIterator {
	var iter *gocql.Iter
	if r.store.debug {
		fmt.Println("PrefixIterator", r.store.tableName, table, string(prefix))
	}
	iter = r.store.Session.Query(`SELECT value, key FROM `+r.store.tableName+` WHERE type = ? AND key >= ?`, table, prefix).Iter()
	rv := &Iterator{
		store:    r.store,
		iterator: iter,
		startKey: prefix,
	}
	rv.Next()
	return rv
}

func (r *Reader) TypedPrefixIterator(table string, prefix []byte) store.TypedKVIterator {
	var iter *gocql.Iter
	if r.store.debug {
		fmt.Println("PrefixIterator", r.store.tableName, table, string(prefix))
	}
	iter = r.store.Session.Query(`SELECT value, key FROM `+r.store.tableName+` WHERE type = ? AND key >= ?`, table, prefix).Iter()
	rv := &TypedKVIterator{
		store:    r.store,
		iterator: iter,
		startKey: prefix,
	}
	rv.Next()
	return rv
}

func (r *Reader) RangeIterator(table string, start, end []byte) store.KVIterator {
	if r.store.debug {
		fmt.Println("RangeIterator", table, string(start), string(end))
	}
	var iter *gocql.Iter
	tableName := r.store.tableName
	if table == "d" {
		tableName = table
	}
	if len(start) == 0 && len(end) == 0 {
		iter = r.store.Session.Query(`SELECT value, key FROM `+tableName+` WHERE type = ?`, table).Iter()
	} else if end != nil && len(end) > 0 {
		iter = r.store.Session.Query(`SELECT value, key FROM `+tableName+` WHERE type = ? AND key >= ? AND key < ?`, table, start, end).Iter()
	} else {
		iter = r.store.Session.Query(`SELECT value, key FROM `+tableName+` WHERE type = ? AND key >= ?`, table, start).Iter()
	}
	rv := Iterator{
		store:    r.store,
		iterator: iter,
	}
	rv.Next()
	return &rv
}

func (r *Reader) Close() error {
	return nil
}

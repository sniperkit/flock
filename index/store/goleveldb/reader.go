//  Copyright (c) 2014 Couchbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// 		http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package goleveldb

import (
	"fmt"

	"encoding/binary"

	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"github.com/wrble/flock/index/store"
)

type Reader struct {
	store    *Store
	snapshot *leveldb.Snapshot
}

func (r *Reader) Get(table string, key []byte) ([]byte, error) {
	if r.store.debug {
		fmt.Println("GET", table, string(key))
	}
	b, err := r.snapshot.Get(store.Combine(table, key), r.store.defaultReadOptions)
	if err == leveldb.ErrNotFound {
		return nil, nil
	}
	return b, err
}

func (r *Reader) GetCounter(table string, key []byte) (int64, error) {
	if r.store.debug {
		fmt.Println("GET", table, string(key))
	}
	b, err := r.snapshot.Get(store.Combine(table, key), r.store.defaultReadOptions)
	if err == leveldb.ErrNotFound {
		return 0, nil
	}
	return int64(binary.LittleEndian.Uint64(b)), err
}

func (r *Reader) MultiGet(table string, keys [][]byte) ([][]byte, error) {
	return store.MultiGet(r, table, keys)
}

func (r *Reader) DocCount() (count uint64, err error) {
	it := r.PrefixIterator("b", []byte{})
	defer func() {
		if cerr := it.Close(); err == nil && cerr != nil {
			err = cerr
		}
	}()
	_, _, valid := it.Current()
	for valid {
		count++
		it.Next()
		_, _, valid = it.Current()
	}
	return
}

func (r *Reader) PrefixIterator(table string, prefix []byte) store.KVIterator {
	if r.store.debug {
		fmt.Println("PrefixIterator", table, string(prefix))
	}

	byteRange := util.BytesPrefix(store.Combine(table, prefix))
	iter := r.snapshot.NewIterator(byteRange, r.store.defaultReadOptions)
	iter.First()
	rv := Iterator{
		store:    r.store,
		iterator: iter,
		table:    table,
	}
	return &rv
}

func (r *Reader) RangeIterator(table string, start, end []byte) store.KVIterator {
	if end == nil {
		end = []byte{0xFF}
	}
	if r.store.debug {
		fmt.Println("RangeIterator", table, string(start), string(end))
	}
	byteRange := &util.Range{
		Start: store.Combine(table, start),
		Limit: store.Combine(table, end),
	}
	iter := r.snapshot.NewIterator(byteRange, r.store.defaultReadOptions)
	iter.First()
	rv := Iterator{
		table:    table,
		store:    r.store,
		iterator: iter,
	}
	return &rv
}

func (r *Reader) Close() error {
	r.snapshot.Release()
	return nil
}

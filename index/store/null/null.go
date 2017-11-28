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

package null

import (
	"github.com/wrble/flock/index/store"
	"github.com/wrble/flock/registry"
)

const Name = "null"

type Store struct{}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	return &Store{}, nil
}

func (i *Store) Close() error {
	return nil
}

func (i *Store) Reader() (store.KVReader, error) {
	return &reader{}, nil
}

func (i *Store) Writer() (store.KVWriter, error) {
	return &writer{}, nil
}

type reader struct{}

func (r *reader) Get(table string, key []byte) ([]byte, error) {
	return nil, nil
}

func (r *reader) GetCounter(table string, key []byte) (int64, error) {
	return 0, nil
}

func (r *reader) DocCount() (uint64, error) {
	return 0, nil
}

func (r *reader) MultiGet(table string, keys [][]byte) ([][]byte, error) {
	return make([][]byte, len(keys)), nil
}

func (r *reader) PrefixIterator(table string, prefix []byte) store.KVIterator {
	return &iterator{}
}

func (r *reader) TypedPrefixIterator(table string, prefix []byte) store.TypedKVIterator {
	return nil
}

func (r *reader) RangeIterator(table string, start, end []byte) store.KVIterator {
	return &iterator{}
}

func (r *reader) Close() error {
	return nil
}

type iterator struct{}

func (i *iterator) SeekFirst()    {}
func (i *iterator) Seek(k []byte) {}
func (i *iterator) Next()         {}

func (i *iterator) Current() ([]byte, []byte, bool) {
	return nil, nil, false
}

func (i *iterator) Key() []byte {
	return nil
}

func (i *iterator) Value() []byte {
	return nil
}

func (i *iterator) Valid() bool {
	return false
}

func (i *iterator) Close() error {
	return nil
}

type batch struct{}

func (i *batch) Set(table string, key, val []byte)                {}
func (i *batch) Delete(table string, key []byte)                  {}
func (i *batch) Merge(table string, key, val []byte)              {}
func (i *batch) Increment(table string, key []byte, amount int64) {}
func (i *batch) Reset()                                           {}
func (i *batch) Close() error                                     { return nil }

type writer struct{}

func (w *writer) NewBatch() store.KVBatch {
	return &batch{}
}

func (w *writer) NewBatchEx(options store.KVBatchOptions) (store.KVBatch, error) {
	return w.NewBatch(), nil
}

func (w *writer) ExecuteBatch(store.KVBatch) error {
	return nil
}

func (w *writer) Close() error {
	return nil
}

func init() {
	registry.RegisterKVStore(Name, New)
}

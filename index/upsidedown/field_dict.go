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

package upsidedown

import (
	"fmt"

	"github.com/wrble/flock/index"
	"github.com/wrble/flock/index/rows"
	"github.com/wrble/flock/index/store"
)

type UpsideDownCouchFieldDict struct {
	indexReader *IndexReader
	iterator    store.KVIterator
	dictRow     *rows.DictionaryRow
	dictEntry   *index.DictEntry
	field       uint16
}

func newUpsideDownCouchFieldDict(indexReader *IndexReader, field uint16, startTerm, endTerm []byte) (*UpsideDownCouchFieldDict, error) {
	dictRow := rows.NewDictionaryRow(startTerm, field, 0)
	startKey := dictRow.Key()
	if endTerm == nil {
		endTerm = []byte{ByteSeparator}
	} else {
		endTerm = incrementBytes(endTerm)
	}
	endKey := rows.NewDictionaryRow(endTerm, field, 0).Key()

	it := indexReader.kvreader.RangeIterator(dictRow.Table(), startKey, endKey)

	return &UpsideDownCouchFieldDict{
		indexReader: indexReader,
		iterator:    it,
		dictRow:     &rows.DictionaryRow{}, // Pre-alloced, reused row.
		dictEntry:   &index.DictEntry{},    // Pre-alloced, reused entry.
		field:       field,
	}, nil
}

func (r *UpsideDownCouchFieldDict) Next() (*index.DictEntry, error) {
	key, val, valid := r.iterator.Current()
	if !valid {
		return nil, nil
	}

	err := r.dictRow.ParseDictionaryK(key)
	if err != nil {
		return nil, fmt.Errorf("unexpected error parsing dictionary row key: %v", err)
	}
	err = r.dictRow.ParseDictionaryK(val)
	if err != nil {
		return nil, fmt.Errorf("unexpected error parsing dictionary row val: %v", err)
	}
	r.dictEntry.Term = string(r.dictRow.Term)
	r.dictEntry.Count = r.dictRow.Count
	// advance the iterator to the next term
	r.iterator.Next()
	return r.dictEntry, nil

}

func (r *UpsideDownCouchFieldDict) Close() error {
	return r.iterator.Close()
}

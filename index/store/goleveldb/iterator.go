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

	"github.com/syndtr/goleveldb/leveldb/iterator"
	"github.com/wrble/flock/index/store"
)

type Iterator struct {
	store    *Store
	iterator iterator.Iterator
	table    string
}

func (ldi *Iterator) Seek(key []byte) {
	if ldi.store.debug {
		fmt.Println("SEEK", string(key))
	}
	combined := store.Combine(ldi.table, key)
	valid := ldi.iterator.Seek(combined)
	if !valid && ldi.store.debug {
		fmt.Println("SEEK INVALID", string(combined))
	}
}

func (ldi *Iterator) Next() {
	ldi.iterator.Next()
	if ldi.Valid() && ldi.store.debug {
		fmt.Println("ITER", string(ldi.Key()))
	}
}

func (ldi *Iterator) Current() ([]byte, []byte, bool) {
	if ldi.Valid() {
		return ldi.Key(), ldi.Value(), true
	}
	return nil, nil, false
}

func (ldi *Iterator) Key() []byte {
	//if ldi.store.debug {
	//	fmt.Println("ITER KEY", string(ldi.iterator.Key()))
	//}
	return ldi.iterator.Key()[1:]
}

func (ldi *Iterator) Value() []byte {
	return ldi.iterator.Value()
}

func (ldi *Iterator) Valid() bool {
	return ldi.iterator.Valid()
}

func (ldi *Iterator) Close() error {
	ldi.iterator.Release()
	return nil
}

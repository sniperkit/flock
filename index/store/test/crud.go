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

package test

import (
	"testing"

	"github.com/facebookgo/ensure"
	"github.com/wrble/flock/index/store"
	"github.com/wrble/flock/index/upsidedown"
)

// basic crud tests

func CommonTestKVCrud(t *testing.T, s store.KVStore) {

	writer, err := s.Writer()
	if err != nil {
		t.Error(err)
	}

	batch := writer.NewBatch()
	ensure.Nil(t, batch.Set("a", upsidedown.NewInternalRow([]byte("key-a"), []byte("val-a"))))
	ensure.Nil(t, batch.Set("z", upsidedown.NewInternalRow([]byte("key-z"), []byte("val-z"))))
	ensure.Nil(t, batch.Delete("z", []byte("z")))
	err = writer.ExecuteBatch(batch)
	if err != nil {
		t.Fatal(err)
	}

	batch.Reset()

	ensure.Nil(t, batch.Set("b", upsidedown.NewInternalRow([]byte("key-b"), []byte("val-b"))))
	ensure.Nil(t, batch.Set("b", upsidedown.NewInternalRow([]byte("key-c"), []byte("val-c"))))
	ensure.Nil(t, batch.Set("b", upsidedown.NewInternalRow([]byte("key-d"), []byte("val-d"))))
	ensure.Nil(t, batch.Set("b", upsidedown.NewInternalRow([]byte("key-e"), []byte("val-e"))))
	ensure.Nil(t, batch.Set("b", upsidedown.NewInternalRow([]byte("key-f"), []byte("val-f"))))
	ensure.Nil(t, batch.Set("b", upsidedown.NewInternalRow([]byte("key-g"), []byte("val-g"))))
	ensure.Nil(t, batch.Set("b", upsidedown.NewInternalRow([]byte("key-h"), []byte("val-h"))))
	ensure.Nil(t, batch.Set("b", upsidedown.NewInternalRow([]byte("key-i"), []byte("val-i"))))
	ensure.Nil(t, batch.Set("b", upsidedown.NewInternalRow([]byte("key-j"), []byte("val-j"))))

	err = writer.ExecuteBatch(batch)
	if err != nil {
		t.Fatal(err)
	}
	err = writer.Close()
	if err != nil {
		t.Fatal(err)
	}

	reader, err := s.Reader()
	if err != nil {
		t.Error(err)
	}
	defer func() {
		err := reader.Close()
		if err != nil {
			t.Fatal(err)
		}
	}()
	it := reader.RangeIterator("b", nil, nil)
	key, val, valid := it.Current()
	if !valid {
		t.Fatalf("valid false, expected true")
	}
	if string(key) != "key-b" {
		t.Fatalf("expected key-b, got %s", key)
	}
	if string(val) != "val-b" {
		t.Fatalf("expected value val-b, got %s", val)
	}

	it.Next()
	key, val, valid = it.Current()
	if !valid {
		t.Fatalf("valid false, expected true")
	}
	if string(key) != "key-c" {
		t.Fatalf("expected key c, got %s", key)
	}
	if string(val) != "val-c" {
		t.Fatalf("expected value val-c, got %s", val)
	}

	it.Seek([]byte("key-i"))
	key, val, valid = it.Current()
	if !valid {
		t.Fatalf("valid false, expected true")
	}
	if string(key) != "key-i" {
		t.Fatalf("expected key key-i, got %s", key)
	}
	if string(val) != "val-i" {
		t.Fatalf("expected value val-i, got %s", val)
	}

	err = it.Close()
	if err != nil {
		t.Fatal(err)
	}
}

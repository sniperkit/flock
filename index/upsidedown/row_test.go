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
	"math"
	"reflect"
	"testing"

	"github.com/golang/protobuf/proto"
	"github.com/wrble/flock/index/rows"
)

func TestRows(t *testing.T) {
	tests := []struct {
		input  UpsideDownCouchRow
		outKey []byte
		outVal []byte
	}{
		{
			NewFieldRow(0, "name"),
			[]byte{'f', 0, 0},
			[]byte{'n', 'a', 'm', 'e', ByteSeparator},
		},
		{
			NewFieldRow(1, "desc"),
			[]byte{'f', 1, 0},
			[]byte{'d', 'e', 's', 'c', ByteSeparator},
		},
		{
			NewFieldRow(513, "style"),
			[]byte{'f', 1, 2},
			[]byte{'s', 't', 'y', 'l', 'e', ByteSeparator},
		},
		{
			rows.NewDictionaryRow([]byte{'b', 'e', 'e', 'r'}, 0, 27),
			[]byte{'d', 0, 0, 'b', 'e', 'e', 'r'},
			[]byte{27},
		},
		{
			rows.NewTermFrequencyRow([]byte{'b', 'e', 'e', 'r'}, 0, []byte("catz"), 3, 3.14),
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', ByteSeparator, 'c', 'a', 't', 'z'},
			[]byte{3, 195, 235, 163, 130, 4},
		},
		{
			rows.NewTermFrequencyRow([]byte{'b', 'e', 'e', 'r'}, 0, []byte("budweiser"), 3, 3.14),
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', ByteSeparator, 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{3, 195, 235, 163, 130, 4},
		},
		{
			rows.NewTermFrequencyRowWithTermVectors([]byte{'b', 'e', 'e', 'r'}, 0, []byte("budweiser"), 3, 3.14, []*rows.TermVector{{Field: 0, Pos: 1, Start: 3, End: 11}, {Field: 0, Pos: 2, Start: 23, End: 31}, {Field: 0, Pos: 3, Start: 43, End: 51}}),
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', ByteSeparator, 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{3, 195, 235, 163, 130, 4, 0, 1, 3, 11, 0, 0, 2, 23, 31, 0, 0, 3, 43, 51, 0},
		},
		// test larger varints
		{
			rows.NewTermFrequencyRowWithTermVectors([]byte{'b', 'e', 'e', 'r'}, 0, []byte("budweiser"), 25896, 3.14, []*rows.TermVector{{Field: 255, Pos: 1, Start: 3, End: 11}, {Field: 0, Pos: 2198, Start: 23, End: 31}, {Field: 0, Pos: 3, Start: 43, End: 51}}),
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', ByteSeparator, 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{168, 202, 1, 195, 235, 163, 130, 4, 255, 1, 1, 3, 11, 0, 0, 150, 17, 23, 31, 0, 0, 3, 43, 51, 0},
		},
		// test vectors with arrayPositions
		{
			rows.NewTermFrequencyRowWithTermVectors([]byte{'b', 'e', 'e', 'r'}, 0, []byte("budweiser"), 25896, 3.14, []*rows.TermVector{{Field: 255, Pos: 1, Start: 3, End: 11, ArrayPositions: []uint64{0}}, {Field: 0, Pos: 2198, Start: 23, End: 31, ArrayPositions: []uint64{1, 2}}, {Field: 0, Pos: 3, Start: 43, End: 51, ArrayPositions: []uint64{3, 4, 5}}}),
			[]byte{'t', 0, 0, 'b', 'e', 'e', 'r', ByteSeparator, 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{168, 202, 1, 195, 235, 163, 130, 4, 255, 1, 1, 3, 11, 1, 0, 0, 150, 17, 23, 31, 2, 1, 2, 0, 3, 43, 51, 3, 3, 4, 5},
		},
		{
			NewBackIndexRow([]byte("budweiser"), []*BackIndexTermsEntry{{Field: proto.Uint32(0), Terms: []string{"beer"}}}, nil),
			[]byte{'b', 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{10, 8, 8, 0, 18, 4, 'b', 'e', 'e', 'r'},
		},
		{
			NewBackIndexRow([]byte("budweiser"), []*BackIndexTermsEntry{{Field: proto.Uint32(0), Terms: []string{"beer"}}, {Field: proto.Uint32(1), Terms: []string{"beat"}}}, nil),
			[]byte{'b', 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{10, 8, 8, 0, 18, 4, 'b', 'e', 'e', 'r', 10, 8, 8, 1, 18, 4, 'b', 'e', 'a', 't'},
		},
		{
			NewBackIndexRow([]byte("budweiser"), []*BackIndexTermsEntry{{Field: proto.Uint32(0), Terms: []string{"beer"}}, {Field: proto.Uint32(1), Terms: []string{"beat"}}}, []*BackIndexStoreEntry{{Field: proto.Uint32(3)}, {Field: proto.Uint32(4)}, {Field: proto.Uint32(5)}}),
			[]byte{'b', 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'},
			[]byte{10, 8, 8, 0, 18, 4, 'b', 'e', 'e', 'r', 10, 8, 8, 1, 18, 4, 'b', 'e', 'a', 't', 18, 2, 8, 3, 18, 2, 8, 4, 18, 2, 8, 5},
		},
		{
			NewStoredRow([]byte("budweiser"), 0, []uint64{}, byte('t'), []byte("an american beer")),
			[]byte{'s', 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r', ByteSeparator, 0, 0},
			[]byte{'t', 'a', 'n', ' ', 'a', 'm', 'e', 'r', 'i', 'c', 'a', 'n', ' ', 'b', 'e', 'e', 'r'},
		},
		{
			NewStoredRow([]byte("budweiser"), 0, []uint64{2, 294, 3078}, byte('t'), []byte("an american beer")),
			[]byte{'s', 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r', ByteSeparator, 0, 0, 2, 166, 2, 134, 24},
			[]byte{'t', 'a', 'n', ' ', 'a', 'm', 'e', 'r', 'i', 'c', 'a', 'n', ' ', 'b', 'e', 'e', 'r'},
		},
		{
			NewInternalRow([]byte("mapping"), []byte(`{"mapping":"json content"}`)),
			[]byte{'i', 'm', 'a', 'p', 'p', 'i', 'n', 'g'},
			[]byte{'{', '"', 'm', 'a', 'p', 'p', 'i', 'n', 'g', '"', ':', '"', 'j', 's', 'o', 'n', ' ', 'c', 'o', 'n', 't', 'e', 'n', 't', '"', '}'},
		},
	}

	// test going from struct to k/v bytes
	for i, test := range tests {
		rk := test.input.Key()
		if !reflect.DeepEqual(rk, test.outKey[1:]) {
			t.Errorf("Expected key to be %v got: %v", string(test.outKey), string(rk))
		}
		rv := test.input.Value()
		if !reflect.DeepEqual(rv, test.outVal) {
			t.Errorf("Expected value to be %v got: %v for %d", test.outVal, rv, i)
		}
	}

	// now test going back from k/v bytes to struct
	for i, test := range tests {
		if !reflect.DeepEqual(test.outKey[1:], test.input.Key()) {
			t.Errorf("Expected Key: %#v got: %#v for %d", test.input.Key(), test.outKey, i)
		}
		if !reflect.DeepEqual(test.outVal, test.input.Value()) {
			t.Errorf("Expected Value: %#v got: %#v for %d", test.input.Value(), test.outVal, i)
		}
	}

}

func TestDictionaryRowValueBug197(t *testing.T) {
	// this was the smallest value that would trigger a crash
	dr := &rows.DictionaryRow{
		Field: 0,
		Term:  []byte("marty"),
		Count: 72057594037927936,
	}
	dr.Value()
	// this is the maximum possible value
	dr = &rows.DictionaryRow{
		Field: 0,
		Term:  []byte("marty"),
		Count: math.MaxUint64,
	}
	dr.Value()
	// neither of these should panic
}

func BenchmarkTermFrequencyRowEncode(b *testing.B) {
	row := rows.NewTermFrequencyRowWithTermVectors(
		[]byte{'b', 'e', 'e', 'r'},
		0,
		[]byte("budweiser"),
		3,
		3.14,
		[]*rows.TermVector{
			{
				Field: 0,
				Pos:   1,
				Start: 3,
				End:   11,
			},
			{
				Field: 0,
				Pos:   2,
				Start: 23,
				End:   31,
			},
			{
				Field: 0,
				Pos:   3,
				Start: 43,
				End:   51,
			},
		})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row.Key()
		row.Value()
	}
}

func BenchmarkTermFrequencyRowDecode(b *testing.B) {
	k := []byte{'t', 0, 0, 'b', 'e', 'e', 'r', ByteSeparator, 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r'}
	v := []byte{3, 195, 235, 163, 130, 4, 0, 1, 3, 11, 0, 0, 2, 23, 31, 0, 0, 3, 43, 51, 0}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := rows.NewTermFrequencyRowKV(k, v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkBackIndexRowEncode(b *testing.B) {
	field := uint32(1)
	t1 := "term1"
	row := NewBackIndexRow([]byte("beername"),
		[]*BackIndexTermsEntry{
			{
				Field: &field,
				Terms: []string{t1},
			},
		},
		[]*BackIndexStoreEntry{
			{
				Field: &field,
			},
		})
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row.Key()
		row.Value()
		b.Logf("%#v", row.Value())
	}
}

func BenchmarkBackIndexRowDecode(b *testing.B) {
	k := []byte{0x62, 0x62, 0x65, 0x65, 0x72, 0x6e, 0x61, 0x6d, 0x65}
	v := []byte{0xa, 0x9, 0x8, 0x1, 0x12, 0x5, 0x74, 0x65, 0x72, 0x6d, 0x31, 0x12, 0x2, 0x8, 0x1}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewBackIndexRowKV(k, v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkStoredRowEncode(b *testing.B) {
	row := NewStoredRow([]byte("budweiser"), 0, []uint64{}, byte('t'), []byte("an american beer"))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		row.Key()
		row.Value()
	}
}

func BenchmarkStoredRowDecode(b *testing.B) {
	k := []byte{'s', 'b', 'u', 'd', 'w', 'e', 'i', 's', 'e', 'r', ByteSeparator, 0, 0}
	v := []byte{'t', 'a', 'n', ' ', 'a', 'm', 'e', 'r', 'i', 'c', 'a', 'n', ' ', 'b', 'e', 'e', 'r'}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := NewStoredRowKV(k, v)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestVisitBackIndexRow(t *testing.T) {
	expected := map[uint32][]byte{
		0: []byte("beer"),
		1: []byte("beat"),
	}
	val := []byte{10, 8, 8, 0, 18, 4, 'b', 'e', 'e', 'r', 10, 8, 8, 1, 18, 4, 'b', 'e', 'a', 't', 18, 2, 8, 3, 18, 2, 8, 4, 18, 2, 8, 5}
	err := visitBackIndexRow(val, func(field uint32, term []byte) {
		if reflect.DeepEqual(expected[field], term) {
			delete(expected, field)
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(expected) > 0 {
		t.Errorf("expected visitor to see these but did not %v", expected)
	}
}

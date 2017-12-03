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
	"bytes"
	"sort"
	"sync/atomic"

	"github.com/wrble/flock/index"
	"github.com/wrble/flock/index/rows"
	"github.com/wrble/flock/index/store"
)

type UpsideDownCouchTermFieldReader struct {
	count              uint64
	indexReader        *IndexReader
	iterator           store.TypedKVIterator
	term               []byte
	tfrNext            *rows.TermFrequencyRow
	tfrPrealloc        rows.TermFrequencyRow
	keyBuf             []byte
	field              uint16
	includeTermVectors bool
}

func newUpsideDownCouchTermFieldReader(indexReader *IndexReader, term []byte, field uint16, includeFreq, includeNorm, includeTermVectors bool) (*UpsideDownCouchTermFieldReader, error) {
	bufNeeded := rows.TermFrequencyRowKeySize(term, nil)
	if bufNeeded < rows.DictionaryRowKeySize(term) {
		bufNeeded = rows.DictionaryRowKeySize(term)
	}

	dictRow := rows.NewDictionaryRow(term, field, 0)
	val, err := indexReader.kvreader.GetCounter(dictRow.Table(), dictRow.Key())
	if err != nil {
		return nil, err
	}
	if val == -1 {
		atomic.AddUint64(&indexReader.index.stats.termSearchersStarted, uint64(1))
		rv := &UpsideDownCouchTermFieldReader{
			count:              0,
			term:               term,
			field:              field,
			includeTermVectors: includeTermVectors,
		}
		rv.tfrNext = &rv.tfrPrealloc
		return rv, nil
	}

	buf := make([]byte, bufNeeded)
	bufUsed := rows.TermFrequencyRowKeyTo(buf, field, term, nil)
	it := indexReader.kvreader.TypedPrefixIterator("t", buf[:bufUsed])

	atomic.AddUint64(&indexReader.index.stats.termSearchersStarted, uint64(1))
	return &UpsideDownCouchTermFieldReader{
		indexReader:        indexReader,
		iterator:           it,
		count:              uint64(val),
		term:               term,
		field:              field,
		includeTermVectors: includeTermVectors,
	}, nil
}

func (r *UpsideDownCouchTermFieldReader) Count() uint64 {
	return r.count
}

func (r *UpsideDownCouchTermFieldReader) Next(preAlloced *index.TermFieldDoc) (*index.TermFieldDoc, error) {
	if r.iterator != nil {
		// We treat tfrNext also like an initialization flag, which
		// tells us whether we need to invoke the underlying
		// iterator.Next().  The first time, don't call iterator.Next().
		if r.tfrNext != nil {
			r.iterator.Next()
		} else {
			r.tfrNext = &r.tfrPrealloc
		}
		_, val, valid := r.iterator.Current()
		if valid {
			currentRow, err := rows.NewTermFrequencyRowFromMap(val)
			if err != nil {
				return nil, err
			}
			rv := preAlloced
			if rv == nil {
				rv = &index.TermFieldDoc{}
			}
			rv.ID = append(rv.ID, currentRow.Doc...)
			rv.Freq = currentRow.Freq
			rv.Score = currentRow.Score
			if currentRow.Vectors != nil {
				rv.Vectors = r.indexReader.index.termFieldVectorsFromTermVectors(currentRow.Vectors)
			}
			return rv, nil
		}
	}
	return nil, nil
}

func (r *UpsideDownCouchTermFieldReader) Advance(docID index.IndexInternalID, preAlloced *index.TermFieldDoc) (rv *index.TermFieldDoc, err error) {
	if r.iterator != nil {
		if r.tfrNext == nil {
			r.tfrNext = &rows.TermFrequencyRow{}
		}
		tfr := rows.InitTermFrequencyRow(r.tfrNext, r.term, r.field, docID, 0, 0)
		r.iterator.Seek(tfr.Key())
		_, val, valid := r.iterator.Current()
		if valid {
			currentRow, err := rows.NewTermFrequencyRowFromMap(val)
			if err != nil {
				return nil, err
			}
			rv = preAlloced
			if rv == nil {
				rv = &index.TermFieldDoc{}
			}
			rv.ID = append(rv.ID, tfr.Doc...)
			rv.Freq = currentRow.Freq
			rv.Score = currentRow.Score
			if currentRow.Vectors != nil {
				rv.Vectors = r.indexReader.index.termFieldVectorsFromTermVectors(currentRow.Vectors)
			}
			return rv, nil
		}
	}
	return nil, nil
}

func (r *UpsideDownCouchTermFieldReader) Close() error {
	if r.indexReader != nil {
		atomic.AddUint64(&r.indexReader.index.stats.termSearchersFinished, uint64(1))
	}
	if r.iterator != nil {
		return r.iterator.Close()
	}
	return nil
}

type UpsideDownCouchDocIDReader struct {
	indexReader *IndexReader
	iterator    store.KVIterator
	only        []string
	onlyPos     int
	onlyMode    bool
}

func newUpsideDownCouchDocIDReaderOnly(indexReader *IndexReader, ids []string) (*UpsideDownCouchDocIDReader, error) {
	// we don't actually own the list of ids, so if before we sort we must copy
	idsCopy := make([]string, len(ids))
	copy(idsCopy, ids)
	// ensure ids are sorted
	sort.Strings(idsCopy)
	startBytes := []byte{0x0}
	if len(idsCopy) > 0 {
		startBytes = []byte(idsCopy[0])
	}
	endBytes := []byte{0xff}
	if len(idsCopy) > 0 {
		endBytes = incrementBytes([]byte(idsCopy[len(idsCopy)-1]))
	}
	bisr := NewBackIndexRow(startBytes, nil, nil)
	bier := NewBackIndexRow(endBytes, nil, nil)
	it := indexReader.kvreader.RangeIterator(bisr.Table(), bisr.Key(), bier.Key())

	return &UpsideDownCouchDocIDReader{
		indexReader: indexReader,
		iterator:    it,
		only:        idsCopy,
		onlyMode:    true,
	}, nil
}

func (r *UpsideDownCouchDocIDReader) Next() (index.IndexInternalID, error) {
	key, val, valid := r.iterator.Current()

	if r.onlyMode {
		var rv index.IndexInternalID
		for valid && r.onlyPos < len(r.only) {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(br.doc, []byte(r.only[r.onlyPos])) {
				ok := r.nextOnly()
				if !ok {
					return nil, nil
				}
				r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
				key, val, valid = r.iterator.Current()
				continue
			} else {
				rv = append([]byte(nil), br.doc...)
				break
			}
		}
		if valid && r.onlyPos < len(r.only) {
			ok := r.nextOnly()
			if ok {
				r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
			}
			return rv, nil
		}

	} else {
		if valid {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}
			rv := append([]byte(nil), br.doc...)
			r.iterator.Next()
			return rv, nil
		}
	}
	return nil, nil
}

func (r *UpsideDownCouchDocIDReader) Advance(docID index.IndexInternalID) (index.IndexInternalID, error) {

	if r.onlyMode {
		r.onlyPos = sort.SearchStrings(r.only, string(docID))
		if r.onlyPos >= len(r.only) {
			// advanced to key after our last only key
			return nil, nil
		}
		r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
		key, val, valid := r.iterator.Current()

		var rv index.IndexInternalID
		for valid && r.onlyPos < len(r.only) {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}
			if !bytes.Equal(br.doc, []byte(r.only[r.onlyPos])) {
				// the only key we seek'd to didn't exist
				// now look for the closest key that did exist in only
				r.onlyPos = sort.SearchStrings(r.only, string(br.doc))
				if r.onlyPos >= len(r.only) {
					// advanced to key after our last only key
					return nil, nil
				}
				// now seek to this new only key
				r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
				key, val, valid = r.iterator.Current()
				continue
			} else {
				rv = append([]byte(nil), br.doc...)
				break
			}
		}
		if valid && r.onlyPos < len(r.only) {
			ok := r.nextOnly()
			if ok {
				r.iterator.Seek(NewBackIndexRow([]byte(r.only[r.onlyPos]), nil, nil).Key())
			}
			return rv, nil
		}
	} else {
		bir := NewBackIndexRow(docID, nil, nil)
		r.iterator.Seek(bir.Key())
		key, val, valid := r.iterator.Current()
		if valid {
			br, err := NewBackIndexRowKV(key, val)
			if err != nil {
				return nil, err
			}
			rv := append([]byte(nil), br.doc...)
			r.iterator.Next()
			return rv, nil
		}
	}
	return nil, nil
}

func (r *UpsideDownCouchDocIDReader) Close() error {
	return r.iterator.Close()
}

// move the r.only pos forward one, skipping duplicates
// return true if there is more data, or false if we got to the end of the list
func (r *UpsideDownCouchDocIDReader) nextOnly() bool {

	// advance 1 position, until we see a different key
	//   it's already sorted, so this skips duplicates
	start := r.onlyPos
	r.onlyPos++
	for r.onlyPos < len(r.only) && r.only[r.onlyPos] == r.only[start] {
		start = r.onlyPos
		r.onlyPos++
	}
	// inidicate if we got to the end of the list
	return r.onlyPos < len(r.only)
}

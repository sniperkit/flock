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
	"encoding/binary"
	"fmt"
	"io"

	"github.com/golang/protobuf/proto"
	"github.com/wrble/flock/index/rows"
)

const ByteSeparator byte = 0xff

type UpsideDownCouchRowStream chan UpsideDownCouchRow

type UpsideDownCouchRow interface {
	Table() string
	Key() []byte
	Value() []byte
	ValueSize() int
	ValueTo([]byte) (int, error)
}

// VERSION

type VersionRow struct {
	version uint8
}

func (v *VersionRow) Table() string {
	return "v"
}

func (v *VersionRow) Key() []byte {
	return []byte(StaticKey)
}

func (v *VersionRow) Value() []byte {
	return []byte{byte(v.version)}
}

func (v *VersionRow) ValueSize() int {
	return 1
}

func (v *VersionRow) ValueTo(buf []byte) (int, error) {
	buf[0] = v.version
	return 1, nil
}

func (v *VersionRow) String() string {
	return fmt.Sprintf("Version: %d", v.version)
}

func NewVersionRow(version uint8) *VersionRow {
	return &VersionRow{
		version: version,
	}
}

func NewVersionRowKV(key, value []byte) (*VersionRow, error) {
	rv := VersionRow{}
	buf := bytes.NewBuffer(value)
	err := binary.Read(buf, binary.LittleEndian, &rv.version)
	if err != nil {
		return nil, err
	}
	return &rv, nil
}

// INTERNAL STORAGE

type InternalRow struct {
	key []byte
	val []byte
}

func (v *InternalRow) Table() string {
	return "i"
}

func (i *InternalRow) Key() []byte {
	return i.key
}

func (i *InternalRow) Value() []byte {
	return i.val
}

func (i *InternalRow) ValueSize() int {
	return len(i.val)
}

func (i *InternalRow) ValueTo(buf []byte) (int, error) {
	actual := copy(buf, i.val)
	return actual, nil
}

func (i *InternalRow) String() string {
	return fmt.Sprintf("InternalStore - Key: %s (% x) Val: %s (% x)", i.key, i.key, i.val, i.val)
}

func NewInternalRow(key, val []byte) *InternalRow {
	return &InternalRow{
		key: key,
		val: val,
	}
}

func NewInternalRowKV(key, value []byte) (*InternalRow, error) {
	rv := InternalRow{}
	rv.key = key[1:]
	rv.val = value
	return &rv, nil
}

// FIELD definition

type FieldRow struct {
	index uint16
	name  string
}

func (v *FieldRow) Table() string {
	return "f"
}

func (f *FieldRow) Key() []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, f.index)
	return buf
}

func (f *FieldRow) Value() []byte {
	return append([]byte(f.name), ByteSeparator)
}

func (f *FieldRow) ValueSize() int {
	return len(f.name) + 1
}

func (f *FieldRow) ValueTo(buf []byte) (int, error) {
	size := copy(buf, f.name)
	buf[size] = ByteSeparator
	return size + 1, nil
}

func (f *FieldRow) String() string {
	return fmt.Sprintf("Field: %d Name: %s", f.index, f.name)
}

func NewFieldRow(index uint16, name string) *FieldRow {
	return &FieldRow{
		index: index,
		name:  name,
	}
}

func NewFieldRowKV(key, value []byte) (*FieldRow, error) {
	rv := FieldRow{}

	buf := bytes.NewBuffer(key)
	err := binary.Read(buf, binary.LittleEndian, &rv.index)
	if err != nil {
		return nil, err
	}

	buf = bytes.NewBuffer(value)
	rv.name, err = buf.ReadString(ByteSeparator)
	if err != nil {
		return nil, err
	}
	rv.name = rv.name[:len(rv.name)-1] // trim off separator byte

	return &rv, nil
}

type BackIndexRow struct {
	doc           []byte
	termsEntries  []*BackIndexTermsEntry
	storedEntries []*BackIndexStoreEntry
}

func (v *BackIndexRow) Table() string {
	return "b"
}

func (br *BackIndexRow) AllTermKeys() [][]byte {
	if br == nil {
		return nil
	}
	rv := make([][]byte, 0, len(br.termsEntries)) // FIXME this underestimates severely
	for _, termsEntry := range br.termsEntries {
		for i := range termsEntry.Terms {
			termRow := rows.NewTermFrequencyRow([]byte(termsEntry.Terms[i]), uint16(termsEntry.GetField()), br.doc, 0, 0)
			rv = append(rv, termRow.Key())
		}
	}
	return rv
}

func (br *BackIndexRow) AllStoredKeys() [][]byte {
	if br == nil {
		return nil
	}
	rv := make([][]byte, len(br.storedEntries))
	for i, storedEntry := range br.storedEntries {
		storedRow := NewStoredRow(br.doc, uint16(storedEntry.GetField()), storedEntry.GetArrayPositions(), 'x', []byte{})
		rv[i] = storedRow.Key()
	}
	return rv
}

func (br *BackIndexRow) Key() []byte {
	return br.doc
}

func (br *BackIndexRow) Value() []byte {
	buf := make([]byte, br.ValueSize())
	size, _ := br.ValueTo(buf)
	return buf[:size]
}

func (br *BackIndexRow) ValueSize() int {
	birv := &BackIndexRowValue{
		TermsEntries:  br.termsEntries,
		StoredEntries: br.storedEntries,
	}
	return birv.Size()
}

func (br *BackIndexRow) ValueTo(buf []byte) (int, error) {
	birv := &BackIndexRowValue{
		TermsEntries:  br.termsEntries,
		StoredEntries: br.storedEntries,
	}
	return birv.MarshalTo(buf)
}

func (br *BackIndexRow) String() string {
	return fmt.Sprintf("Backindex DocId: `%s` Terms Entries: %v, Stored Entries: %v", string(br.doc), br.termsEntries, br.storedEntries)
}

func NewBackIndexRow(docID []byte, entries []*BackIndexTermsEntry, storedFields []*BackIndexStoreEntry) *BackIndexRow {
	return &BackIndexRow{
		doc:           docID,
		termsEntries:  entries,
		storedEntries: storedFields,
	}
}

func NewBackIndexRowKV(key, value []byte) (*BackIndexRow, error) {
	rv := BackIndexRow{}
	buf := bytes.NewBuffer(key)
	var err error

	rv.doc, err = buf.ReadBytes(ByteSeparator)
	if err == io.EOF && len(rv.doc) < 1 {
		err = fmt.Errorf("invalid doc length 0 - % x", key)
	}
	if err != nil && err != io.EOF {
		return nil, err
	} else if err == nil {
		rv.doc = rv.doc[:len(rv.doc)-1] // trim off separator byte
	}

	var birv BackIndexRowValue
	err = proto.Unmarshal(value, &birv)
	if err != nil {
		return nil, err
	}
	rv.termsEntries = birv.TermsEntries
	rv.storedEntries = birv.StoredEntries

	return &rv, nil
}

// STORED

type StoredRow struct {
	doc            []byte
	field          uint16
	arrayPositions []uint64
	typ            byte
	value          []byte
}

func (v *StoredRow) Table() string {
	return "s"
}

func (s *StoredRow) Key() []byte {
	buf := make([]byte, s.KeySize())
	size, _ := s.KeyTo(buf)
	return buf[0:size]
}

func (s *StoredRow) KeySize() int {
	return len(s.doc) + 1 + 2 + (binary.MaxVarintLen64 * len(s.arrayPositions))
}

func (s *StoredRow) KeyTo(buf []byte) (int, error) {
	docLen := len(s.doc)
	copy(buf, s.doc)
	buf[docLen] = ByteSeparator
	binary.LittleEndian.PutUint16(buf[docLen+1:], s.field)
	bytesUsed := docLen + 1 + 2
	for _, arrayPosition := range s.arrayPositions {
		varbytes := binary.PutUvarint(buf[bytesUsed:], arrayPosition)
		bytesUsed += varbytes
	}
	return bytesUsed, nil
}

func (s *StoredRow) Value() []byte {
	buf := make([]byte, s.ValueSize())
	size, _ := s.ValueTo(buf)
	return buf[:size]
}

func (s *StoredRow) ValueSize() int {
	return len(s.value) + 1
}

func (s *StoredRow) ValueTo(buf []byte) (int, error) {
	buf[0] = s.typ
	used := copy(buf[1:], s.value)
	return used + 1, nil
}

func (s *StoredRow) String() string {
	return fmt.Sprintf("Document: %s Field %d, Array Positions: %v, Type: %s Value: %s", s.doc, s.field, s.arrayPositions, string(s.typ), s.value)
}

func (s *StoredRow) ScanPrefixForDoc() []byte {
	docLen := len(s.doc)
	buf := make([]byte, docLen+1)
	copy(buf, s.doc)
	buf[docLen] = ByteSeparator
	return buf
}

func NewStoredRow(docID []byte, field uint16, arrayPositions []uint64, typ byte, value []byte) *StoredRow {
	return &StoredRow{
		doc:            docID,
		field:          field,
		arrayPositions: arrayPositions,
		typ:            typ,
		value:          value,
	}
}

func NewStoredRowK(key []byte) (*StoredRow, error) {
	rv := StoredRow{}

	buf := bytes.NewBuffer(key)
	var err error
	rv.doc, err = buf.ReadBytes(ByteSeparator)
	if len(rv.doc) < 2 { // 1 for min doc id length, 1 for separator
		err = fmt.Errorf("invalid doc length 0")
		return nil, err
	}

	rv.doc = rv.doc[:len(rv.doc)-1] // trim off separator byte

	err = binary.Read(buf, binary.LittleEndian, &rv.field)
	if err != nil {
		return nil, err
	}

	rv.arrayPositions = make([]uint64, 0)
	nextArrayPos, err := binary.ReadUvarint(buf)
	for err == nil {
		rv.arrayPositions = append(rv.arrayPositions, nextArrayPos)
		nextArrayPos, err = binary.ReadUvarint(buf)
	}
	return &rv, nil
}

func NewStoredRowKV(key, value []byte) (*StoredRow, error) {
	rv, err := NewStoredRowK(key)
	if err != nil {
		return nil, err
	}
	rv.typ = value[0]
	rv.value = value[1:]
	return rv, nil
}

type backIndexFieldTermVisitor func(field uint32, term []byte)

// visitBackIndexRow is designed to process a protobuf encoded
// value, without creating unnecessary garbage.  Instead values are passed
// to a callback, inspected first, and only copied if necessary.
// Due to the fact that this borrows from generated code, it must be marnually
// updated if the protobuf definition changes.
//
// This code originates from:
// func (m *BackIndexRowValue) Unmarshal(data []byte) error
// the sections which create garbage or parse unintersting sections
// have been commented out.  This was done by design to allow for easier
// merging in the future if that original function is regenerated
func visitBackIndexRow(data []byte, callback backIndexFieldTermVisitor) error {
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field TermsEntries", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if msglen < 0 {
				return ErrInvalidLengthUpsidedown
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			// dont parse term entries
			// m.TermsEntries = append(m.TermsEntries, &BackIndexTermsEntry{})
			// if err := m.TermsEntries[len(m.TermsEntries)-1].Unmarshal(data[iNdEx:postIndex]); err != nil {
			// 	return err
			// }
			// instead, inspect them
			if err := visitBackIndexRowFieldTerms(data[iNdEx:postIndex], callback); err != nil {
				return err
			}
			iNdEx = postIndex
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field StoredEntries", wireType)
			}
			var msglen int
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				msglen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + msglen
			if msglen < 0 {
				return ErrInvalidLengthUpsidedown
			}
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			// don't parse stored entries
			// m.StoredEntries = append(m.StoredEntries, &BackIndexStoreEntry{})
			// if err := m.StoredEntries[len(m.StoredEntries)-1].Unmarshal(data[iNdEx:postIndex]); err != nil {
			// 	return err
			// }
			iNdEx = postIndex
		default:
			var sizeOfWire int
			for {
				sizeOfWire++
				wire >>= 7
				if wire == 0 {
					break
				}
			}
			iNdEx -= sizeOfWire
			skippy, err := skipUpsidedown(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthUpsidedown
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			// don't track unrecognized data
			//m.XXX_unrecognized = append(m.XXX_unrecognized, data[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}

	return nil
}

// visitBackIndexRowFieldTerms is designed to process a protobuf encoded
// sub-value within the BackIndexRowValue, without creating unnecessary garbage.
// Instead values are passed to a callback, inspected first, and only copied if
// necessary.  Due to the fact that this borrows from generated code, it must
// be marnually updated if the protobuf definition changes.
//
// This code originates from:
// func (m *BackIndexTermsEntry) Unmarshal(data []byte) error {
// the sections which create garbage or parse uninteresting sections
// have been commented out.  This was done by design to allow for easier
// merging in the future if that original function is regenerated
func visitBackIndexRowFieldTerms(data []byte, callback backIndexFieldTermVisitor) error {
	var theField uint32

	var hasFields [1]uint64
	l := len(data)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := data[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Field", wireType)
			}
			var v uint32
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				v |= (uint32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			// m.Field = &v
			theField = v
			hasFields[0] |= uint64(0x00000001)
		case 2:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Terms", wireType)
			}
			var stringLen uint64
			for shift := uint(0); ; shift += 7 {
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := data[iNdEx]
				iNdEx++
				stringLen |= (uint64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			postIndex := iNdEx + int(stringLen)
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			//m.Terms = append(m.Terms, string(data[iNdEx:postIndex]))
			callback(theField, data[iNdEx:postIndex])
			iNdEx = postIndex
		default:
			var sizeOfWire int
			for {
				sizeOfWire++
				wire >>= 7
				if wire == 0 {
					break
				}
			}
			iNdEx -= sizeOfWire
			skippy, err := skipUpsidedown(data[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthUpsidedown
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			//m.XXX_unrecognized = append(m.XXX_unrecognized, data[iNdEx:iNdEx+skippy]...)
			iNdEx += skippy
		}
	}
	// if hasFields[0]&uint64(0x00000001) == 0 {
	// 	return new(github_com_golang_protobuf_proto.RequiredNotSetError)
	// }

	return nil
}

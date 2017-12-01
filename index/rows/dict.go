package rows

import (
	"encoding/binary"
	"fmt"
)

// DICTIONARY

const DictionaryRowMaxValueSize = binary.MaxVarintLen64
const DictionaryTable = "d"

type DictionaryRow struct {
	Term  []byte
	Count uint64
	Field uint16
}

func (v *DictionaryRow) Table() string {
	return DictionaryTable
}

func (dr *DictionaryRow) Key() []byte {
	buf := make([]byte, DictionaryRowKeySize(dr.Term))
	binary.LittleEndian.PutUint16(buf, dr.Field)
	copy(buf[2:], dr.Term)
	return buf
}

func DictionaryRowKeySize(term []byte) int {
	return len(term) + 2
}

func (dr *DictionaryRow) Value() []byte {
	buf := make([]byte, dr.ValueSize())
	size, _ := dr.ValueTo(buf)
	return buf[:size]
}

func (dr *DictionaryRow) ValueSize() int {
	return DictionaryRowMaxValueSize
}

func (dr *DictionaryRow) ValueTo(buf []byte) (int, error) {
	used := binary.PutUvarint(buf, dr.Count)
	return used, nil
}

func (dr *DictionaryRow) String() string {
	return fmt.Sprintf("Dictionary Term: `%s` Field: %d Count: %d ", string(dr.Term), dr.Field, dr.Count)
}

func NewDictionaryRow(term []byte, field uint16, count uint64) *DictionaryRow {
	return &DictionaryRow{
		Term:  term,
		Field: field,
		Count: count,
	}
}

func NewDictionaryRowK(key []byte) (*DictionaryRow, error) {
	rv := &DictionaryRow{}
	err := rv.ParseDictionaryK(key)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func (dr *DictionaryRow) ParseDictionaryK(key []byte) error {
	dr.Field = binary.LittleEndian.Uint16(key[:2])
	if dr.Term != nil {
		dr.Term = dr.Term[:0]
	}
	dr.Term = append(dr.Term, key[2:]...)
	return nil
}

func (dr *DictionaryRow) ParseDictionaryV(value []byte) error {
	count, err := dictionaryRowParseV(value)
	if err != nil {
		return err
	}
	dr.Count = count
	return nil
}

func dictionaryRowParseV(value []byte) (uint64, error) {
	count, nread := binary.Uvarint(value)
	if nread <= 0 {
		return 0, fmt.Errorf("DictionaryRow parse Uvarint error, nread: %d", nread)
	}
	return count, nil
}

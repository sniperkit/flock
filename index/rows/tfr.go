package rows

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
)

// TERM FIELD FREQUENCY

type TermVector struct {
	Field          uint16
	ArrayPositions []uint64
	Pos            uint64
	Start          uint64
	End            uint64
}

func (tv *TermVector) String() string {
	return fmt.Sprintf("Field: %d Pos: %d Start: %d End %d ArrayPositions: %#v", tv.Field, tv.Pos, tv.Start, tv.End, tv.ArrayPositions)
}

type TermFrequencyRow struct {
	Term    []byte
	Doc     []byte
	Freq    float32
	Score   float32 // a "pre" score of this document if this term was searched for, allows truncation of term frequency iteration
	Vectors []*TermVector
	Field   uint16
}

func (v *TermFrequencyRow) Table() string {
	return "t"
}

func (tfr *TermFrequencyRow) ScanPrefixForField() []byte {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, tfr.Field)
	return buf
}

func (tfr *TermFrequencyRow) ScanPrefixForFieldTermPrefix() []byte {
	buf := make([]byte, 2+len(tfr.Term))
	binary.LittleEndian.PutUint16(buf[0:2], tfr.Field)
	copy(buf[2:], tfr.Term)
	return buf
}

func (tfr *TermFrequencyRow) ScanPrefixForFieldTerm() []byte {
	buf := make([]byte, 3+len(tfr.Term))
	binary.LittleEndian.PutUint16(buf[0:2], tfr.Field)
	termLen := copy(buf[2:], tfr.Term)
	buf[2+termLen] = ByteSeparator
	return buf
}

func (tfr *TermFrequencyRow) Key() []byte {
	buf := make([]byte, TermFrequencyRowKeySize(tfr.Term, tfr.Doc))
	size, _ := tfr.KeyTo(buf)
	return buf[:size]
}

func TermFrequencyRowKeySize(term, doc []byte) int {
	return 3 + len(term) + len(doc)
}

func (tfr *TermFrequencyRow) KeyTo(buf []byte) (int, error) {
	return TermFrequencyRowKeyTo(buf, tfr.Field, tfr.Term, tfr.Doc), nil
}

func (tfr *TermFrequencyRow) Value() []byte {
	buf := make([]byte, tfr.ValueSize())
	size, _ := tfr.ValueTo(buf)
	return buf[:size]
}

func (tfr *TermFrequencyRow) ValueTo(buf []byte) (int, error) {
	used := binary.PutUvarint(buf[:binary.MaxVarintLen64], 0)

	normuint32 := math.Float32bits(tfr.Score)
	newbuf := buf[used : used+binary.MaxVarintLen64]
	used += binary.PutUvarint(newbuf, uint64(normuint32))

	for _, vector := range tfr.Vectors {
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], uint64(vector.Field))
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], vector.Pos)
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], vector.Start)
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], vector.End)
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], uint64(len(vector.ArrayPositions)))
		for _, arrayPosition := range vector.ArrayPositions {
			used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], arrayPosition)
		}
	}
	return used, nil
}

func TermFrequencyRowKeyTo(buf []byte, field uint16, term, doc []byte) int {
	binary.LittleEndian.PutUint16(buf[0:2], field)
	termLen := copy(buf[2:], term)
	buf[2+termLen] = ByteSeparator
	docLen := copy(buf[2+termLen+1:], doc)
	return 2 + termLen + 1 + docLen
}

func (tfr *TermFrequencyRow) DictionaryRowKey() []byte {
	dr := NewDictionaryRow(tfr.Term, tfr.Field, 0)
	return dr.Key()
}

func (tfr *TermFrequencyRow) TFVectorsValue() []byte {
	buf := make([]byte, tfr.ValueSize())
	size, _ := tfr.TFVectorsValueTo(buf)
	return buf[:size]
}

func (tfr *TermFrequencyRow) ValueSize() int {
	bufLen := binary.MaxVarintLen64 + binary.MaxVarintLen64
	for _, vector := range tfr.Vectors {
		bufLen += (binary.MaxVarintLen64 * 4) + (1+len(vector.ArrayPositions))*binary.MaxVarintLen64
	}
	return bufLen
}

func (tfr *TermFrequencyRow) TFVectorsValueTo(buf []byte) (int, error) {
	used := 0

	for _, vector := range tfr.Vectors {
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], uint64(vector.Field))
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], vector.Pos)
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], vector.Start)
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], vector.End)
		used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], uint64(len(vector.ArrayPositions)))
		for _, arrayPosition := range vector.ArrayPositions {
			used += binary.PutUvarint(buf[used:used+binary.MaxVarintLen64], arrayPosition)
		}
	}
	return used, nil
}

func (tfr *TermFrequencyRow) String() string {
	return fmt.Sprintf("Term: `%s` Field: %d DocId: `%s` Frequency: %d Score: %f Vectors: %v", string(tfr.Term), tfr.Field, string(tfr.Doc), tfr.Freq, tfr.Score, tfr.Vectors)
}

func InitTermFrequencyRow(tfr *TermFrequencyRow, term []byte, field uint16, docID []byte, freq float32, score float32) *TermFrequencyRow {
	tfr.Term = term
	tfr.Field = field
	tfr.Doc = docID
	tfr.Freq = freq
	tfr.Score = score
	return tfr
}

func NewTermFrequencyRow(term []byte, field uint16, docID []byte, freq float32, score float32) *TermFrequencyRow {
	return &TermFrequencyRow{
		Term:  term,
		Field: field,
		Doc:   docID,
		Freq:  freq,
		Score: score,
	}
}

func NewTermFrequencyRowWithTermVectors(term []byte, field uint16, docID []byte, freq float32, score float32, vectors []*TermVector) *TermFrequencyRow {
	return &TermFrequencyRow{
		Term:    term,
		Field:   field,
		Doc:     docID,
		Freq:    freq,
		Score:   score,
		Vectors: vectors,
	}
}

func NewTermFrequencyRowK(key []byte) (*TermFrequencyRow, error) {
	rv := &TermFrequencyRow{}
	err := rv.parseK(key)
	if err != nil {
		return nil, err
	}
	return rv, nil
}

func (tfr *TermFrequencyRow) parseK(key []byte) error {
	keyLen := len(key)
	if keyLen < 3 {
		return fmt.Errorf("invalid term frequency key, no valid field")
	}
	tfr.Field = binary.LittleEndian.Uint16(key[1:3])

	termEndPos := bytes.IndexByte(key[3:], ByteSeparator)
	if termEndPos < 0 {
		return fmt.Errorf("invalid term frequency key, no byte separator terminating term")
	}
	tfr.Term = key[3 : 3+termEndPos]

	docLen := keyLen - (3 + termEndPos + 1)
	if docLen < 1 {
		return fmt.Errorf("invalid term frequency key, empty docid")
	}
	tfr.Doc = key[3+termEndPos+1:]

	return nil
}

func (tfr *TermFrequencyRow) parseKDoc(key []byte, term []byte) error {
	tfr.Doc = key[3+len(term):]
	if len(tfr.Doc) <= 0 {
		return fmt.Errorf("invalid term frequency key, empty docid")
	}

	return nil
}

func (tfr *TermFrequencyRow) parseTFVectorsV(value []byte, includeTermVectors bool) error {
	bytesRead := 0
	currOffset := bytesRead

	tfr.Vectors = nil
	if !includeTermVectors {
		return nil
	}

	var field uint64
	field, bytesRead = binary.Uvarint(value[currOffset:])
	for bytesRead > 0 {
		currOffset += bytesRead
		tv := TermVector{}
		tv.Field = uint16(field)
		// at this point we expect at least one term vector
		if tfr.Vectors == nil {
			tfr.Vectors = make([]*TermVector, 0)
		}

		tv.Pos, bytesRead = binary.Uvarint(value[currOffset:])
		if bytesRead <= 0 {
			return fmt.Errorf("invalid term frequency value, vector contains no position")
		}
		currOffset += bytesRead

		tv.Start, bytesRead = binary.Uvarint(value[currOffset:])
		if bytesRead <= 0 {
			return fmt.Errorf("invalid term frequency value, vector contains no start")
		}
		currOffset += bytesRead

		tv.End, bytesRead = binary.Uvarint(value[currOffset:])
		if bytesRead <= 0 {
			return fmt.Errorf("invalid term frequency value, vector contains no end")
		}
		currOffset += bytesRead

		var arrayPositionsLen uint64 = 0
		arrayPositionsLen, bytesRead = binary.Uvarint(value[currOffset:])
		if bytesRead <= 0 {
			return fmt.Errorf("invalid term frequency value, vector contains no arrayPositionLen")
		}
		currOffset += bytesRead

		if arrayPositionsLen > 0 {
			tv.ArrayPositions = make([]uint64, arrayPositionsLen)
			for i := 0; uint64(i) < arrayPositionsLen; i++ {
				tv.ArrayPositions[i], bytesRead = binary.Uvarint(value[currOffset:])
				if bytesRead <= 0 {
					return fmt.Errorf("invalid term frequency value, vector contains no arrayPosition of index %d", i)
				}
				currOffset += bytesRead
			}
		}

		tfr.Vectors = append(tfr.Vectors, &tv)
		// try to read next record (may not exist)
		field, bytesRead = binary.Uvarint(value[currOffset:])
	}
	if len(value[currOffset:]) > 0 && bytesRead <= 0 {
		return fmt.Errorf("invalid term frequency value, vector field invalid")
	}

	return nil
}

func NewTermFrequencyRowKV(key, value []byte) (*TermFrequencyRow, error) {
	rv, err := NewTermFrequencyRowK(key)
	if err != nil {
		return nil, err
	}

	err = rv.parseTFVectorsV(value, true)
	if err != nil {
		return nil, err
	}
	return rv, nil

}

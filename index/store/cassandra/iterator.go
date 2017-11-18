package cassandra

import (
	"bytes"
	"fmt"

	"github.com/gocql/gocql"
)

type Iterator struct {
	store    *Store
	iterator *gocql.Iter

	startKey []byte

	currentKey   []byte
	currentValue []byte
	currentValid bool
}

func (ldi *Iterator) Seek(key []byte) {
	if ldi.store.debug {
		fmt.Println("SEEK", string(key))
	}
	for {
		if ldi.store.debug && !ldi.Valid() {
			fmt.Println("SEEK INVALID: ", string(key))
			return
		}
		if ldi.store.debug {
			fmt.Println("SEEK EQ", string(key), string(ldi.currentKey))
		}
		if bytes.Compare(ldi.currentKey, key) >= 0 {
			if ldi.store.debug {
				fmt.Println("SEEK DONE: ", string(key))
			}
			return
		}
		ldi.Next()
	}
}

func (ldi *Iterator) Next() {
	ldi.currentValid = ldi.iterator.Scan(&ldi.currentValue, &ldi.currentKey)
	if !bytes.HasPrefix(ldi.currentKey, ldi.startKey) {
		ldi.currentValid = false
	}
	if ldi.store.debug {
		fmt.Println("NEXT", ldi.currentValid, string(ldi.currentKey))
	}
}

func (ldi *Iterator) Current() ([]byte, []byte, bool) {
	if ldi.currentValid {
		return ldi.Key(), ldi.Value(), true
	}
	return nil, nil, false
}

func (ldi *Iterator) Key() []byte {
	if ldi.store.debug {
		fmt.Println("ITER KEY", string(ldi.currentKey))
	}
	return ldi.currentKey
}

func (ldi *Iterator) Value() []byte {
	return ldi.currentValue
}

func (ldi *Iterator) Valid() bool {
	return ldi.currentValid
}

func (ldi *Iterator) Close() error {
	return nil
}

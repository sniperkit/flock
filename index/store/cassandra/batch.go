package cassandra

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/wrble/flock/index/store"
)

type Batch struct {
	store *Store
	merge *EmulatedMerge
	batch *gocql.Batch
}

func (b *Batch) Set(table string, key, val []byte) {
	b.batch.Query(`INSERT INTO `+b.store.tableName+` (type, key, value) VALUES (?, ?, ?)`, table, key, val)
	if b.store.debug {
		fmt.Println("INSERT TABLE:", b.store.tableName, table, string(key), string(val))
	}
}

func (b *Batch) Delete(table string, key []byte) {
	b.batch.Query(`DELETE FROM `+b.store.tableName+` WHERE type = ? AND key = ?`, table, key)
}

func (b *Batch) Merge(table string, key, val []byte) {
	b.merge.Merge(table, key, val)
}

func (b *Batch) Reset() {
	b.batch = b.store.Session.NewBatch(gocql.LoggedBatch)
	b.merge = NewEmulatedMerge(b.store.mo)
}

func (b *Batch) Close() error {
	b.batch = nil
	b.batch = nil
	b.merge = nil
	return nil
}

type EmulatedMerge struct {
	Merges map[string]map[string][][]byte // table, key, merge ops
	mo     store.MergeOperator
}

func NewEmulatedMerge(mo store.MergeOperator) *EmulatedMerge {
	return &EmulatedMerge{
		Merges: make(map[string]map[string][][]byte),
		mo:     mo,
	}
}

func (m *EmulatedMerge) Merge(table string, key, val []byte) {
	t, ok := m.Merges[string(table)]
	if !ok {
		t = make(map[string][][]byte)
		m.Merges[string(table)] = t
	}
	ops, ok := t[string(key)]
	if ok && len(ops) > 0 {
		last := ops[len(ops)-1]
		mergedVal, partialMergeOk := m.mo.PartialMerge(key, last, val)
		if partialMergeOk {
			// replace last entry with the result of the merge
			ops[len(ops)-1] = mergedVal
		} else {
			// could not partial merge, append this to the end
			ops = append(ops, val)
		}
	} else {
		ops = [][]byte{val}
	}
	t[string(key)] = ops
}

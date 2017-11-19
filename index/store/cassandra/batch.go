package cassandra

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/wrble/flock/index/store"
)

type Batch struct {
	store        *Store
	merge        *EmulatedMerge
	batch        *gocql.Batch
	counterBatch *gocql.Batch
}

const MAX_BATCH_STATEMENTS = 1 // try and estimate what's under 5K
const BATCH_TYPE = gocql.UnloggedBatch

func (b *Batch) Set(table string, key, val []byte) {
	b.batch.Query(`INSERT INTO `+b.store.tableName+` (type, key, value) VALUES (?, ?, ?)`, table, key, val)
	if b.store.debug {
		fmt.Println("INSERT TABLE:", b.store.tableName, table, string(key), string(val))
	}
	b.CheckExecute()
}

func (b *Batch) Delete(table string, key []byte) {
	b.batch.Query(`DELETE FROM `+b.store.tableName+` WHERE type = ? AND key = ?`, table, key)
	b.CheckExecute()
}

func (b *Batch) Increment(table string, key []byte, amount int64) {
	b.counterBatch.Query(`UPDATE d SET value = value + ? WHERE type = ? AND key = ?;`, amount, table, key)
	b.CheckExecute()
}

// Check if we need to execute the partial batch due to size
func (b *Batch) CheckExecute() error {
	//fmt.Println("BATCH SIZE:", b.batch.Size())
	if b.batch.Size() >= MAX_BATCH_STATEMENTS || b.counterBatch.Size() >= MAX_BATCH_STATEMENTS {
		return b.Execute()
	}
	return nil
}

func (b *Batch) Execute() error {
	err := b.store.Session.ExecuteBatch(b.batch)
	if err != nil {
		return err
	}
	err = b.store.Session.ExecuteBatch(b.counterBatch)
	if err != nil {
		return err
	}
	b.Reset()
	return nil
}

func (b *Batch) Reset() {
	b.batch = b.store.Session.NewBatch(BATCH_TYPE)
	b.counterBatch = b.store.Session.NewBatch(gocql.CounterBatch)
	b.merge = NewEmulatedMerge(b.store.mo)
}

func (b *Batch) Close() error {
	b.batch = nil
	b.batch = nil
	b.merge = nil
	return nil
}

type EmulatedMerge struct {
	Merges map[string]int // table, key, merge ops
	mo     store.MergeOperator
}

func NewEmulatedMerge(mo store.MergeOperator) *EmulatedMerge {
	return &EmulatedMerge{
		Merges: make(map[string]int),
		mo:     mo,
	}
}

func (m *EmulatedMerge) Merge(table string, key, val []byte) {
	_, ok := m.Merges[string(table)]
	if !ok {
		m.Merges[string(table)] = 0
	}
	m.Merges[string(table)] += 1
}

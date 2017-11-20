package cassandra

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/wrble/flock/index/store"
)

type Writer struct {
	store *Store
}

func (w *Writer) NewBatch() store.KVBatch {
	rv := Batch{
		store:        w.store,
		merge:        NewEmulatedMerge(w.store.mo),
		batch:        w.store.Session.NewBatch(BATCH_TYPE),
		counterBatch: w.store.Session.NewBatch(gocql.CounterBatch),
	}
	return &rv
}

func (w *Writer) NewBatchEx(options store.KVBatchOptions) (store.KVBatch, error) {
	return w.NewBatch(), nil
}

func (w *Writer) ExecuteBatch(b store.KVBatch) error {
	batch, ok := b.(*Batch)
	if !ok {
		return fmt.Errorf("wrong type of batch")
	}
	return batch.Execute()
}

func (w *Writer) Close() error {
	return nil
}

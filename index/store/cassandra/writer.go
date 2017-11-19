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
	//valLen := 0
	//reader := &Reader{store: w.store}
	//
	//// first process merges
	//for table, merge := range batch.merge.Merges {
	//	batch.Set(table, kb, mergedVal)
	//}
	//if w.store.debug {
	//	fmt.Println("Merges:", len(batch.merge.Merges), "Batch:", batch.batch, "Merge len: ", valLen)
	//}

	// now execute the batch
	err := w.store.Session.ExecuteBatch(batch.batch)
	if err != nil {
		return err
	}
	return w.store.Session.ExecuteBatch(batch.counterBatch)
}

func (w *Writer) Close() error {
	return nil
}

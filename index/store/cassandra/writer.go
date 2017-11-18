package cassandra

import (
	"fmt"

	"github.com/gocql/gocql"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/wrble/flock/index/store"
)

type Writer struct {
	store *Store
}

func (w *Writer) NewBatch() store.KVBatch {
	rv := Batch{
		store: w.store,
		merge: NewEmulatedMerge(w.store.mo),
		batch: w.store.Session.NewBatch(gocql.LoggedBatch),
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
	valLen := 0
	reader := &Reader{store: w.store}

	// first process merges
	for table, merge := range batch.merge.Merges {
		for k, mergeOps := range merge {
			kb := []byte(k)
			existingVal, err := reader.Get(table, kb)
			if err != nil && err != leveldb.ErrNotFound {
				return err
			}
			mergedVal, fullMergeOk := w.store.mo.FullMerge(kb, existingVal, mergeOps)
			if !fullMergeOk {
				return fmt.Errorf("merge operator returned failure")
			}
			// add the final merge to this batch
			batch.Set(table, kb, mergedVal)
			valLen += len(mergedVal)
		}
	}

	if w.store.debug {
		fmt.Println("Merges:", len(batch.merge.Merges), "Batch:", batch.batch, "Merge len: ", valLen)
	}

	// now execute the batch
	return w.store.Session.ExecuteBatch(batch.batch)
}

func (w *Writer) Close() error {
	return nil
}

package cassandra

import (
	"os"
	"testing"

	"strings"

	"github.com/wrble/flock/index/store"
	"github.com/wrble/flock/index/store/test"
)

func open(t *testing.T, mo store.MergeOperator) store.KVStore {
	tableName := "idx"
	rv, err := New(mo, map[string]interface{}{
		"keyspace": "example",
		"table":    tableName,
		"hosts":    []string{"127.0.0.1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	st := rv.(*Store)
	err = DropTables(st.Session, tableName)
	if err != nil && !strings.Contains(err.Error(), "unconfigured table") {
		t.Fatal(err)
	}
	err = CreateTables(st.Session, tableName)
	if err != nil {
		t.Fatal(err)
	}
	return rv
}

func cleanup(t *testing.T, s store.KVStore) {
	err := s.Close()
	if err != nil {
		t.Fatal(err)
	}
	err = os.RemoveAll("test")
	if err != nil {
		t.Fatal(err)
	}
}

func TestCassandraKVCrud(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestKVCrud(t, s)
}

//func TestCassandraReaderIsolation(t *testing.T) {
//	s := open(t, nil)
//	defer cleanup(t, s)
//	test.CommonTestReaderIsolation(t, s)
//}

func TestCassandraReaderOwnsGetBytes(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestReaderOwnsGetBytes(t, s)
}

//func TestCassandraWriterOwnsBytes(t *testing.T) {
//	s := open(t, nil)
//	defer cleanup(t, s)
//	test.CommonTestWriterOwnsBytes(t, s)
//}

func TestCassandraPrefixIterator(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestPrefixIterator(t, s)
}

func TestCassandraPrefixIteratorSeek(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestPrefixIteratorSeek(t, s)
}

func TestCassandraRangeIterator(t *testing.T) {
	s := open(t, nil)
	defer cleanup(t, s)
	test.CommonTestRangeIterator(t, s)
}

//func TestCassandraRangeIteratorSeek(t *testing.T) {
//	s := open(t, nil)
//	defer cleanup(t, s)
//	test.CommonTestRangeIteratorSeek(t, s)
//}

func TestCassandraMerge(t *testing.T) {
	s := open(t, &test.TestMergeCounter{})
	defer cleanup(t, s)
	test.CommonTestMerge(t, s)
}

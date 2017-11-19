package cassandra

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/wrble/flock/index/store"
	"github.com/wrble/flock/registry"
)

const (
	Name = "cassandra"
)

var Tables = []string{
	"b",
	"d",
	"i",
	"f",
	"s",
	"t",
	"v",
}

func init() {
	registry.RegisterKVStore(Name, New)
}

type Store struct {
	mo store.MergeOperator

	Session *gocql.Session

	cluster   *gocql.ClusterConfig
	tableName string

	debug bool
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	if err := validate("hosts", config); err != nil {
		return nil, err
	}
	if err := validate("keyspace", config); err != nil {
		return nil, err
	}
	if err := validate("table", config); err != nil {
		return nil, err
	}
	cluster := gocql.NewCluster(config["hosts"].([]string)...)
	cluster.Keyspace = config["keyspace"].(string)
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	st := Store{
		mo:        mo,
		cluster:   cluster,
		Session:   session,
		tableName: config["table"].(string),
		debug:     false,
	}
	return &st, nil
}

func CreateTables(session *gocql.Session, tableName string) error {
	err := session.Query(`CREATE TABLE d (type text, key blob, value counter, PRIMARY KEY(type, key))`).Exec()
	if err != nil {
		return err
	}
	return session.Query(`CREATE TABLE ` + tableName + ` (type text, key blob, value blob, PRIMARY KEY(type, key))`).Exec()

	//for _, tableName := range Tables {
	//	err := session.Query(`CREATE TABLE ` + tableName + ` (key blob, value blob, PRIMARY KEY(key, value))`).Exec()
	//	if err != nil {
	//		return err
	//	}
	//}
	//return nil
}

func DropTables(session *gocql.Session, tableName string) error {
	err := session.Query(`DROP TABLE d`).Exec()
	if err != nil && strings.Contains(err.Error(), "unconfigured table") {
		return nil
	}
	err = session.Query(`DROP TABLE ` + tableName).Exec()
	if err != nil && strings.Contains(err.Error(), "unconfigured table") {
		return nil
	}
	return err

	//for _, tableName := range Tables {
	//	err := session.Query(`DROP TABLE ` + tableName).Exec()
	//	if err != nil && !strings.Contains(err.Error(), "unconfigured table") {
	//		return err
	//	}
	//	fmt.Println("DROPPED", tableName)
	//}
	//return nil
}

func validate(key string, config map[string]interface{}) error {
	value := config[key]
	if value == nil {
		return fmt.Errorf("must specify: " + key)
	}
	return nil
}

func (cdbs *Store) Close() error {
	cdbs.Session.Close()
	return nil
}

func (cdbs *Store) Reader() (store.KVReader, error) {
	return &Reader{
		store: cdbs,
	}, nil
}

func (cdbs *Store) Writer() (store.KVWriter, error) {
	return &Writer{
		store: cdbs,
	}, nil
}

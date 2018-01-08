package cassandra

import (
	"fmt"
	"strings"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"
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

const SharedTable = "idx"

func init() {
	registry.RegisterKVStore(Name, New)
}

type Store struct {
	mo store.MergeOperator

	Session *gocql.Session

	cluster *gocql.ClusterConfig

	debug bool
}

func New(mo store.MergeOperator, config map[string]interface{}) (store.KVStore, error) {
	if err := validate("hosts", config); err != nil {
		return nil, err
	}
	if err := validate("keyspace", config); err != nil {
		return nil, err
	}
	hosts, ok := config["hosts"].([]string)
	if !ok {
		interfaceHosts, ok := config["hosts"].([]interface{})
		if ok {
			hosts = []string{}
			for _, h := range interfaceHosts {
				hosts = append(hosts, h.(string))
			}
		} else {
			return nil, errors.New("Must specify 'hosts' param as []string")
		}
	}
	cluster := gocql.NewCluster(hosts...)
	cluster.Keyspace = config["keyspace"].(string)
	cluster.Consistency = gocql.Quorum
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	st := Store{
		mo:      mo,
		cluster: cluster,
		Session: session,
		debug:   false,
	}
	return &st, nil
}

func CreateTables(session *gocql.Session) error {
	var err error
	err = session.Query(`CREATE TABLE d (type text, key blob, value counter, PRIMARY KEY(type, key))`).Exec()
	if err != nil {
		return err
	}
	err = session.Query(`CREATE TABLE t (type text, key blob, freq float, score float, vectors blob, PRIMARY KEY((type), key));`).Exec()
	//err = session.Query(`CREATE TABLE t (type text, key blob, freq float, score float, vectors blob, PRIMARY KEY((type), score, key)) WITH CLUSTERING ORDER BY (score DESC);`).Exec()
	if err != nil {
		return err
	}
	//err = session.Query(`CREATE INDEX t_score ON t (score);`).Exec()
	//if err != nil {
	//	return err
	//}
	return WrapError(session.Query(`CREATE TABLE `+SharedTable+` (type text, key blob, value blob, PRIMARY KEY(type, key))`).Exec(), "Create Tables")

	//for _, tableName := range Tables {
	//	err := session.Query(`CREATE TABLE ` + tableName + ` (key blob, value blob, PRIMARY KEY(key, value))`).Exec()
	//	if err != nil {
	//		return err
	//	}
	//}
	//return nil
}

func DropTables(session *gocql.Session) error {
	var err error
	err = session.Query(`DROP TABLE d`).Exec()
	if err != nil && !strings.Contains(err.Error(), "unconfigured table") {
		return WrapError(err, "drop table d")
	}
	err = session.Query(`DROP TABLE t`).Exec()
	if err != nil && !strings.Contains(err.Error(), "unconfigured table") {
		return WrapError(err, "drop table t")
	}
	err = session.Query(`DROP TABLE ` + SharedTable).Exec()
	if err != nil && !strings.Contains(err.Error(), "unconfigured table") {
		return WrapError(err, "drop table "+SharedTable)
	}
	return nil

	//for _, tableName := range Tables {
	//	err := session.Query(`DROP TABLE ` + tableName).Exec()
	//	if err != nil && !strings.Contains(err.Error(), "unconfigured table") {
	//		return err
	//	}
	//	fmt.Println("DROPPED", tableName)
	//}
	//return nil
}

func TableMapping(t string) string {
	if t == "d" || t == "t" {
		return t
	}
	return SharedTable
}

func WrapError(err error, context string) error {
	if err != nil {
		return errors.New(context + ": " + err.Error())
	}
	return nil
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

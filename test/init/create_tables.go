package main

import (
	"log"

	"fmt"

	"github.com/wrble/flock"
	"github.com/wrble/flock/index/store/cassandra"
)

func main() {
	store, err := cassandra.New(nil, flock.Config.DefaultKVConfig)
	if err != nil {
		log.Fatal(err)
	}

	err = cassandra.CreateTables(store.(*cassandra.Store).Session, flock.Config.DefaultKVConfig["table"].(string))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Tables created!")
}

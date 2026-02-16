package metadata

import (
	"log"

	"github.com/dgraph-io/badger/v4"
)

var DB *badger.DB

func InitDB(path string) {
	opts := badger.DefaultOptions(path)

	db, err := badger.Open(opts)
	if err != nil {
		log.Fatalln(err)
	}

	DB = db
}

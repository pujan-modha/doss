package metadata

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/dgraph-io/badger/v4"
)

type BucketMeta struct {
	Name      string
	OwnerID   string
	CreatedAt time.Time
}

func CreateBucket(name string) error {
	key := []byte("bucket/" + name)

	return DB.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == nil {
			return ErrBucketAlreadyExists
		}
		bucket := BucketMeta{
			Name:      name,
			OwnerID:   "local-dev-user",
			CreatedAt: time.Now(),
		}
		data, err := json.Marshal(bucket)
		if err != nil {
			return err
		}
		return txn.Set(key, data)
	})
}

func GetBucket(name string) (*BucketMeta, error) {
	key := []byte("bucket/" + name)

	var bucket BucketMeta

	err := DB.View(
		func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrBucketNotFound
			}
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				return json.Unmarshal(val, &bucket)
			})
		})
	if err != nil {
		return nil, err
	}
	return &bucket, nil
}

func ListBuckets() ([]string, error) {
	var res []string
	err := DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte("bucket/")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			res = append(res, string(k[len(prefix):]))
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func DeleteBucket(name string) error {
	key := []byte("bucket/" + name)

	err := DB.Update(
		func(txn *badger.Txn) error {
			_, err := txn.Get(key)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrBucketNotFound
			}
			if err != nil {
				return err
			}
			return txn.Delete(key)
		})
	if err != nil {
		return err
	}
	return nil
}

func HeadBucket(name string) error {
	key := []byte("bucket/" + name)

	return DB.View(
		func(txn *badger.Txn) error {
			_, err := txn.Get(key)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrBucketNotFound
			}
			return err
		},
	)
}

type Location struct {
	Location string `json:"location"`
}

func GetBucketLocation(name string) (*Location, error) {
	key := []byte("bucket/" + name)

	err := DB.View(
		func(txn *badger.Txn) error {
			_, err := txn.Get(key)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrBucketNotFound
			}
			return err
		},
	)

	if err != nil {
		return nil, err
	}

	return &Location{
		Location: "local",
	}, nil
}

type BucketCORS struct {
	AllowedOrigins []string `json:"allowed_origins"`
	AllowedMethods []string `json:"allowed_methods"`
	AllowedHeaders []string `json:"allowed_headers"`
	ExposeHeaders  []string `json:"expose_headers"`
}

func GetBucketCORS(name string) (*BucketCORS, error) {
	if err := HeadBucket(name); err != nil {
		return nil, err
	}

	key := []byte("bucket/" + name + "/cors")

	var cors BucketCORS

	err := DB.View(
		func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				return json.Unmarshal(val, &cors)
			})
		})
	if err != nil {
		return nil, err
	}
	return &cors, nil
}

func PutBucketCORS(name string, bucketCORS *BucketCORS) error {
	if err := HeadBucket(name); err != nil {
		return err
	}

	key := []byte("bucket/" + name + "/cors")

	return DB.Update(func(txn *badger.Txn) error {
		data, err := json.Marshal(bucketCORS)
		if err != nil {
			return err
		}
		return txn.Set(key, data)
	})
}

func DeleteBucketCORS(name string) error {
	if err := HeadBucket(name); err != nil {
		return err
	}

	key := []byte("bucket/" + name + "/cors")

	err := DB.Update(
		func(txn *badger.Txn) error {
			err := txn.Delete(key)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return nil
			}
			return err
		})
	if err != nil {
		return err
	}
	return nil
}

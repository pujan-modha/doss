package metadata

import (
	"bytes"
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

func CreateBucket(ownerID string, name string) error {
	key := []byte("bucket/" + name)

	return DB.Update(func(txn *badger.Txn) error {
		_, err := txn.Get(key)
		if err == nil {
			return ErrBucketAlreadyExists
		}
		if errors.Is(err, badger.ErrKeyNotFound) {
			bucket := BucketMeta{
				Name:      name,
				OwnerID:   ownerID,
				CreatedAt: time.Now(),
			}
			data, err := json.Marshal(bucket)
			if err != nil {
				return err
			}
			return txn.Set(key, data)
		}
		return err
	})
}

func GetBucketMetadata(ownerID string, name string) (*BucketMeta, error) {
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
	if bucket.OwnerID != ownerID {
		return nil, ErrNoAccess
	}
	return &bucket, nil
}

func ListBuckets(ownerID string) ([]string, error) {
	var res []string

	err := DB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte("bucket/")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			suffix := k[len(prefix):]
			if bytes.IndexByte(suffix, '/') != -1 {
				continue // skip bucket sub-resources like cors/notification
			}
			var meta BucketMeta
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &meta)
			}); err != nil {
				return err
			}

			if meta.OwnerID != ownerID {
				continue
			}

			res = append(res, string(suffix))
		}
		return nil
	})

	if err != nil {
		return nil, err
	}
	if res == nil {
		res = []string{}
	}
	return res, nil
}

func DeleteBucket(ownerID string, name string) error {
	key := []byte("bucket/" + name)
	subresourcePrefix := []byte("bucket/" + name + "/")

	err := DB.Update(
		func(txn *badger.Txn) error {
			bucket, err := txn.Get(key)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrBucketNotFound
			}
			if err != nil {
				return err
			}
			if err := bucket.Value(func(val []byte) error {
				var data BucketMeta
				if err := json.Unmarshal(val, &data); err != nil {
					return err
				}
				if data.OwnerID != ownerID {
					return ErrNoAccess
				}
				if err := txn.Delete(key); err != nil {
					return err
				}

				it := txn.NewIterator(badger.DefaultIteratorOptions)
				defer it.Close()

				for it.Seek(subresourcePrefix); it.ValidForPrefix(subresourcePrefix); it.Next() {
					k := append([]byte{}, it.Item().Key()...)
					if err := txn.Delete(k); err != nil {
						return err
					}
				}

				return nil
			}); err != nil {
				return err
			}
			return err
		})
	if err != nil {
		return err
	}
	return nil
}

func HeadBucket(ownerID string, name string) error {
	key := []byte("bucket/" + name)

	return DB.View(
		func(txn *badger.Txn) error {
			bucket, err := txn.Get(key)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrBucketNotFound
			}
			if err != nil {
				return err
			}
			if err := bucket.Value(func(val []byte) error {
				var data BucketMeta
				if err := json.Unmarshal(val, &data); err != nil {
					return err
				}
				if data.OwnerID != ownerID {
					return ErrNoAccess
				}
				return nil
			}); err != nil {
				return err
			}
			return nil
		},
	)
}

type Location struct {
	Location string `json:"location"`
}

func GetBucketLocation(ownerID string, name string) (*Location, error) {
	key := []byte("bucket/" + name)

	err := DB.View(
		func(txn *badger.Txn) error {
			bucket, err := txn.Get(key)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrBucketNotFound
			}
			if err != nil {
				return err
			}
			if err := bucket.Value(func(val []byte) error {
				var data BucketMeta
				if err := json.Unmarshal(val, &data); err != nil {
					return err
				}
				if data.OwnerID != ownerID {
					return ErrNoAccess
				}
				return nil
			}); err != nil {
				return err
			}
			return nil
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

func GetBucketCORS(ownerID string, name string) (*BucketCORS, error) {
	if err := HeadBucket(ownerID, name); err != nil {
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

func PutBucketCORS(ownerID string, name string, bucketCORS *BucketCORS) error {
	if err := HeadBucket(ownerID, name); err != nil {
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

func DeleteBucketCORS(ownerID string, name string) error {
	if err := HeadBucket(ownerID, name); err != nil {
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

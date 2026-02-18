package metadata

import (
	"bytes"
	"encoding/json"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

type BucketNotificationConfig struct {
	Rules []NotificationRule `json:"rules"`
}

type NotificationRule struct {
	ID       string   `json:"id,omitempty"`
	TargetID string   `json:"target_id"` // reference: user-owned target
	Events   []string `json:"events"`    // e.g., s3:ObjectCreated:Put, s3:ObjectRemoved:Delete
	Prefix   string   `json:"prefix,omitempty"`
	Suffix   string   `json:"suffix,omitempty"`
}

const (
	PutObjectEvent    = "s3:ObjectCreated:Put"
	DeleteObjectEvent = "s3:ObjectRemoved:Delete"
)

func GetBucketNotification(ownerID string, name string) (*BucketNotificationConfig, error) {
	if err := HeadBucket(ownerID, name); err != nil {
		return nil, err
	}

	key := []byte("bucket/" + name + "/notification")

	var cfg BucketNotificationConfig

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
				return json.Unmarshal(val, &cfg)
			})
		})
	if err != nil {
		return nil, err
	}

	if cfg.Rules == nil {
		cfg.Rules = []NotificationRule{}
	}
	return &cfg, nil
}

func PutBucketNotification(ownerID string, name string, cfg *BucketNotificationConfig) error {
	if err := HeadBucket(ownerID, name); err != nil {
		return err
	}

	if cfg == nil {
		return ErrInvalidNotificationConfig
	}
	seen := map[string]struct{}{}
	for _, rule := range cfg.Rules {
		if rule.TargetID == "" || len(rule.Events) == 0 {
			return ErrInvalidNotificationConfig
		}
		if _, ok := seen[rule.TargetID]; !ok {
			exists, err := NotificationTargetExists(ownerID, rule.TargetID)
			if err != nil {
				return err
			}
			if !exists {
				return ErrInvalidNotificationConfig
			}
			seen[rule.TargetID] = struct{}{}
		}
		for _, event := range rule.Events {
			if event != PutObjectEvent && event != DeleteObjectEvent {
				return ErrInvalidNotificationConfig
			}
		}
	}

	key := []byte("bucket/" + name + "/notification")

	return DB.Update(func(txn *badger.Txn) error {
		data, err := json.Marshal(cfg)
		if err != nil {
			return err
		}
		return txn.Set(key, data)
	})
}

func IsNotificationTargetInUse(ownerID, targetID string) (bool, error) {
	inUse := false

	err := DB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		prefix := []byte("bucket/")
		suffix := []byte("/notification")

		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()

			// Only bucket notification config keys: bucket/<name>/notification
			if !bytes.HasSuffix(k, suffix) {
				continue
			}

			bucketName := string(k[len(prefix) : len(k)-len(suffix)])
			if bucketName == "" {
				continue
			}

			// Owner check from a bucket metadata key
			bucketKey := []byte("bucket/" + bucketName)
			bucketItem, err := txn.Get(bucketKey)
			if errors.Is(err, badger.ErrKeyNotFound) {
				continue // orphan subresource, ignore
			}
			if err != nil {
				return err
			}

			var meta BucketMeta
			if err := bucketItem.Value(func(val []byte) error {
				return json.Unmarshal(val, &meta)
			}); err != nil {
				return err
			}

			if meta.OwnerID != ownerID {
				continue
			}

			var cfg BucketNotificationConfig
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &cfg)
			}); err != nil {
				return err
			}

			for _, rule := range cfg.Rules {
				if rule.TargetID == targetID {
					inUse = true
					return nil
				}
			}
		}

		return nil
	})

	return inUse, err
}

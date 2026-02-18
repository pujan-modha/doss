package metadata

import (
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
		return nil, ErrBucketNotFound
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
		return ErrBucketNotFound
	}

	if cfg == nil {
		return ErrInvalidNotificationConfig
	}

	for _, rule := range cfg.Rules {
		if rule.TargetID == "" {
			return ErrInvalidNotificationConfig
		}
		if len(rule.Events) == 0 {
			return ErrInvalidNotificationConfig
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

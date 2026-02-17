package metadata

import (
	"encoding/json"
	"errors"

	"github.com/dgraph-io/badger/v4"
)

type NotificationTarget struct {
	ID         string `json:"id"`   // unique per user
	OwnerID    string `json:"-"`    // auth subject
	Type       string `json:"type"` // "rabbitmq"
	URL        string `json:"url"`  // amqp://...
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key,omitempty"`
	Durable    bool   `json:"durable"`
	Enabled    bool   `json:"enabled"`
}

func PutNotificationTarget(ownerID string, t *NotificationTarget) error {
	if t == nil || t.ID == "" || t.URL == "" || t.Exchange == "" || t.Type != "rabbitmq" {
		return ErrInvalidNotificationTargetConfig
	}

	if t.OwnerID != ownerID {
		return ErrNoAccess
	}

	key := []byte("target/" + ownerID + "/" + t.ID)

	return DB.Update(func(txn *badger.Txn) error {
		data, err := json.Marshal(t)
		if err != nil {
			return err
		}
		return txn.Set(key, data)
	})
}

func GetNotificationTarget(ownerID, targetID string) (*NotificationTarget, error) {
	key := []byte("target/" + ownerID + "/" + targetID)

	var t NotificationTarget

	err := DB.View(
		func(txn *badger.Txn) error {
			item, err := txn.Get(key)
			if errors.Is(err, badger.ErrKeyNotFound) {
				return ErrNotificationTargetNotFound
			}
			if err != nil {
				return err
			}
			return item.Value(func(val []byte) error {
				return json.Unmarshal(val, &t)
			})
		})
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func DeleteNotificationTarget(ownerID, targetID string) error {
	key := []byte("target/" + ownerID + "/" + targetID)

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
func ListNotificationTargets(ownerID string) ([]NotificationTarget, error) {
	var res []NotificationTarget
	err := DB.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		it := txn.NewIterator(opts)
		defer it.Close()

		prefix := []byte("target/" + ownerID + "/")
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			var t NotificationTarget
			item := it.Item()
			if err := item.Value(func(val []byte) error {
				return json.Unmarshal(val, &t)
			}); err != nil {
				return err
			}
			res = append(res, t)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if res == nil {
		res = []NotificationTarget{}
	}
	return res, nil
}

package metadata

import "errors"

var (
	ErrBucketNotFound            = errors.New("bucket not found")
	ErrBucketAlreadyExists       = errors.New("bucket already exists")
	ErrInvalidNotificationConfig = errors.New("invalid notification config")
)

package metadata

import "errors"

var (
	ErrBucketNotFound                  = errors.New("bucket not found")
	ErrBucketAlreadyExists             = errors.New("bucket already exists")
	ErrInvalidNotificationConfig       = errors.New("invalid notification config")
	ErrInvalidNotificationTargetConfig = errors.New("invalid notification target config")
	ErrNoAccess                        = errors.New("no access")
	ErrNotificationTargetNotFound      = errors.New("notification target not found")
	ErrNotificationTargetInUse         = errors.New("notification target in use")
)

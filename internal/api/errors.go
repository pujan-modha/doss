package api

import "errors"

var (
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrInternal            = errors.New("internal error")
	ErrBucketNameRequired  = errors.New("bucket name required")
	ErrBucketAlreadyExists = errors.New("bucket already exists")
	ErrBadRequest          = errors.New("bad request")
	ErrTargetIDRequired    = errors.New("target_id required")
	ErrTargetNotFound      = errors.New("target not found")
	ErrForbidden           = errors.New("forbidden")
	ErrUnauthorized        = errors.New("unauthorized")
)

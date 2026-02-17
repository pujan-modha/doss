package api

import (
	"doss/internal/metadata"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func BucketPutHandler(w http.ResponseWriter, r *http.Request) {
	bucketName, ok := parseBucketName(w, r)
	if !ok {
		return
	}

	if r.URL.Query().Has("cors") {
		handlePutBucketCORS(w, r, bucketName)
		return
	}

	if r.URL.Query().Has("notification") {
		handlePutBucketNotification(w, r, bucketName)
		return
	}

	if err := metadata.CreateBucket(bucketName); err != nil {
		log.Printf("CreateBucket error: %v", err)
		writeError(w, http.StatusConflict, ErrBucketAlreadyExists)
		return
	}

	w.WriteHeader(http.StatusOK)
}

// BucketGetHandler
// TODO: Change to ListObjects/ListObjectsV2
func BucketGetHandler(w http.ResponseWriter, r *http.Request) {
	bucketName, ok := parseBucketName(w, r)
	if !ok {
		return
	}

	if r.URL.Query().Has("location") {
		handleGetBucketLocation(w, bucketName)
		return
	}

	if r.URL.Query().Has("cors") {
		handleGetBucketCORS(w, bucketName)
		return
	}

	if r.URL.Query().Has("notification") {
		handleGetBucketNotification(w, bucketName)
		return
	}

	bucket, err := metadata.GetBucket(bucketName)
	if err != nil {
		log.Printf("GetBucket error: %v", err)
		writeError(w, http.StatusNotFound, ErrBucketNotFound)
		return
	}

	writeJSON(w, http.StatusOK, bucket)
}

func BucketListHandler(w http.ResponseWriter, _ *http.Request) {
	list, err := metadata.ListBuckets()
	if err != nil {
		log.Printf("ListBuckets error: %v", err)
		writeError(w, http.StatusInternalServerError, ErrInternal)
		return
	}

	writeJSON(w, http.StatusOK, list)
}

func BucketDeleteHandler(w http.ResponseWriter, r *http.Request) {
	bucketName, ok := parseBucketName(w, r)
	if !ok {
		return
	}

	if r.URL.Query().Has("cors") {
		handleDeleteBucketCORS(w, bucketName)
		return
	}

	if err := metadata.DeleteBucket(bucketName); err != nil {
		log.Printf("DeleteBucket error: %v", err)
		writeError(w, http.StatusNotFound, ErrBucketNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func BucketHeadHandler(w http.ResponseWriter, r *http.Request) {
	bucketName := chi.URLParam(r, "bucket")
	if bucketName == "" {
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	if err := metadata.HeadBucket(bucketName); err != nil {
		log.Printf("HeadBucket error: %v", err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

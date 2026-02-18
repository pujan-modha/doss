package api

import (
	"doss/internal/metadata"
	"log"
	"net/http"
)

func BucketPutHandler(w http.ResponseWriter, r *http.Request) {
	bucketName, ok := parseBucketName(w, r)
	if !ok {
		return
	}

	ownerID := getOwnerID(r)
	if ownerID == "" {
		writeError(w, http.StatusUnauthorized, ErrUnauthorized)
		return
	}

	if r.URL.Query().Has("cors") {
		handlePutBucketCORS(w, r, ownerID, bucketName)
		return
	}

	if r.URL.Query().Has("notification") {
		handlePutBucketNotification(w, r, ownerID, bucketName)
		return
	}

	if err := metadata.CreateBucket(ownerID, bucketName); err != nil {
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

	ownerID := getOwnerID(r)
	if ownerID == "" {
		writeError(w, http.StatusUnauthorized, ErrUnauthorized)
		return
	}

	if r.URL.Query().Has("location") {
		handleGetBucketLocation(w, ownerID, bucketName)
		return
	}

	if r.URL.Query().Has("cors") {
		handleGetBucketCORS(w, ownerID, bucketName)
		return
	}

	if r.URL.Query().Has("notification") {
		handleGetBucketNotification(w, ownerID, bucketName)
		return
	}

	bucket, err := metadata.GetBucket(ownerID, bucketName)
	if err != nil {
		log.Printf("GetBucket error: %v", err)
		writeError(w, http.StatusNotFound, ErrBucketNotFound)
		return
	}

	writeJSON(w, http.StatusOK, bucket)
}

func BucketListHandler(w http.ResponseWriter, r *http.Request) {
	ownerID := getOwnerID(r)
	if ownerID == "" {
		writeError(w, http.StatusUnauthorized, ErrUnauthorized)
		return
	}
	list, err := metadata.ListBuckets(ownerID)
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

	ownerID := getOwnerID(r)
	if ownerID == "" {
		writeError(w, http.StatusUnauthorized, ErrUnauthorized)
		return
	}

	if r.URL.Query().Has("cors") {
		handleDeleteBucketCORS(w, ownerID, bucketName)
		return
	}

	if err := metadata.DeleteBucket(ownerID, bucketName); err != nil {
		log.Printf("DeleteBucket error: %v", err)
		writeError(w, http.StatusNotFound, ErrBucketNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func BucketHeadHandler(w http.ResponseWriter, r *http.Request) {
	bucketName, ok := parseBucketName(w, r)
	if !ok {
		return
	}

	ownerID := getOwnerID(r)
	if ownerID == "" {
		writeError(w, http.StatusUnauthorized, ErrUnauthorized)
		return
	}

	if err := metadata.HeadBucket(ownerID, bucketName); err != nil {
		log.Printf("HeadBucket error: %v", err)
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
}

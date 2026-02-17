package api

import (
	"doss/internal/auth"
	"doss/internal/metadata"
	"encoding/json"
	"errors"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
)

func handleGetBucketLocation(w http.ResponseWriter, bucketName string) {
	loc, err := metadata.GetBucketLocation(bucketName)
	if err != nil {
		log.Printf("GetBucketLocation error: %v", err)
		writeError(w, http.StatusNotFound, ErrBucketNotFound)
		return
	}

	writeJSON(w, http.StatusOK, loc)
}

func handleGetBucketCORS(w http.ResponseWriter, bucketName string) {
	cors, err := metadata.GetBucketCORS(bucketName)
	if err != nil {
		log.Printf("GetBucketCORS error: %v", err)
		writeError(w, http.StatusNotFound, ErrBucketNotFound)
		return
	}

	writeJSON(w, http.StatusOK, cors)
}

func handlePutBucketCORS(w http.ResponseWriter, r *http.Request, bucketName string) {
	var cors metadata.BucketCORS
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	if err := decoder.Decode(&cors); err != nil {
		log.Printf("handlePutBucketCORS Decode error: %v", err)
		writeError(w, http.StatusBadRequest, ErrBadRequest)
		return
	}
	err := metadata.PutBucketCORS(bucketName, &cors)
	if errors.Is(err, metadata.ErrBucketNotFound) {
		log.Printf("PutBucketCORS error: %v", err)
		writeError(w, http.StatusNotFound, ErrBucketNotFound)
		return
	}
	if err != nil {
		log.Printf("PutBucketCORS error: %v", err)
		writeError(w, http.StatusInternalServerError, ErrInternal)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func handleDeleteBucketCORS(w http.ResponseWriter, bucketName string) {
	if err := metadata.DeleteBucketCORS(bucketName); err != nil {
		log.Printf("DeleteBucketCORS error: %v", err)
		writeError(w, http.StatusNotFound, ErrBucketNotFound)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func handleGetBucketNotification(w http.ResponseWriter, bucketName string) {
	cfg, err := metadata.GetBucketNotification(bucketName)
	if errors.Is(err, metadata.ErrBucketNotFound) {
		log.Printf("GetBucketNotification error: %v", err)
		writeError(w, http.StatusNotFound, ErrBucketNotFound)
		return
	}
	if err != nil {
		log.Printf("GetBucketNotification error: %v", err)
		writeError(w, http.StatusInternalServerError, ErrInternal)
		return
	}

	writeJSON(w, http.StatusOK, cfg)
}

func handlePutBucketNotification(w http.ResponseWriter, r *http.Request, bucketName string) {
	var cfg metadata.BucketNotificationConfig
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	if err := decoder.Decode(&cfg); err != nil {
		log.Printf("handlePutBucketNotification error: %v", err)
		writeError(w, http.StatusBadRequest, ErrBadRequest)
		return
	}
	err := metadata.PutBucketNotification(bucketName, &cfg)
	if errors.Is(err, metadata.ErrInvalidNotificationConfig) {
		log.Printf("PutBucketNotification error: %v", err)
		writeError(w, http.StatusBadRequest, ErrBadRequest)
		return
	}
	if errors.Is(err, metadata.ErrBucketNotFound) {
		log.Printf("PutBucketNotification error: %v", err)
		writeError(w, http.StatusNotFound, ErrBucketNotFound)
		return
	}
	if err != nil {
		log.Printf("PutBucketNotification error: %v", err)
		writeError(w, http.StatusInternalServerError, ErrInternal)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func parseBucketName(w http.ResponseWriter, r *http.Request) (string, bool) {
	b := chi.URLParam(r, "bucket")
	if b == "" {
		writeError(w, http.StatusBadRequest, ErrBucketNameRequired)
		return "", false
	}
	return b, true
}

func parseTargetID(w http.ResponseWriter, r *http.Request) (string, bool) {
	b := chi.URLParam(r, "targetID")
	if b == "" {
		writeError(w, http.StatusBadRequest, ErrTargetIDRequired)
		return "", false
	}
	return b, true
}

func getOwnerID(r *http.Request) string {
	ownerID, ok := auth.OwnerIDFromContext(r.Context())
	if !ok {
		return ""
	}
	return ownerID
}

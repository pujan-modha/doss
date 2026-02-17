package api

import (
	"doss/internal/metadata"
	"encoding/json"
	"errors"
	"log"
	"net/http"
)

type putTargetRequest struct {
	ID         string `json:"id"`
	Type       string `json:"type"`
	URL        string `json:"url"`
	Exchange   string `json:"exchange"`
	RoutingKey string `json:"routing_key,omitempty"`
	Durable    bool   `json:"durable"`
	Enabled    bool   `json:"enabled"`
}

func TargetItemGetHandler(w http.ResponseWriter, r *http.Request) {
	targetID, ok := parseTargetID(w, r)
	if !ok {
		return
	}

	ownerID := getOwnerID(r)
	if ownerID == "" {
		writeError(w, http.StatusUnauthorized, ErrUnauthorized)
		return
	}

	target, err := metadata.GetNotificationTarget(ownerID, targetID)
	if errors.Is(err, metadata.ErrNotificationTargetNotFound) {
		log.Printf("GetNotificationTarget error: %v", err)
		writeError(w, http.StatusNotFound, ErrTargetNotFound)
		return
	}
	if err != nil {
		log.Printf("GetNotificationTarget error: %v", err)
		writeError(w, http.StatusInternalServerError, ErrInternal)
		return
	}

	writeJSON(w, http.StatusOK, target)
}

func TargetItemPutHandler(w http.ResponseWriter, r *http.Request) {
	targetID, ok := parseTargetID(w, r)
	if !ok {
		return
	}

	ownerID := getOwnerID(r)
	if ownerID == "" {
		writeError(w, http.StatusUnauthorized, ErrUnauthorized)
		return
	}

	var req putTargetRequest
	decoder := json.NewDecoder(r.Body)
	defer r.Body.Close()
	if err := decoder.Decode(&req); err != nil {
		log.Printf("TargetItemPutHandler error: %v", err)
		writeError(w, http.StatusBadRequest, ErrBadRequest)
		return
	}

	if req.ID != "" && req.ID != targetID {
		writeError(w, http.StatusBadRequest, ErrBadRequest)
		return
	}

	target := metadata.NotificationTarget{
		ID:         targetID, // force from a path
		OwnerID:    ownerID,  // force from auth context
		Type:       req.Type,
		URL:        req.URL,
		Exchange:   req.Exchange,
		RoutingKey: req.RoutingKey,
		Durable:    req.Durable,
		Enabled:    req.Enabled,
	}

	err := metadata.PutNotificationTarget(ownerID, &target)

	if errors.Is(err, metadata.ErrInvalidNotificationTargetConfig) {
		log.Printf("PutNotificationTarget error: %v", err)
		writeError(w, http.StatusBadRequest, ErrBadRequest)
		return
	}
	if errors.Is(err, metadata.ErrNoAccess) {
		log.Printf("PutNotificationTarget error: %v", err)
		writeError(w, http.StatusForbidden, ErrForbidden)
		return
	}
	if err != nil {
		log.Printf("PutNotificationTarget error: %v", err)
		writeError(w, http.StatusInternalServerError, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func TargetItemDeleteHandler(w http.ResponseWriter, r *http.Request) {
	targetID, ok := parseTargetID(w, r)
	if !ok {
		return
	}

	ownerID := getOwnerID(r)
	if ownerID == "" {
		writeError(w, http.StatusUnauthorized, ErrUnauthorized)
		return
	}

	if err := metadata.DeleteNotificationTarget(ownerID, targetID); err != nil {
		log.Printf("DeleteNotificationTarget error: %v", err)
		writeError(w, http.StatusInternalServerError, ErrInternal)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

func TargetCollectionGetHandler(w http.ResponseWriter, r *http.Request) {
	ownerID := getOwnerID(r)
	if ownerID == "" {
		writeError(w, http.StatusUnauthorized, ErrUnauthorized)
		return
	}

	targets, err := metadata.ListNotificationTargets(ownerID)
	if err != nil {
		log.Printf("ListNotificationTargets error: %v", err)
		writeError(w, http.StatusInternalServerError, ErrInternal)
		return
	}

	writeJSON(w, http.StatusOK, targets)
}

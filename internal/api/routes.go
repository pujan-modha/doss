package api

import (
	"doss/internal/auth"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-chi/cors"
)

func RegisterRoutes() http.Handler {
	r := chi.NewRouter()
	r.Use(middleware.Logger)
	r.Use(cors.Handler(cors.Options{
		AllowedOrigins:   []string{"https://*", "http://*"},
		AllowedMethods:   []string{"GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"},
		AllowedHeaders:   []string{"Accept", "Authorization", "Content-Type"},
		AllowCredentials: true,
		MaxAge:           300,
	}))

	r.Get("/doss/v1/health", HealthHandler)

	r.Group(func(r chi.Router) {
		r.Use(auth.Middleware)

		r.Get("/", BucketListHandler)
		r.Put("/{bucket}", BucketPutHandler)
		r.Get("/{bucket}", BucketGetHandler) // TODO: Change to ListObjects/ListObjectsV2
		r.Delete("/{bucket}", BucketDeleteHandler)
		r.Head("/{bucket}", BucketHeadHandler)

		r.Get("/doss/v1/targets", TargetCollectionGetHandler)
		r.Get("/doss/v1/targets/{targetID}", TargetItemGetHandler)
		r.Put("/doss/v1/targets/{targetID}", TargetItemPutHandler)
		r.Delete("/doss/v1/targets/{targetID}", TargetItemDeleteHandler)

	})

	return r
}

func HealthHandler(w http.ResponseWriter, _ *http.Request) {
	resp := map[string]string{
		"message": "Healthy",
	}

	writeJSON(w, http.StatusOK, resp)
}

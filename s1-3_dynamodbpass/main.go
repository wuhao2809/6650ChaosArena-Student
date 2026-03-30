package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	table := os.Getenv("DYNAMODB_TABLE")
	if table == "" {
		table = "albums"
	}

	region := os.Getenv("AWS_REGION")
	if region == "" {
		region = "us-west-2"
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		log.Fatalf("failed to load AWS config: %v", err)
	}
	client := dynamodb.NewFromConfig(cfg)

	mux := http.NewServeMux()

	// S1: health check
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	})

	// S2/S3: album endpoints
	mux.HandleFunc("/albums/", func(w http.ResponseWriter, r *http.Request) {
		// Parse albumID from path: /albums/{albumID}
		parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/"), "/")
		if len(parts) < 2 || parts[1] == "" {
			http.Error(w, `{"error":"missing album_id"}`, http.StatusBadRequest)
			return
		}
		albumID := parts[1]

		switch r.Method {
		case http.MethodPut:
			handlePutAlbum(w, r, client, table, albumID)
		case http.MethodGet:
			handleGetAlbum(w, r, client, table, albumID)
		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
	})

	log.Printf("listening on :%s (table=%s region=%s)", port, table, region)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func handlePutAlbum(w http.ResponseWriter, r *http.Request, client *dynamodb.Client, table, albumID string) {
	var album Album
	if err := json.NewDecoder(r.Body).Decode(&album); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}
	// URL path is authoritative for the album_id
	album.AlbumID = albumID

	if err := putAlbum(r.Context(), client, table, album); err != nil {
		log.Printf("putAlbum error: %v", err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(album)
}

func handleGetAlbum(w http.ResponseWriter, r *http.Request, client *dynamodb.Client, table, albumID string) {
	album, err := getAlbum(r.Context(), client, table, albumID)
	if err != nil {
		log.Printf("getAlbum error: %v", err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}
	if album == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(album)
}

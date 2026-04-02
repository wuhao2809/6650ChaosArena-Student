package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"
)

func getEnv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

type App struct {
	albumStore *AlbumStore
	photoStore *PhotoStore
	s3svc      *S3Service
}

func main() {
	port        := getEnv("PORT", "8080")
	region      := getEnv("AWS_REGION", "us-west-2")
	albumsTable := getEnv("ALBUMS_TABLE", "full-album-albums")
	photosTable := getEnv("PHOTOS_TABLE", "full-album-photos")
	bucket      := os.Getenv("S3_BUCKET")

	if bucket == "" {
		log.Fatal("S3_BUCKET environment variable is required")
	}

	ctx := context.Background()
	cfg, err := config.LoadDefaultConfig(ctx, config.WithRegion(region))
	if err != nil {
		log.Fatalf("load AWS config: %v", err)
	}

	app := &App{
		albumStore: &AlbumStore{client: dynamodb.NewFromConfig(cfg), table: albumsTable},
		photoStore: &PhotoStore{client: dynamodb.NewFromConfig(cfg), table: photosTable},
		s3svc:      &S3Service{client: s3.NewFromConfig(cfg), bucket: bucket},
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", app.handleHealth)
	mux.HandleFunc("/albums", app.handleListAlbums) // S6: exact match, no trailing slash
	mux.HandleFunc("/albums/", app.handleAlbums)

	log.Printf("listening on :%s (albums=%s photos=%s bucket=%s)", port, albumsTable, photosTable, bucket)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

// handleHealth satisfies S1.
func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

// handleAlbums routes /albums/{albumID}[/photos[/{photoID}]].
func (a *App) handleAlbums(w http.ResponseWriter, r *http.Request) {
	path  := strings.TrimPrefix(r.URL.Path, "/albums/")
	parts := strings.SplitN(path, "/", 3)

	albumID := parts[0]
	if albumID == "" {
		http.Error(w, `{"error":"missing album_id"}`, http.StatusBadRequest)
		return
	}

	// /albums/{albumID}
	if len(parts) == 1 {
		switch r.Method {
		case http.MethodPut:
			a.handlePutAlbum(w, r, albumID)
		case http.MethodGet:
			a.handleGetAlbum(w, r, albumID)
		default:
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
		return
	}

	if parts[1] != "photos" {
		http.NotFound(w, r)
		return
	}

	// /albums/{albumID}/photos
	if len(parts) < 3 || parts[2] == "" {
		if r.Method == http.MethodPost {
			a.handlePostPhoto(w, r, albumID)
		} else {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
		return
	}

	// /albums/{albumID}/photos/{photoID}
	photoID := parts[2]
	switch r.Method {
	case http.MethodGet:
		a.handleGetPhoto(w, r, albumID, photoID)
	case http.MethodDelete:
		a.handleDeletePhoto(w, r, albumID, photoID)
	default:
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
	}
}

// handleListAlbums satisfies S6: GET /albums returns all albums.
func (a *App) handleListAlbums(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	albums, err := a.albumStore.List(r.Context())
	if err != nil {
		log.Printf("list albums: %v", err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(albums)
}

// handlePutAlbum satisfies S2 (create).
func (a *App) handlePutAlbum(w http.ResponseWriter, r *http.Request, albumID string) {
	var album Album
	if err := json.NewDecoder(r.Body).Decode(&album); err != nil {
		http.Error(w, `{"error":"invalid JSON"}`, http.StatusBadRequest)
		return
	}
	album.AlbumID = albumID
	if err := a.albumStore.Put(r.Context(), album); err != nil {
		log.Printf("put album %s: %v", albumID, err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(album)
}

// handleGetAlbum satisfies S2 (read).
func (a *App) handleGetAlbum(w http.ResponseWriter, r *http.Request, albumID string) {
	album, err := a.albumStore.Get(r.Context(), albumID)
	if err != nil {
		log.Printf("get album %s: %v", albumID, err)
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
	json.NewEncoder(w).Encode(album)
}

// handlePostPhoto satisfies S3 (upload, returns 202 immediately).
func (a *App) handlePostPhoto(w http.ResponseWriter, r *http.Request, albumID string) {
	if err := r.ParseMultipartForm(32 << 20); err != nil {
		http.Error(w, `{"error":"bad multipart form"}`, http.StatusBadRequest)
		return
	}
	file, _, err := r.FormFile("photo")
	if err != nil {
		http.Error(w, `{"error":"missing photo field"}`, http.StatusBadRequest)
		return
	}
	defer file.Close()

	data, err := io.ReadAll(file)
	if err != nil {
		http.Error(w, `{"error":"read photo"}`, http.StatusInternalServerError)
		return
	}

	photoID := uuid.New().String()
	if err := a.photoStore.Put(r.Context(), Photo{
		AlbumID: albumID,
		PhotoID: photoID,
		Status:  "processing",
	}); err != nil {
		log.Printf("put photo %s: %v", photoID, err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}

	// Process asynchronously: upload to S3 and update DynamoDB.
	go a.processPhoto(albumID, photoID, data)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"photo_id": photoID,
		"status":   "processing",
	})
}

// processPhoto uploads photo data to S3 and marks the record as completed.
func (a *App) processPhoto(albumID, photoID string, data []byte) {
	ctx := context.Background()

	url, err := a.s3svc.Upload(ctx, albumID, photoID, data)
	if err != nil {
		log.Printf("s3 upload %s/%s: %v", albumID, photoID, err)
		if updateErr := a.photoStore.UpdateStatus(ctx, albumID, photoID, "failed", ""); updateErr != nil {
			log.Printf("update photo failed status: %v", updateErr)
		}
		return
	}

	if err := a.photoStore.UpdateStatus(ctx, albumID, photoID, "completed", url); err != nil {
		log.Printf("update photo completed %s/%s: %v", albumID, photoID, err)
	}
}

// handleGetPhoto satisfies S3 (poll for status).
func (a *App) handleGetPhoto(w http.ResponseWriter, r *http.Request, albumID, photoID string) {
	photo, err := a.photoStore.Get(r.Context(), albumID, photoID)
	if err != nil {
		log.Printf("get photo %s/%s: %v", albumID, photoID, err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}
	if photo == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]string{"error": "not found"})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(photo)
}

// handleDeletePhoto satisfies S4 (delete from DynamoDB + S3).
func (a *App) handleDeletePhoto(w http.ResponseWriter, r *http.Request, albumID, photoID string) {
	// Delete from S3 first so the presigned URL immediately becomes invalid.
	if err := a.s3svc.Delete(r.Context(), albumID, photoID); err != nil {
		log.Printf("s3 delete %s/%s: %v", albumID, photoID, err)
		// Log but continue: the DynamoDB record must still be removed.
	}

	if err := a.photoStore.Delete(r.Context(), albumID, photoID); err != nil {
		log.Printf("ddb delete photo %s/%s: %v", albumID, photoID, err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "deleted"})
}

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
	albumsTable := getEnv("ALBUMS_TABLE", "naive-albums")
	photosTable := getEnv("PHOTOS_TABLE", "naive-photos")
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
	mux.HandleFunc("/albums", app.handleListAlbums)   // S6: exact match, no trailing slash
	mux.HandleFunc("/albums/", app.handleAlbums)

	log.Printf("listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
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

// handleAlbums routes /albums/{albumID}[/photos[/{photoID}]].
func (a *App) handleAlbums(w http.ResponseWriter, r *http.Request) {
	path  := strings.TrimPrefix(r.URL.Path, "/albums/")
	parts := strings.SplitN(path, "/", 3)

	albumID := parts[0]
	if albumID == "" {
		http.Error(w, `{"error":"missing album_id"}`, http.StatusBadRequest)
		return
	}

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

	if len(parts) < 3 || parts[2] == "" {
		if r.Method == http.MethodPost {
			a.handlePostPhoto(w, r, albumID)
		} else {
			http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		}
		return
	}

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

	// BUG: naive worker does not check if the record was deleted before
	// writing "completed". If DELETE races the background goroutine,
	// the worker will overwrite the deletion with a completed record.
	go a.naiveProcessPhoto(albumID, photoID, data)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	json.NewEncoder(w).Encode(map[string]string{
		"photo_id": photoID,
		"status":   "processing",
	})
}

// naiveProcessPhoto uploads to S3 and unconditionally writes "completed" to
// DynamoDB — it never checks whether the record was deleted in the meantime.
// This causes S7 to fail: a DELETE that races the upload will be overwritten.
func (a *App) naiveProcessPhoto(albumID, photoID string, data []byte) {
	ctx := context.Background()

	url, err := a.s3svc.Upload(ctx, albumID, photoID, data)
	if err != nil {
		log.Printf("s3 upload %s/%s: %v", albumID, photoID, err)
		// Unconditional status update — does not check for prior deletion.
		a.photoStore.UpdateStatus(ctx, albumID, photoID, "failed", "")
		return
	}

	// Unconditional: writes "completed" even if DELETE already ran.
	if err := a.photoStore.UpdateStatus(ctx, albumID, photoID, "completed", url); err != nil {
		log.Printf("update photo completed %s/%s: %v", albumID, photoID, err)
	}
}

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

func (a *App) handleDeletePhoto(w http.ResponseWriter, r *http.Request, albumID, photoID string) {
	if err := a.s3svc.Delete(r.Context(), albumID, photoID); err != nil {
		log.Printf("s3 delete %s/%s: %v", albumID, photoID, err)
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

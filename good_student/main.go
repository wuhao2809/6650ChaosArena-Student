package main

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	awss3 "github.com/aws/aws-sdk-go-v2/service/s3"
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
	albumsTable := getEnv("ALBUMS_TABLE", "good-student-albums")
	photosTable := getEnv("PHOTOS_TABLE", "good-student-photos")
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
		s3svc:      newS3Service(awss3.NewFromConfig(cfg), bucket),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/health", app.handleHealth)
	mux.HandleFunc("/albums", app.handleListAlbums) // exact match — GET /albums
	mux.HandleFunc("/albums/", app.handleAlbums)

	log.Printf("listening on :%s", port)
	log.Fatal(http.ListenAndServe(":"+port, mux))
}

func (a *App) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
}

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

// handlePostPhoto — streaming design for low accept latency (S15).
//
// The key insight: we can find the multipart "photo" part by reading only its
// headers (~200 bytes), assign seq and write metadata, send 202, then stream
// the actual file bytes through a pipe to S3 — all without ever holding the
// full payload in memory.
//
// Timeline:
//   t=0      r.MultipartReader() + NextPart()  — reads ~200 bytes of headers
//   t≈20ms   IncrementPhotoSeq + photoStore.Put — two DynamoDB round-trips
//   t≈20ms   202 sent + flushed                 ← accept latency ends here
//   t≈20ms…  io.Copy(pw, part)  feeds the pipe in the handler goroutine
//              s3manager drains the pipe into S3 in a background goroutine
//   t≈Xs     S3 upload completes, ConditionalUpdateStatus marks "completed"
//
// s3manager uses S3 multipart upload for the io.PipeReader (non-seekable),
// which requires CreateMultipartUpload / UploadPart / CompleteMultipartUpload
// IAM permissions — these are granted in terraform/main.tf.
//
// ConditionalUpdateStatus uses attribute_exists(photo_id): if DELETE races
// the upload and wins, the goroutine's "completed" write is silently dropped
// rather than resurrecting the deleted record (S9).
func (a *App) handlePostPhoto(w http.ResponseWriter, r *http.Request, albumID string) {
	// 1. Locate the "photo" part — reads multipart boundary + part headers only.
	mr, err := r.MultipartReader()
	if err != nil {
		http.Error(w, `{"error":"bad multipart"}`, http.StatusBadRequest)
		return
	}
	var part *multipart.Part
	for {
		p, nextErr := mr.NextPart()
		if nextErr != nil {
			break
		}
		if p.FormName() == "photo" {
			part = p
			break
		}
		io.Copy(io.Discard, p)
		p.Close()
	}
	if part == nil {
		http.Error(w, `{"error":"missing photo field"}`, http.StatusBadRequest)
		return
	}
	defer part.Close()

	// 2. Assign seq atomically and write the metadata record — before 202.
	seq, err := a.albumStore.IncrementPhotoSeq(r.Context(), albumID)
	if err != nil {
		log.Printf("increment seq %s: %v", albumID, err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}
	photoID := uuid.New().String()
	if err := a.photoStore.Put(r.Context(), Photo{
		AlbumID: albumID,
		PhotoID: photoID,
		Seq:     seq,
		Status:  "processing",
	}); err != nil {
		log.Printf("put photo %s: %v", photoID, err)
		http.Error(w, `{"error":"internal server error"}`, http.StatusInternalServerError)
		return
	}

	// 3. Send 202 with explicit Content-Length.
	//
	// Without Content-Length, Go uses chunked transfer encoding and the
	// client's io.ReadAll(resp.Body) blocks until the handler returns.
	// With Content-Length, the client unblocks after N bytes — even though
	// the handler is still streaming to S3 in the background.
	respBody, _ := json.Marshal(map[string]interface{}{
		"photo_id": photoID,
		"seq":      seq,
		"status":   "processing",
	})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(respBody)))
	w.WriteHeader(http.StatusAccepted)
	w.Write(respBody) //nolint:errcheck
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}

	// 4. Stream file bytes: handler goroutine → pipe → S3 upload goroutine.
	//
	// The handler must stay alive (not return) because returning closes r.Body,
	// which would EOF the multipart reader mid-stream.  io.Copy in the handler
	// feeds the pipe; the upload goroutine drains it.
	pr, pw := io.Pipe()

	go func() {
		// If the upload stops consuming pr (e.g. S3 error), pr.CloseWithError
		// propagates back to pw.Write in io.Copy, unblocking the handler.
		var uploadErr error
		defer func() {
			if uploadErr != nil {
				pr.CloseWithError(uploadErr)
			} else {
				pr.Close()
			}
		}()

		// Background context: request context is cancelled when handler returns,
		// but the upload must outlive the handler.
		var url string
		url, uploadErr = a.s3svc.UploadStream(context.Background(), albumID, photoID, pr)
		if uploadErr != nil {
			log.Printf("s3 upload %s/%s: %v", albumID, photoID, uploadErr)
			a.photoStore.ConditionalUpdateStatus(context.Background(), albumID, photoID, "failed", "")
			return
		}
		if err := a.photoStore.ConditionalUpdateStatus(
			context.Background(), albumID, photoID, "completed", url,
		); err != nil {
			log.Printf("update completed %s/%s: %v", albumID, photoID, err)
		}
	}()

	// Feed the pipe from the request body.  Blocks until all bytes are copied
	// or the upload goroutine signals an error via pr.CloseWithError.
	if _, copyErr := io.Copy(pw, part); copyErr != nil {
		pw.CloseWithError(copyErr)
		return
	}
	pw.Close()
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

// handleDeletePhoto deletes the S3 object first, then the DynamoDB record.
// Both S3 DeleteObject and DynamoDB DeleteItem are idempotent, so a second
// DELETE returns 200 rather than 5xx (S7/S8).
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

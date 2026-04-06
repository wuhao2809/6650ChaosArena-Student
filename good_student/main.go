package main

import (
	"bytes"
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

// handlePostPhoto — buffer-then-upload design.
//
// Timeline:
//   t=0      r.MultipartReader() + NextPart()  — reads part headers
//   t=0…     io.ReadAll(part)                  — buffers full body from r.Body
//   t≈Xms    IncrementPhotoSeq + photoStore.Put — two DynamoDB round-trips
//   t≈Xms    202 sent                           ← accept latency ends here
//   t≈Xms…   background goroutine uploads from bytes.NewReader to S3
//   t≈Ys     S3 upload completes, ConditionalUpdateStatus marks "completed"
//
// Why buffer first: the ALB closes the backend TCP connection once it has
// forwarded the 202 response to the client.  Any attempt to read r.Body after
// WriteHeader returns therefore receives an unexpected EOF.  Buffering the
// full body synchronously before writing any response avoids this entirely.
//
// bytes.NewReader is seekable and has a known length, so the S3 transfer
// manager uses a single PutObject call instead of multipart upload — no
// additional IAM permissions required beyond s3:PutObject.
//
// ConditionalUpdateStatus uses attribute_exists(photo_id): if DELETE races
// the upload and wins, the goroutine's "completed" write is silently dropped
// rather than resurrecting the deleted record (S9).
func (a *App) handlePostPhoto(w http.ResponseWriter, r *http.Request, albumID string) {
	// 1. Locate the "photo" part.
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

	// 2. Buffer the full body while r.Body is still reliably open.
	//    Must happen before WriteHeader — the ALB closes the backend connection
	//    after forwarding the response, making subsequent r.Body reads fail.
	photoData, err := io.ReadAll(part)
	if err != nil {
		log.Printf("read photo body %s: %v", albumID, err)
		http.Error(w, `{"error":"read body failed"}`, http.StatusBadRequest)
		return
	}

	// 3. Assign seq atomically and write the metadata record.
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

	// 4. Send 202.
	respBody, _ := json.Marshal(map[string]interface{}{
		"photo_id": photoID,
		"seq":      seq,
		"status":   "processing",
	})
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(respBody)))
	w.WriteHeader(http.StatusAccepted)
	w.Write(respBody) //nolint:errcheck

	// 5. Upload to S3 from the buffered data.
	//    bytes.NewReader is seekable — S3 manager uses PutObject directly.
	go func() {
		url, uploadErr := a.s3svc.UploadStream(context.Background(), albumID, photoID, bytes.NewReader(photoData))
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

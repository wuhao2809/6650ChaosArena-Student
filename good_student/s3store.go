package main

import (
	"context"
	"io"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Service wraps S3 streaming upload, presign, and delete operations.
type S3Service struct {
	client   *s3.Client
	uploader *manager.Uploader
	bucket   string
}

func newS3Service(client *s3.Client, bucket string) *S3Service {
	return &S3Service{
		client:   client,
		uploader: manager.NewUploader(client),
		bucket:   bucket,
	}
}

func (svc *S3Service) objectKey(albumID, photoID string) string {
	return "albums/" + albumID + "/photos/" + photoID
}

// UploadStream streams an io.Reader to S3 using the S3 Transfer Manager.
//
// Unlike a raw PutObject call, the manager does not require a seekable body or
// a known Content-Length.  For bodies smaller than the part size (5 MB default)
// it buffers and uses a single PutObject; for larger bodies it uses S3 multipart
// upload, reading one part at a time — so peak memory per upload is bounded by
// the part size, not the total file size.
//
// This is the correct approach for streaming 100 MB uploads in S15 without
// OOM-killing the container.
func (svc *S3Service) UploadStream(ctx context.Context, albumID, photoID string, body io.Reader) (string, error) {
	key := svc.objectKey(albumID, photoID)

	_, err := svc.uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: aws.String(svc.bucket),
		Key:    aws.String(key),
		Body:   body,
	})
	if err != nil {
		return "", err
	}

	presigner := s3.NewPresignClient(svc.client)
	req, err := presigner.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(svc.bucket),
		Key:    aws.String(key),
	}, s3.WithPresignExpires(time.Hour))
	if err != nil {
		return "", err
	}
	return req.URL, nil
}

// Delete removes the S3 object for a photo.  S3 DeleteObject is always
// idempotent — it returns success even when the key does not exist.
func (svc *S3Service) Delete(ctx context.Context, albumID, photoID string) error {
	_, err := svc.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(svc.bucket),
		Key:    aws.String(svc.objectKey(albumID, photoID)),
	})
	return err
}

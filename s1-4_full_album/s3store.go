package main

import (
	"bytes"
	"context"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// S3Service wraps S3 put, presign, and delete operations for photos.
type S3Service struct {
	client *s3.Client
	bucket string
}

func (svc *S3Service) objectKey(albumID, photoID string) string {
	return "albums/" + albumID + "/photos/" + photoID
}

// Upload stores photo bytes in S3 and returns a 1-hour presigned GET URL.
func (svc *S3Service) Upload(ctx context.Context, albumID, photoID string, data []byte) (string, error) {
	key := svc.objectKey(albumID, photoID)

	_, err := svc.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(svc.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
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

// Delete removes the S3 object for a photo.
func (svc *S3Service) Delete(ctx context.Context, albumID, photoID string) error {
	_, err := svc.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(svc.bucket),
		Key:    aws.String(svc.objectKey(albumID, photoID)),
	})
	return err
}

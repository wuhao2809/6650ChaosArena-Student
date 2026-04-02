package main

import (
	"context"
	"errors"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// ---------------------------------------------------------------------------
// Albums
// ---------------------------------------------------------------------------

type Album struct {
	AlbumID     string `dynamodbav:"album_id" json:"album_id"`
	Title       string `dynamodbav:"title" json:"title"`
	Description string `dynamodbav:"description" json:"description"`
	Owner       string `dynamodbav:"owner" json:"owner"`
}

type AlbumStore struct {
	client *dynamodb.Client
	table  string
}

func (s *AlbumStore) Put(ctx context.Context, album Album) error {
	item, err := attributevalue.MarshalMap(album)
	if err != nil {
		return err
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      item,
	})
	return err
}

func (s *AlbumStore) Get(ctx context.Context, albumID string) (*Album, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			"album_id": &types.AttributeValueMemberS{Value: albumID},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, nil
	}
	var album Album
	if err := attributevalue.UnmarshalMap(out.Item, &album); err != nil {
		return nil, err
	}
	return &album, nil
}

func (s *AlbumStore) List(ctx context.Context) ([]Album, error) {
	out, err := s.client.Scan(ctx, &dynamodb.ScanInput{
		TableName: aws.String(s.table),
	})
	if err != nil {
		return nil, err
	}
	var albums []Album
	// Filter out counter-only items (no title) that result from IncrementPhotoSeq
	// called on an album_id that has not yet been PUT.  In normal operation the
	// album PUT always precedes any photo upload, but the filter is defensive.
	for _, item := range out.Items {
		if _, hasTitleAttr := item["title"]; !hasTitleAttr {
			continue
		}
		var a Album
		if err := attributevalue.UnmarshalMap(item, &a); err == nil {
			albums = append(albums, a)
		}
	}
	if albums == nil {
		albums = []Album{} // return empty slice, not nil, for JSON []
	}
	return albums, nil
}

// IncrementPhotoSeq atomically increments the per-album photo counter and
// returns the new value as the seq number for the next photo.
//
// Uses DynamoDB ADD which treats a missing attribute as 0, so the first
// upload to any album always returns seq=1. Because this is a single
// conditional DynamoDB call, it is safe under arbitrary horizontal scaling —
// no two concurrent uploads to the same album can receive the same seq.
func (s *AlbumStore) IncrementPhotoSeq(ctx context.Context, albumID string) (int64, error) {
	out, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			"album_id": &types.AttributeValueMemberS{Value: albumID},
		},
		UpdateExpression: aws.String("ADD photo_count :one"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one": &types.AttributeValueMemberN{Value: "1"},
		},
		ReturnValues: types.ReturnValueUpdatedNew,
	})
	if err != nil {
		return 0, err
	}
	attr, ok := out.Attributes["photo_count"]
	if !ok {
		return 0, errors.New("photo_count attribute missing from UpdateItem response")
	}
	n, ok := attr.(*types.AttributeValueMemberN)
	if !ok {
		return 0, errors.New("photo_count is not a number attribute")
	}
	seq, err := strconv.ParseInt(n.Value, 10, 64)
	if err != nil {
		return 0, err
	}
	return seq, nil
}

// ---------------------------------------------------------------------------
// Photos
// ---------------------------------------------------------------------------

// Photo uses a composite key: album_id (hash) + photo_id (range).
// Seq is the per-album monotone counter assigned at POST time.
type Photo struct {
	AlbumID string `dynamodbav:"album_id" json:"album_id"`
	PhotoID string `dynamodbav:"photo_id" json:"photo_id"`
	Seq     int64  `dynamodbav:"seq" json:"seq"`
	Status  string `dynamodbav:"status" json:"status"`
	URL     string `dynamodbav:"url,omitempty" json:"url,omitempty"`
}

type PhotoStore struct {
	client *dynamodb.Client
	table  string
}

func (s *PhotoStore) Put(ctx context.Context, photo Photo) error {
	item, err := attributevalue.MarshalMap(photo)
	if err != nil {
		return err
	}
	_, err = s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item:      item,
	})
	return err
}

func (s *PhotoStore) Get(ctx context.Context, albumID, photoID string) (*Photo, error) {
	out, err := s.client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			"album_id": &types.AttributeValueMemberS{Value: albumID},
			"photo_id": &types.AttributeValueMemberS{Value: photoID},
		},
	})
	if err != nil {
		return nil, err
	}
	if len(out.Item) == 0 {
		return nil, nil
	}
	var photo Photo
	if err := attributevalue.UnmarshalMap(out.Item, &photo); err != nil {
		return nil, err
	}
	return &photo, nil
}

// ConditionalUpdateStatus patches status and url only if the record still
// exists.  The ConditionExpression `attribute_exists(photo_id)` makes the
// write a no-op when the photo has already been deleted — this prevents the
// background upload goroutine from resurrecting a deleted record (S9).
func (s *PhotoStore) ConditionalUpdateStatus(ctx context.Context, albumID, photoID, status, url string) error {
	expr := "SET #s = :s"
	names := map[string]string{"#s": "status"}
	values := map[string]types.AttributeValue{
		":s": &types.AttributeValueMemberS{Value: status},
	}
	if url != "" {
		expr += ", #u = :u"
		names["#u"] = "url"
		values[":u"] = &types.AttributeValueMemberS{Value: url}
	}
	_, err := s.client.UpdateItem(ctx, &dynamodb.UpdateItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			"album_id": &types.AttributeValueMemberS{Value: albumID},
			"photo_id": &types.AttributeValueMemberS{Value: photoID},
		},
		UpdateExpression:          aws.String(expr),
		ExpressionAttributeNames:  names,
		ExpressionAttributeValues: values,
		ConditionExpression:       aws.String("attribute_exists(photo_id)"),
	})
	if err != nil {
		// ConditionalCheckFailedException means the record was deleted — that
		// is the expected outcome when DELETE races the upload (S9). Treat it
		// as a no-op rather than an error.
		var condFail *types.ConditionalCheckFailedException
		if errors.As(err, &condFail) {
			return nil
		}
		return err
	}
	return nil
}

// Delete removes the DynamoDB record.  DynamoDB DeleteItem is always
// idempotent — it succeeds even when the item is already absent, so a second
// DELETE call does not cause a 5xx (S7/S8).
func (s *PhotoStore) Delete(ctx context.Context, albumID, photoID string) error {
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			"album_id": &types.AttributeValueMemberS{Value: albumID},
			"photo_id": &types.AttributeValueMemberS{Value: photoID},
		},
	})
	return err
}

package main

import (
	"context"

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
	if err := attributevalue.UnmarshalListOfMaps(out.Items, &albums); err != nil {
		return nil, err
	}
	return albums, nil
}

// ---------------------------------------------------------------------------
// Photos
// ---------------------------------------------------------------------------

// Photo uses a composite key: album_id (hash) + photo_id (range).
type Photo struct {
	AlbumID string `dynamodbav:"album_id" json:"album_id"`
	PhotoID string `dynamodbav:"photo_id" json:"photo_id"`
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

// UpdateStatus unconditionally patches status and url — no check for prior deletion.
func (s *PhotoStore) UpdateStatus(ctx context.Context, albumID, photoID, status, url string) error {
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
	})
	return err
}

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

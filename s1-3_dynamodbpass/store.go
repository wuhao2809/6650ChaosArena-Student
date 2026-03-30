package main

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

// Album represents a single album record stored in DynamoDB.
type Album struct {
	AlbumID     string `dynamodbav:"album_id" json:"album_id"`
	Title       string `dynamodbav:"title" json:"title"`
	Description string `dynamodbav:"description" json:"description"`
	Owner       string `dynamodbav:"owner" json:"owner"`
}

// putAlbum writes an Album to DynamoDB using PutItem.
func putAlbum(ctx context.Context, client *dynamodb.Client, table string, album Album) error {
	item, err := attributevalue.MarshalMap(album)
	if err != nil {
		return err
	}
	_, err = client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(table),
		Item:      item,
	})
	return err
}

// getAlbum retrieves an Album by album_id. Returns nil if the item does not exist.
func getAlbum(ctx context.Context, client *dynamodb.Client, table string, albumID string) (*Album, error) {
	out, err := client.GetItem(ctx, &dynamodb.GetItemInput{
		TableName: aws.String(table),
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

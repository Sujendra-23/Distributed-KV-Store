package dynamostore

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

// NewClient builds a DynamoDB client. With a non-empty endpoint (DynamoDB
// Local / LocalStack) it uses static dummy credentials unless real ones are
// present in the environment; with an empty endpoint it uses the standard AWS
// credential chain and region resolution.
func NewClient(ctx context.Context, endpoint, region string) (*dynamodb.Client, error) {
	opts := []func(*config.LoadOptions) error{}
	if region != "" {
		opts = append(opts, config.WithRegion(region))
	}
	if endpoint != "" {
		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("local", "local", "")))
		if region == "" {
			opts = append(opts, config.WithRegion("us-east-1"))
		}
	}
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, err
	}
	return dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		if endpoint != "" {
			o.BaseEndpoint = aws.String(endpoint)
		}
	}), nil
}

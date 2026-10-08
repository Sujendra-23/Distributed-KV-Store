// Package dynamostore is a DynamoDB-backed storage engine for a KV node.
//
// Each item is {key (S, partition key), value (B), version (N), tombstone
// (BOOL)}. Last-writer-wins is enforced by DynamoDB itself, not by an
// in-process mutex: every write is a conditional PutItem whose
// ConditionExpression compares the stored `version` attribute with the
// incoming one, so concurrent writers on different processes cannot clobber
// a newer version. Deletes are tombstone writes (version bumped, value
// dropped), never DeleteItem.
package dynamostore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

const (
	attrKey       = "key"
	attrValue     = "value"
	attrVersion   = "version"
	attrTombstone = "tombstone"

	// maxPutRetries bounds the read-modify-conditional-write loop used for
	// locally originated writes when another writer keeps winning the race.
	maxPutRetries = 10
)

// Store implements store.Backend on top of one DynamoDB table.
type Store struct {
	db    *dynamodb.Client
	table string
}

// New wraps an existing client. Call EnsureTable first if the table may not exist.
func New(db *dynamodb.Client, table string) *Store { return &Store{db: db, table: table} }

// EnsureTable creates the table (on-demand billing, partition key "key") if
// it does not exist, and waits until it is ACTIVE.
func (s *Store) EnsureTable(ctx context.Context) error {
	_, err := s.db.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String(s.table),
		AttributeDefinitions: []types.AttributeDefinition{
			{AttributeName: aws.String(attrKey), AttributeType: types.ScalarAttributeTypeS},
		},
		KeySchema: []types.KeySchemaElement{
			{AttributeName: aws.String(attrKey), KeyType: types.KeyTypeHash},
		},
		BillingMode: types.BillingModePayPerRequest,
	})
	var inUse *types.ResourceInUseException
	if err != nil && !errors.As(err, &inUse) {
		return fmt.Errorf("create table %q: %w", s.table, err)
	}
	waiter := dynamodb.NewTableExistsWaiter(s.db)
	return waiter.Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String(s.table)}, 30*time.Second)
}

type item struct {
	value     []byte
	version   int64
	tombstone bool
	exists    bool
}

func (s *Store) read(ctx context.Context, key string) (item, error) {
	out, err := s.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(s.table),
		Key:            map[string]types.AttributeValue{attrKey: &types.AttributeValueMemberS{Value: key}},
		ConsistentRead: aws.Bool(true), // version decisions need the latest write
	})
	if err != nil {
		return item{}, err
	}
	if len(out.Item) == 0 {
		return item{}, nil
	}
	it := item{exists: true}
	if v, ok := out.Item[attrValue].(*types.AttributeValueMemberB); ok {
		it.value = v.Value
	}
	if v, ok := out.Item[attrVersion].(*types.AttributeValueMemberN); ok {
		it.version, _ = strconv.ParseInt(v.Value, 10, 64)
	}
	if v, ok := out.Item[attrTombstone].(*types.AttributeValueMemberBOOL); ok {
		it.tombstone = v.Value
	}
	return it, nil
}

// conditionalPut writes the item only if no item exists or its stored version
// is strictly lower than `version`. applied=false (nil error) means DynamoDB
// rejected it via ConditionalCheckFailedException, i.e. the write was stale.
func (s *Store) conditionalPut(ctx context.Context, key string, value []byte, version int64, tombstone bool) (applied bool, err error) {
	av := map[string]types.AttributeValue{
		attrKey:       &types.AttributeValueMemberS{Value: key},
		attrVersion:   &types.AttributeValueMemberN{Value: strconv.FormatInt(version, 10)},
		attrTombstone: &types.AttributeValueMemberBOOL{Value: tombstone},
	}
	if !tombstone {
		av[attrValue] = &types.AttributeValueMemberB{Value: value}
	}
	_, err = s.db.PutItem(ctx, &dynamodb.PutItemInput{
		TableName:           aws.String(s.table),
		Item:                av,
		ConditionExpression: aws.String("attribute_not_exists(#k) OR #v < :ver"),
		ExpressionAttributeNames: map[string]string{
			"#k": attrKey,
			"#v": attrVersion,
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":ver": &types.AttributeValueMemberN{Value: strconv.FormatInt(version, 10)},
		},
	})
	if err != nil {
		var ccf *types.ConditionalCheckFailedException
		if errors.As(err, &ccf) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// writeNext assigns version = current+1 using optimistic concurrency: read
// the current version, attempt a conditional write at current+1, and retry if
// another writer got there first (the condition rejects us).
func (s *Store) writeNext(ctx context.Context, key string, value []byte, tombstone bool) (int64, error) {
	for i := 0; i < maxPutRetries; i++ {
		cur, err := s.read(ctx, key)
		if err != nil {
			return 0, err
		}
		next := cur.version + 1
		ok, err := s.conditionalPut(ctx, key, value, next, tombstone)
		if err != nil {
			return 0, err
		}
		if ok {
			return next, nil
		}
	}
	return 0, fmt.Errorf("dynamostore: write to %q lost the version race %d times", key, maxPutRetries)
}

func (s *Store) Put(ctx context.Context, key string, value []byte) (int64, error) {
	return s.writeNext(ctx, key, value, false)
}

func (s *Store) Delete(ctx context.Context, key string) (int64, error) {
	return s.writeNext(ctx, key, nil, true)
}

func (s *Store) ApplyReplicated(ctx context.Context, key string, value []byte, version int64, tombstone bool) (bool, error) {
	return s.conditionalPut(ctx, key, value, version, tombstone)
}

func (s *Store) Get(ctx context.Context, key string) ([]byte, bool, error) {
	it, err := s.read(ctx, key)
	if err != nil {
		return nil, false, err
	}
	if !it.exists || it.tombstone {
		return nil, false, nil
	}
	return it.value, true, nil
}

// Count returns live (non-tombstone) keys via a filtered, paginated Scan.
// It is O(table size); fine for a node-local health metric, not a hot path.
func (s *Store) Count(ctx context.Context) (int, error) {
	total := 0
	var start map[string]types.AttributeValue
	for {
		out, err := s.db.Scan(ctx, &dynamodb.ScanInput{
			TableName:                aws.String(s.table),
			Select:                   types.SelectCount,
			FilterExpression:         aws.String("#t = :f"),
			ExpressionAttributeNames: map[string]string{"#t": attrTombstone},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":f": &types.AttributeValueMemberBOOL{Value: false},
			},
			ExclusiveStartKey: start,
		})
		if err != nil {
			return 0, err
		}
		total += int(out.Count)
		if out.LastEvaluatedKey == nil {
			return total, nil
		}
		start = out.LastEvaluatedKey
	}
}

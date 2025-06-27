package redis

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/boostgo/contextx"
	"github.com/boostgo/errorx"
	"github.com/redis/go-redis/v9"
)

type singleClient struct {
	client redis.UniversalClient
}

func New(address string, port, db int, password string, opts ...Option) (Client, error) {
	conn, err := Connect(address, port, db, password, opts...)
	if err != nil {
		return nil, err
	}

	return &singleClient{
		client: conn,
	}, nil
}

func Must(address string, port, db int, password string, opts ...Option) Client {
	client, err := New(address, port, db, password, opts...)
	if err != nil {
		panic(err)
	}

	return client
}

func NewFromClient(conn *redis.Client) Client {
	return &singleClient{
		client: conn,
	}
}

func (client *singleClient) Close() error {
	return client.client.Close()
}

func (client *singleClient) Client(ctx context.Context) (redis.UniversalClient, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, err
	}

	return client.client, nil
}

func (client *singleClient) Pipeline(ctx context.Context) (redis.Pipeliner, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, err
	}

	return client.client.Pipeline(), nil
}

func (client *singleClient) TxPipeline(ctx context.Context) (redis.Pipeliner, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, err
	}

	return client.client.TxPipeline(), nil
}

func (client *singleClient) Keys(ctx context.Context, pattern string) ([]string, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, err
	}

	return client.client.Keys(ctx, pattern).Result()
}

func (client *singleClient) Delete(ctx context.Context, keys ...string) error {
	if err := contextx.Validate(ctx); err != nil {
		return err
	}

	if len(keys) == 0 {
		return nil
	}

	// clean up keys from empty
	keys = slices.DeleteFunc(keys, func(key string) bool {
		return key == ""
	})

	if len(keys) == 0 {
		return nil
	}

	return client.client.Del(ctx, keys...).Err()
}

func (client *singleClient) Dump(ctx context.Context, key string) (string, error) {
	if err := validate(ctx, key); err != nil {
		return "", err
	}

	return client.client.Dump(ctx, key).Result()
}

func (client *singleClient) Rename(ctx context.Context, oldKey, newKey string) error {
	if err := validate(ctx, oldKey); err != nil {
		return ErrInvalidKey.
			SetError(err).
			AddParam("key_type", "old")
	}

	if err := validate(ctx, newKey); err != nil {
		return ErrInvalidKey.
			SetError(err).
			AddParam("key_type", "new")
	}

	return client.client.Rename(ctx, oldKey, newKey).Err()
}

func (client *singleClient) Refresh(ctx context.Context, key string, ttl time.Duration) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	return client.client.Expire(ctx, key, ttl).Err()
}

func (client *singleClient) RefreshAt(ctx context.Context, key string, at time.Time) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	return client.client.ExpireAt(ctx, key, at).Err()
}

func (client *singleClient) TTL(ctx context.Context, key string) (time.Duration, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	ttl, err := client.client.TTL(ctx, key).Result()
	if err != nil {
		return ttl, err
	}

	const notExistKey = -2
	if ttl == notExistKey {
		return ttl, errorx.ErrNotFound
	}

	return ttl, nil
}

func (client *singleClient) Set(ctx context.Context, key string, value any, ttl ...time.Duration) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	var expireAt time.Duration
	if len(ttl) > 0 && ttl[0] > 0 {
		expireAt = ttl[0]
	}

	return client.client.Set(ctx, key, value, expireAt).Err()
}

func (client *singleClient) Get(ctx context.Context, key string) (string, error) {
	if err := validate(ctx, key); err != nil {
		return "", err
	}

	result, err := client.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return result, ErrKeyNotFound.
				AddParam("key", key)
		}

		return result, err
	}

	return result, nil
}

func (client *singleClient) MGet(ctx context.Context, keys []string) ([]any, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, err
	}

	return client.client.MGet(ctx, keys...).Result()
}

func (client *singleClient) Exist(ctx context.Context, key string) (int64, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	return client.client.Exists(ctx, key).Result()
}

func (client *singleClient) GetBytes(ctx context.Context, key string) ([]byte, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	result, err := client.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return result, ErrKeyNotFound.
				AddParam("key", key)
		}

		return result, err
	}

	return result, nil
}

func (client *singleClient) GetInt(ctx context.Context, key string) (int, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	result, err := client.client.Get(ctx, key).Int()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return result, ErrKeyNotFound.
				AddParam("key", key)
		}

		return result, err
	}

	return result, nil
}

func (client *singleClient) Parse(ctx context.Context, key string, export any) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	result, err := client.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrKeyNotFound.
				AddParam("key", key)
		}

		return err
	}

	return json.Unmarshal(result, &export)
}

func (client *singleClient) HSet(ctx context.Context, key string, value map[string]any) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	return client.client.HSet(ctx, key, value).Err()
}

func (client *singleClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	return client.client.HGetAll(ctx, key).Result()
}

func (client *singleClient) HGet(ctx context.Context, key, field string) (string, error) {
	if err := validate(ctx, key); err != nil {
		return "", err
	}

	return client.client.HGet(ctx, key, field).Result()
}

func (client *singleClient) HGetInt(ctx context.Context, key, field string) (int, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	return client.client.HGet(ctx, key, field).Int()
}

func (client *singleClient) HGetBool(ctx context.Context, key, field string) (bool, error) {
	if err := validate(ctx, key); err != nil {
		return false, err
	}

	return client.client.HGet(ctx, key, field).Bool()
}

func (client *singleClient) HExist(ctx context.Context, key, field string) (bool, error) {
	if err := validate(ctx, key); err != nil {
		return false, err
	}

	return client.client.HExists(ctx, key, field).Result()
}

func (client *singleClient) HScan(
	ctx context.Context,
	key string,
	cursor uint64,
	pattern string,
	count int64,
) ([]string, uint64, error) {
	if err := validate(ctx, key); err != nil {
		return nil, 0, err
	}

	return client.client.HScan(ctx, key, cursor, pattern, count).Result()
}

func (client *singleClient) HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	return client.client.HIncrBy(ctx, key, field, incr).Result()
}

func (client *singleClient) HIncrByFloat(
	ctx context.Context,
	key, field string,
	incr float64,
) (float64, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	return client.client.HIncrByFloat(ctx, key, field, incr).Result()
}

func (client *singleClient) HDelete(ctx context.Context, key string, fields ...string) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	return client.client.HDel(ctx, key, fields...).Err()
}

func (client *singleClient) HKeys(ctx context.Context, key string) ([]string, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	return client.client.HKeys(ctx, key).Result()
}

func (client *singleClient) HLen(ctx context.Context, key string) (int64, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	return client.client.HLen(ctx, key).Result()
}

func (client *singleClient) HMGet(ctx context.Context, key string, fields ...string) ([]any, error) {
	if len(fields) == 0 {
		return []any{}, nil
	}

	if err := validate(ctx, key); err != nil {
		return []any{}, err
	}

	return client.client.HMGet(ctx, key, fields...).Result()
}

func (client *singleClient) HMSet(ctx context.Context, key string, values ...any) error {
	if len(values) == 0 {
		return nil
	}

	if err := validate(ctx, key); err != nil {
		return err
	}

	return client.client.HMSet(ctx, key, values...).Err()
}

func (client *singleClient) HSetNX(ctx context.Context, key, field string, value any) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	return client.client.HSetNX(ctx, key, field, value).Err()
}

func (client *singleClient) Scan(
	ctx context.Context,
	cursor uint64,
	pattern string,
	count int64,
) ([]string, uint64, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, 0, err
	}

	return client.client.Scan(ctx, cursor, pattern, count).Result()
}

func (client *singleClient) HScanNoValues(
	ctx context.Context,
	key string,
	cursor uint64,
	pattern string,
	count int64,
) ([]string, uint64, error) {
	if err := validate(ctx, key); err != nil {
		return nil, 0, err
	}

	return client.client.HScanNoValues(ctx, key, cursor, pattern, count).Result()
}

func (client *singleClient) HVals(ctx context.Context, key string) ([]string, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	return client.client.HVals(ctx, key).Result()
}

func (client *singleClient) HRandField(ctx context.Context, key string, count int) ([]string, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	return client.client.HRandField(ctx, key, count).Result()
}

func (client *singleClient) HRandFieldWithValues(
	ctx context.Context,
	key string,
	count int,
) ([]redis.KeyValue, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	return client.client.HRandFieldWithValues(ctx, key, count).Result()
}

func (client *singleClient) HExpire(
	ctx context.Context,
	key string,
	expiration time.Duration,
	fields ...string,
) ([]int64, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	return client.client.HExpire(ctx, key, expiration, fields...).Result()
}

func (client *singleClient) HTTL(ctx context.Context, key string, fields ...string) ([]int64, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	return client.client.HTTL(ctx, key, fields...).Result()
}

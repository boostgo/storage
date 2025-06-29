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

func (c *singleClient) Close() error {
	return c.client.Close()
}

func (c *singleClient) Client(ctx context.Context) (redis.UniversalClient, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, err
	}

	return c.client, nil
}

func (c *singleClient) Pipeline(ctx context.Context) (redis.Pipeliner, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, err
	}

	return c.client.Pipeline(), nil
}

func (c *singleClient) TxPipeline(ctx context.Context) (redis.Pipeliner, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, err
	}

	return c.client.TxPipeline(), nil
}

func (c *singleClient) Keys(ctx context.Context, pattern string) ([]string, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, err
	}

	return c.client.Keys(ctx, pattern).Result()
}

func (c *singleClient) Delete(ctx context.Context, keys ...string) error {
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

	return c.client.Del(ctx, keys...).Err()
}

func (c *singleClient) Dump(ctx context.Context, key string) (string, error) {
	if err := validate(ctx, key); err != nil {
		return "", err
	}

	return c.client.Dump(ctx, key).Result()
}

func (c *singleClient) Rename(ctx context.Context, oldKey, newKey string) error {
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

	return c.client.Rename(ctx, oldKey, newKey).Err()
}

func (c *singleClient) Refresh(ctx context.Context, key string, ttl time.Duration) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	return c.client.Expire(ctx, key, ttl).Err()
}

func (c *singleClient) RefreshAt(ctx context.Context, key string, at time.Time) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	return c.client.ExpireAt(ctx, key, at).Err()
}

func (c *singleClient) TTL(ctx context.Context, key string) (time.Duration, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	ttl, err := c.client.TTL(ctx, key).Result()
	if err != nil {
		return ttl, err
	}

	const notExistKey = -2
	if ttl == notExistKey {
		return ttl, errorx.ErrNotFound
	}

	return ttl, nil
}

func (c *singleClient) Set(ctx context.Context, key string, value any, ttl ...time.Duration) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	var expireAt time.Duration
	if len(ttl) > 0 && ttl[0] > 0 {
		expireAt = ttl[0]
	}

	return c.client.Set(ctx, key, value, expireAt).Err()
}

func (c *singleClient) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	if err := validate(ctx, key); err != nil {
		return false, err
	}

	return c.client.SetNX(ctx, key, value, ttl).Result()
}

func (c *singleClient) Get(ctx context.Context, key string) (string, error) {
	if err := validate(ctx, key); err != nil {
		return "", err
	}

	result, err := c.client.Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return result, ErrKeyNotFound.
				AddParam("key", key)
		}

		return result, err
	}

	return result, nil
}

func (c *singleClient) MGet(ctx context.Context, keys []string) ([]any, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, err
	}

	return c.client.MGet(ctx, keys...).Result()
}

func (c *singleClient) Exist(ctx context.Context, key string) (int64, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	return c.client.Exists(ctx, key).Result()
}

func (c *singleClient) GetBytes(ctx context.Context, key string) ([]byte, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	result, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return result, ErrKeyNotFound.
				AddParam("key", key)
		}

		return result, err
	}

	return result, nil
}

func (c *singleClient) GetInt(ctx context.Context, key string) (int, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	result, err := c.client.Get(ctx, key).Int()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return result, ErrKeyNotFound.
				AddParam("key", key)
		}

		return result, err
	}

	return result, nil
}

func (c *singleClient) Parse(ctx context.Context, key string, export any) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	result, err := c.client.Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrKeyNotFound.
				AddParam("key", key)
		}

		return err
	}

	return json.Unmarshal(result, &export)
}

func (c *singleClient) HSet(ctx context.Context, key string, value map[string]any) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	return c.client.HSet(ctx, key, value).Err()
}

func (c *singleClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	return c.client.HGetAll(ctx, key).Result()
}

func (c *singleClient) HGet(ctx context.Context, key, field string) (string, error) {
	if err := validate(ctx, key); err != nil {
		return "", err
	}

	return c.client.HGet(ctx, key, field).Result()
}

func (c *singleClient) HGetInt(ctx context.Context, key, field string) (int, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	return c.client.HGet(ctx, key, field).Int()
}

func (c *singleClient) HGetBool(ctx context.Context, key, field string) (bool, error) {
	if err := validate(ctx, key); err != nil {
		return false, err
	}

	return c.client.HGet(ctx, key, field).Bool()
}

func (c *singleClient) HExist(ctx context.Context, key, field string) (bool, error) {
	if err := validate(ctx, key); err != nil {
		return false, err
	}

	return c.client.HExists(ctx, key, field).Result()
}

func (c *singleClient) HScan(
	ctx context.Context,
	key string,
	cursor uint64,
	pattern string,
	count int64,
) ([]string, uint64, error) {
	if err := validate(ctx, key); err != nil {
		return nil, 0, err
	}

	return c.client.HScan(ctx, key, cursor, pattern, count).Result()
}

func (c *singleClient) HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	return c.client.HIncrBy(ctx, key, field, incr).Result()
}

func (c *singleClient) HIncrByFloat(
	ctx context.Context,
	key, field string,
	incr float64,
) (float64, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	return c.client.HIncrByFloat(ctx, key, field, incr).Result()
}

func (c *singleClient) HDelete(ctx context.Context, key string, fields ...string) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	return c.client.HDel(ctx, key, fields...).Err()
}

func (c *singleClient) HKeys(ctx context.Context, key string) ([]string, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	return c.client.HKeys(ctx, key).Result()
}

func (c *singleClient) HLen(ctx context.Context, key string) (int64, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	return c.client.HLen(ctx, key).Result()
}

func (c *singleClient) HMGet(ctx context.Context, key string, fields ...string) ([]any, error) {
	if len(fields) == 0 {
		return []any{}, nil
	}

	if err := validate(ctx, key); err != nil {
		return []any{}, err
	}

	return c.client.HMGet(ctx, key, fields...).Result()
}

func (c *singleClient) HMSet(ctx context.Context, key string, values ...any) error {
	if len(values) == 0 {
		return nil
	}

	if err := validate(ctx, key); err != nil {
		return err
	}

	return c.client.HMSet(ctx, key, values...).Err()
}

func (c *singleClient) HSetNX(ctx context.Context, key, field string, value any) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	return c.client.HSetNX(ctx, key, field, value).Err()
}

func (c *singleClient) Scan(
	ctx context.Context,
	cursor uint64,
	pattern string,
	count int64,
) ([]string, uint64, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, 0, err
	}

	return c.client.Scan(ctx, cursor, pattern, count).Result()
}

func (c *singleClient) HScanNoValues(
	ctx context.Context,
	key string,
	cursor uint64,
	pattern string,
	count int64,
) ([]string, uint64, error) {
	if err := validate(ctx, key); err != nil {
		return nil, 0, err
	}

	return c.client.HScanNoValues(ctx, key, cursor, pattern, count).Result()
}

func (c *singleClient) HVals(ctx context.Context, key string) ([]string, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	return c.client.HVals(ctx, key).Result()
}

func (c *singleClient) HRandField(ctx context.Context, key string, count int) ([]string, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	return c.client.HRandField(ctx, key, count).Result()
}

func (c *singleClient) HRandFieldWithValues(
	ctx context.Context,
	key string,
	count int,
) ([]redis.KeyValue, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	return c.client.HRandFieldWithValues(ctx, key, count).Result()
}

func (c *singleClient) HExpire(
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

	return c.client.HExpire(ctx, key, expiration, fields...).Result()
}

func (c *singleClient) HTTL(ctx context.Context, key string, fields ...string) ([]int64, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	return c.client.HTTL(ctx, key, fields...).Result()
}

func (c *singleClient) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	if err := validateMultiple(ctx, keys); err != nil {
		return nil, err
	}

	return c.client.Eval(ctx, script, keys, args...).Result()
}

func (c *singleClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) (any, error) {
	if err := validateMultiple(ctx, keys); err != nil {
		return nil, err
	}

	return c.client.EvalSha(ctx, sha1, keys, args...).Result()
}

func (c *singleClient) EvalRO(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	if err := validateMultiple(ctx, keys); err != nil {
		return nil, err
	}

	return c.client.EvalRO(ctx, script, keys, args...).Result()
}

func (c *singleClient) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...any) (any, error) {
	if err := validateMultiple(ctx, keys); err != nil {
		return nil, err
	}

	return c.client.EvalShaRO(ctx, sha1, keys, args...).Result()
}

func (c *singleClient) ScriptExists(ctx context.Context, hashes ...string) ([]bool, error) {
	return c.client.ScriptExists(ctx, hashes...).Result()
}

func (c *singleClient) ScriptFlush(ctx context.Context) (string, error) {
	return c.client.ScriptFlush(ctx).Result()
}

func (c *singleClient) ScriptKill(ctx context.Context) (string, error) {
	return c.client.ScriptKill(ctx).Result()
}

func (c *singleClient) ScriptLoad(ctx context.Context, script string) (string, error) {
	return c.client.ScriptLoad(ctx, script).Result()
}

package redis

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/boostgo/contextx"
	"github.com/boostgo/errorx"
	"github.com/boostgo/storage"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"
)

type ClientSelector func(ctx context.Context, clients []ShardClient) ShardClient

type shardClient struct {
	clients *Clients
}

// NewShard creates client implementation as shard client.
//
// Need to provide Clients object which contains multiple clients for sharding
func NewShard(clients *Clients) Client {
	return &shardClient{
		clients: clients,
	}
}

func (c *shardClient) Close() error {
	return c.clients.Close()
}

func (c *shardClient) Client(ctx context.Context) (redis.UniversalClient, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, err
	}

	client, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return client.Client(), nil
}

func (c *shardClient) Pipeline(ctx context.Context) (redis.Pipeliner, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().Pipeline(), nil
}

func (c *shardClient) TxPipeline(ctx context.Context) (redis.Pipeliner, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().TxPipeline(), nil
}

func (c *shardClient) Keys(ctx context.Context, pattern string) ([]string, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().Keys(ctx, pattern).Result()
}

func (c *shardClient) Delete(ctx context.Context, keys ...string) error {
	if err := contextx.Validate(ctx); err != nil {
		return err
	}

	if len(keys) == 0 {
		return nil
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return err
	}

	// clean up keys from empty
	keys = slices.DeleteFunc(keys, func(key string) bool {
		return key == ""
	})

	if len(keys) == 0 {
		return nil
	}

	return raw.Client().Del(ctx, keys...).Err()
}

func (c *shardClient) Dump(ctx context.Context, key string) (string, error) {
	if err := validate(ctx, key); err != nil {
		return "", err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return "", err
	}

	return raw.Client().Dump(ctx, key).Result()
}

func (c *shardClient) Rename(ctx context.Context, oldKey, newKey string) error {
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

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return err
	}

	return raw.Client().Rename(ctx, oldKey, newKey).Err()
}

func (c *shardClient) Refresh(ctx context.Context, key string, ttl time.Duration) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return err
	}

	return raw.Client().Expire(ctx, key, ttl).Err()
}

func (c *shardClient) RefreshAt(ctx context.Context, key string, at time.Time) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return err
	}

	return raw.Client().ExpireAt(ctx, key, at).Err()
}

func (c *shardClient) TTL(ctx context.Context, key string) (time.Duration, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return 0, err
	}

	ttl, err := raw.Client().TTL(ctx, key).Result()
	if err != nil {
		return ttl, err
	}

	const notExistKey = -2
	if ttl == notExistKey {
		return ttl, errorx.ErrNotFound
	}

	return ttl, nil
}

func (c *shardClient) Set(ctx context.Context, key string, value any, ttl ...time.Duration) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	var expireAt time.Duration
	if len(ttl) > 0 && ttl[0] > 0 {
		expireAt = ttl[0]
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return err
	}

	return raw.Client().Set(ctx, key, value, expireAt).Err()
}

func (c *shardClient) SetNX(ctx context.Context, key string, value any, ttl time.Duration) (bool, error) {
	if err := validate(ctx, key); err != nil {
		return false, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return false, err
	}

	return raw.Client().SetNX(ctx, key, value, ttl).Result()
}

func (c *shardClient) Get(ctx context.Context, key string) (string, error) {
	if err := validate(ctx, key); err != nil {
		return "", err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return "", err
	}

	result, err := raw.Client().Get(ctx, key).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return result, ErrKeyNotFound.
				AddParam("key", key)
		}

		return result, err
	}

	return result, nil
}

func (c *shardClient) MGet(ctx context.Context, keys []string) ([]any, error) {
	if err := validateMultiple(ctx, keys); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	result, err := raw.Client().MGet(ctx, keys...).Result()
	if err != nil {
		return nil, err
	}

	return result, nil
}

func (c *shardClient) Exist(ctx context.Context, key string) (int64, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return 0, err
	}

	return raw.Client().Exists(ctx, key).Result()
}

func (c *shardClient) GetBytes(ctx context.Context, key string) ([]byte, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	result, err := raw.Client().Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, ErrKeyNotFound.
				AddParam("key", key)
		}

		return result, err
	}

	return result, nil
}

func (c *shardClient) GetInt(ctx context.Context, key string) (int, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return 0, err
	}

	result, err := raw.Client().Get(ctx, key).Int()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return 0, ErrKeyNotFound.
				AddParam("key", key)
		}

		return 0, err
	}

	return result, nil
}

func (c *shardClient) Parse(ctx context.Context, key string, export any) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return err
	}

	var result []byte
	result, err = raw.Client().Get(ctx, key).Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrKeyNotFound.
				AddParam("key", key)
		}

		return err
	}

	return json.Unmarshal(result, &export)
}

func (c *shardClient) HSet(ctx context.Context, key string, value map[string]any) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return err
	}

	return raw.Client().HSet(ctx, key, value).Err()
}

func (c *shardClient) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().HGetAll(ctx, key).Result()
}

func (c *shardClient) HGet(ctx context.Context, key, field string) (string, error) {
	if err := validate(ctx, key); err != nil {
		return "", err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return "", err
	}

	return raw.Client().HGet(ctx, key, field).Result()
}

func (c *shardClient) HGetInt(ctx context.Context, key, field string) (int, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return 0, err
	}

	return raw.Client().HGet(ctx, key, field).Int()
}

func (c *shardClient) HGetBool(ctx context.Context, key, field string) (bool, error) {
	if err := validate(ctx, key); err != nil {
		return false, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return false, err
	}

	return raw.Client().HGet(ctx, key, field).Bool()
}

func (c *shardClient) HExist(ctx context.Context, key, field string) (bool, error) {
	if err := validate(ctx, key); err != nil {
		return false, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return false, err
	}

	return raw.Client().HExists(ctx, key, field).Result()
}

func (c *shardClient) HDelete(ctx context.Context, key string, fields ...string) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return err
	}

	return raw.Client().HDel(ctx, key, fields...).Err()
}

func (c *shardClient) HScan(
	ctx context.Context,
	key string,
	cursor uint64,
	pattern string,
	count int64,
) ([]string, uint64, error) {
	if err := validate(ctx, key); err != nil {
		return nil, 0, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, 0, err
	}

	return raw.Client().HScan(ctx, key, cursor, pattern, count).Result()
}

func (c *shardClient) Scan(
	ctx context.Context,
	cursor uint64,
	pattern string,
	count int64,
) ([]string, uint64, error) {
	if err := contextx.Validate(ctx); err != nil {
		return nil, 0, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, 0, err
	}

	return raw.Client().Scan(ctx, cursor, pattern, count).Result()
}

func (c *shardClient) HIncrBy(ctx context.Context, key, field string, incr int64) (int64, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return 0, err
	}

	return raw.Client().HIncrBy(ctx, key, field, incr).Result()
}

func (c *shardClient) HIncrByFloat(ctx context.Context, key, field string, incr float64) (float64, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return 0, err
	}

	return raw.Client().HIncrByFloat(ctx, key, field, incr).Result()
}

func (c *shardClient) HKeys(ctx context.Context, key string) ([]string, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().HKeys(ctx, key).Result()
}

func (c *shardClient) HLen(ctx context.Context, key string) (int64, error) {
	if err := validate(ctx, key); err != nil {
		return 0, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return 0, err
	}

	return raw.Client().HLen(ctx, key).Result()
}

func (c *shardClient) HMGet(ctx context.Context, key string, fields ...string) ([]any, error) {
	if len(fields) == 0 {
		return []any{}, nil
	}

	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().HMGet(ctx, key, fields...).Result()
}

func (c *shardClient) HMSet(ctx context.Context, key string, values ...any) error {
	if len(values) == 0 {
		return nil
	}

	if err := validate(ctx, key); err != nil {
		return err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return err
	}

	return raw.Client().HMSet(ctx, key, values...).Err()
}

func (c *shardClient) HSetNX(ctx context.Context, key, field string, value any) error {
	if err := validate(ctx, key); err != nil {
		return err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return err
	}

	return raw.Client().HSetNX(ctx, key, field, value).Err()
}

func (c *shardClient) HScanNoValues(
	ctx context.Context,
	key string,
	cursor uint64,
	pattern string,
	count int64,
) ([]string, uint64, error) {
	if err := validate(ctx, key); err != nil {
		return nil, cursor, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, 0, err
	}

	return raw.Client().HScanNoValues(ctx, key, cursor, pattern, count).Result()
}

func (c *shardClient) HVals(ctx context.Context, key string) ([]string, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().HVals(ctx, key).Result()
}

func (c *shardClient) HRandField(ctx context.Context, key string, count int) ([]string, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().HRandField(ctx, key, count).Result()
}

func (c *shardClient) HRandFieldWithValues(ctx context.Context, key string, count int) ([]redis.KeyValue, error) {
	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().HRandFieldWithValues(ctx, key, count).Result()
}

func (c *shardClient) HExpire(
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

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().HExpire(ctx, key, expiration, fields...).Result()
}

func (c *shardClient) HTTL(ctx context.Context, key string, fields ...string) ([]int64, error) {
	if len(fields) == 0 {
		return nil, nil
	}

	if err := validate(ctx, key); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().HTTL(ctx, key, fields...).Result()
}

func (c *shardClient) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	if err := validateMultiple(ctx, keys); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().Eval(ctx, script, keys, args...).Result()
}

func (c *shardClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) (any, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	if err := validateMultiple(ctx, keys); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().EvalSha(ctx, sha1, keys, args...).Result()
}

func (c *shardClient) EvalRO(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	if err := validateMultiple(ctx, keys); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().EvalRO(ctx, script, keys, args...).Result()
}

func (c *shardClient) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...any) (any, error) {
	if len(keys) == 0 {
		return nil, nil
	}

	if err := validateMultiple(ctx, keys); err != nil {
		return nil, err
	}

	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().EvalShaRO(ctx, sha1, keys, args...).Result()
}

func (c *shardClient) ScriptExists(ctx context.Context, hashes ...string) ([]bool, error) {
	raw, err := c.clients.Get(ctx)
	if err != nil {
		return nil, err
	}

	return raw.Client().ScriptExists(ctx, hashes...).Result()
}

func (c *shardClient) ScriptFlush(ctx context.Context) (string, error) {
	raw, err := c.clients.Get(ctx)
	if err != nil {
		return "", err
	}

	return raw.Client().ScriptFlush(ctx).Result()
}

func (c *shardClient) ScriptKill(ctx context.Context) (string, error) {
	raw, err := c.clients.Get(ctx)
	if err != nil {
		return "", err
	}

	return raw.Client().ScriptKill(ctx).Result()
}

func (c *shardClient) ScriptLoad(ctx context.Context, script string) (string, error) {
	raw, err := c.clients.Get(ctx)
	if err != nil {
		return "", err
	}

	return raw.Client().ScriptLoad(ctx, script).Result()
}

type ShardClient interface {
	Key() string
	Conditions() []string
	Client() redis.UniversalClient
	Close() error
}

// Clients contain all clients for shard client and selector for choosing connection
type Clients struct {
	clients  []ShardClient
	selector ClientSelector
}

func newClients(clients []ShardClient, selector ClientSelector) *Clients {
	return &Clients{
		clients:  clients,
		selector: selector,
	}
}

// Get returns shard connect by using selector
func (c *Clients) Get(ctx context.Context) (ShardClient, error) {
	// get shard by provided selector
	conn := c.selector(ctx, c.clients)
	if conn == nil {
		return nil, storage.ErrConnNotSelected
	}

	return conn, nil
}

// Clients return all shard clients
func (c *Clients) Clients() []ShardClient {
	return c.clients
}

// RawConnections returns all clients as []*sqlx.DB
func (c *Clients) RawConnections() []redis.UniversalClient {
	clients := make([]redis.UniversalClient, len(c.clients))
	for idx, client := range c.clients {
		clients[idx] = client.Client()
	}
	return clients
}

// Close all clients in parallel
func (c *Clients) Close() error {
	wg := errgroup.Group{}

	for _, conn := range c.clients {
		wg.Go(conn.Close)
	}

	return wg.Wait()
}

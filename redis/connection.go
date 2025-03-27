package redis

import (
	"context"
	"fmt"
	"time"

	"github.com/boostgo/errorx"
	"github.com/redis/go-redis/v9"
)

type Option func(options *redis.Options)

type ShardConnectConfig struct {
	Key        string   `json:"key" yaml:"key"`
	Address    string   `json:"address" yaml:"address"`
	Port       int      `json:"port" yaml:"port"`
	DB         int      `json:"db" yaml:"db"`
	Password   string   `json:"password" yaml:"password"`
	Conditions []string `json:"conditions" yaml:"conditions"`
}

type ConnectionConfig struct {
	Address  string
	Port     int
	DB       int
	Password string
}

func Connect(address string, port, db int, password string, opts ...Option) (*redis.Client, error) {
	options := &redis.Options{
		Addr:     fmt.Sprintf("%s:%d", address, port),
		Password: password,
		DB:       db,
	}

	for _, opt := range opts {
		opt(options)
	}

	client := redis.NewClient(options)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		return nil, err
	}

	return client, nil
}

func MustConnect(address string, port, db int, password string, opts ...Option) *redis.Client {
	client, err := Connect(address, port, db, password, opts...)
	if err != nil {
		panic(err)
	}

	return client
}

// ShardConnect contain connection & it's key for shard client
type ShardConnect interface {
	Key() string
	Conditions() []string
	Client() redis.UniversalClient
	Close() error
}

type shardConnect struct {
	key        string
	conditions []string
	client     redis.UniversalClient
}

func newShardConnect(key string, conditions []string, client redis.UniversalClient) ShardConnect {
	return &shardConnect{
		key:        key,
		conditions: conditions,
		client:     client,
	}
}

// Key returns key of connection
func (conn *shardConnect) Key() string {
	return conn.key
}

// Client return single client
func (conn *shardConnect) Client() redis.UniversalClient {
	return conn.client
}

func (conn *shardConnect) Conditions() []string {
	if conn.conditions == nil {
		return []string{}
	}

	return conn.conditions
}

func (conn *shardConnect) Close() error {
	return conn.client.Close()
}

// ConnectShards connect all provided connections and create Connections object
func ConnectShards(connectionStrings []ShardConnectConfig, selector ClientSelector, options ...Option) (*Clients, error) {
	// validate for connection key unique and for empty
	// also, validate for empty connection string
	keys := make(map[string]struct{}, len(connectionStrings))
	for _, cs := range connectionStrings {
		if cs.Key == "" {
			return nil, errorx.New("Client key is empty")
		}

		if cs.Address == "" {
			return nil, errorx.
				New("Client address is empty").
				AddContext("key", cs.Key)
		}

		if cs.Port == 0 {
			return nil, errorx.
				New("Client port is zero").
				AddContext("key", cs.Key)
		}

		if _, ok := keys[cs.Key]; ok {
			return nil, errorx.
				New("Connection keys cannot duplicate").
				AddContext("key", cs.Key)
		}

		keys[cs.Key] = struct{}{}
	}

	// connect every shard
	connections := make([]ShardClient, len(connectionStrings))
	for idx, cs := range connectionStrings {
		connection, err := Connect(cs.Address, cs.Port, cs.DB, cs.Password, options...)
		if err != nil {
			return nil, err
		}

		connections[idx] = newShardConnect(cs.Key, cs.Conditions, connection)
	}

	return newClients(connections, selector), nil
}

// MustConnectShards calls ConnectShards and if error catch throws panic
func MustConnectShards(connectionStrings []ShardConnectConfig, selector ClientSelector, options ...Option) *Clients {
	connections, err := ConnectShards(connectionStrings, selector, options...)
	if err != nil {
		panic(err)
	}

	return connections
}

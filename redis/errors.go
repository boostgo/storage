package redis

import "github.com/boostgo/errorx"

var (
	ErrPing = errorx.New("redis.ping")

	ErrKeyEmpty = errorx.New("redis.empty_key")

	ErrClientAddressEmpty           = errorx.New("redis.client_address_empty")
	ErrClientPortZero               = errorx.New("redis.client_port_zero")
	ErrClientConnectionKeyDuplicate = errorx.New("redis.client_connection_key_duplicate")
	ErrClientKeyEmpty               = errorx.New("redis.client_key_empty")

	ErrKeyNotFound = errorx.New("redis.key_not_found").SetError(errorx.ErrNotFound)
	ErrInvalidKey  = errorx.New("redis.invalid_key")
)

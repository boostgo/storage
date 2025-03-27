package sql

import "context"

type Logger interface {
	Print(ctx context.Context, key, queryType, query string, args []any)
}

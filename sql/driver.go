package sql

import (
	"database/sql"
	"database/sql/driver"
	"slices"

	"github.com/jackc/pgx/v5/stdlib"
	"github.com/lib/pq"
	"github.com/mailru/go-clickhouse"
)

const (
	PqDriver  = "postgres"
	PgxDriver = "pgx"
	ChDriver  = "clickhouse"
)

func init() {
	// register drivers
	RegisterDriver("postgres", &pq.Driver{})
	RegisterDriver("pgx", stdlib.GetDefaultDriver())
	clickhouse.Map("") // call package clickhouse to register ch driver
}

// RegisterDriver sql package wrap for sql.Register function.
//
// For simple usage of common pkg names (sql)
func RegisterDriver(driverName string, driver driver.Driver) {
	if slices.Contains(sql.Drivers(), driverName) {
		return
	}

	sql.Register(driverName, driver)
}

package sql

import (
	"context"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

// Connect to the database.
//
// "options" can override default settings
func Connect(
	driverName, connectionString string,
	timeout time.Duration,
	options ...func(connection *sqlx.DB),
) (*sqlx.DB, error) {
	if driverName == "" {
		driverName = PqDriver
	}

	if driverName != PqDriver {
		connectionString = strings.ReplaceAll(connectionString, " binary_parameters=yes", "")
	}

	connection, err := sqlx.Open(driverName, connectionString)
	if err != nil {
		return nil, NewOpenConnectError(err, driverName, connectionString)
	}

	// set default settings
	connection.SetMaxOpenConns(10)
	connection.SetMaxIdleConns(10)
	connection.SetConnMaxLifetime(time.Second * 10)
	connection.SetConnMaxIdleTime(time.Second * 10)

	// apply options
	for _, option := range options {
		option(connection)
	}

	// make ping
	ctx := context.Background()
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	if err = connection.PingContext(ctx); err != nil {
		return nil, ErrPing.SetError(err)
	}

	return connection, nil
}

// MustConnect calls Connect and if err catch throws panic
func MustConnect(
	driverName, connectionString string,
	timeout time.Duration,
	options ...func(connection *sqlx.DB),
) *sqlx.DB {
	connection, err := Connect(driverName, connectionString, timeout, options...)
	if err != nil {
		panic(err)
	}

	return connection
}

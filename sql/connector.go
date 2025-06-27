package sql

import (
	"fmt"
	"os"
	"time"

	"github.com/jmoiron/sqlx"
)

// Connector helper for creating connection
type Connector struct {
	host             string
	port             int
	username         string
	password         string
	database         string
	schema           string
	binaryParameters bool
	writeTimeout     int
	readTimeout      int

	timeout time.Duration

	maxOpenConnections int
	maxIdleConnections int
	maxConnLifetime    time.Duration
	maxIdleTime        time.Duration
}

// NewConnector creates Connector object
func NewConnector() *Connector {
	const (
		defaultMaxOpenConnections = 10
		defaultMaxIdleConnections = 10
		defaultMaxConnLifetime    = time.Second * 10
		defaultMaxIdleTime        = time.Second * 10
	)

	return &Connector{
		maxOpenConnections: defaultMaxOpenConnections,
		maxIdleConnections: defaultMaxIdleConnections,
		maxConnLifetime:    defaultMaxConnLifetime,
		maxIdleTime:        defaultMaxIdleTime,
	}
}

// Host set host of database
func (connector *Connector) Host(host string) *Connector {
	connector.host = host
	return connector
}

// Port set port of database
func (connector *Connector) Port(port int) *Connector {
	connector.port = port
	return connector
}

// Username set username of database user
func (connector *Connector) Username(username string) *Connector {
	connector.username = username
	return connector
}

// Password set password of database user
func (connector *Connector) Password(password string) *Connector {
	connector.password = password
	return connector
}

// Database set database name
func (connector *Connector) Database(database string) *Connector {
	connector.database = database
	return connector
}

// Schema set schema name
func (connector *Connector) Schema(schema string) *Connector {
	connector.schema = schema
	return connector
}

// BinaryParameters set binary_parameters=yes param
func (connector *Connector) BinaryParameters(binaryParameters bool) *Connector {
	connector.binaryParameters = binaryParameters
	return connector
}

// ReadTimeout set readTimeout parameter.
//
// For clickhouse
func (connector *Connector) ReadTimeout(readTimeout int) *Connector {
	connector.readTimeout = readTimeout
	return connector
}

// WriteTimeout set writeTimeout parameter.
//
// For clickhouse
func (connector *Connector) WriteTimeout(writeTimeout int) *Connector {
	connector.writeTimeout = writeTimeout
	return connector
}

// Timeout set timeout for connect & ping (via [context.Context])
func (connector *Connector) Timeout(timeout time.Duration) *Connector {
	connector.timeout = timeout
	return connector
}

// MaxOpenConnections set max open connections option for connection setting
func (connector *Connector) MaxOpenConnections(maxOpenConnections int) *Connector {
	connector.maxOpenConnections = maxOpenConnections
	return connector
}

// MaxIdleConnections set max idle connections option for connection setting
func (connector *Connector) MaxIdleConnections(maxIdleConnections int) *Connector {
	connector.maxIdleConnections = maxIdleConnections
	return connector
}

// MaxIdleTime set max idle time option for connection setting
func (connector *Connector) MaxIdleTime(maxIdleTime time.Duration) *Connector {
	connector.maxIdleTime = maxIdleTime
	return connector
}

// ConnectionMaxLifetime set max connection lifetime option for connection setting
func (connector *Connector) ConnectionMaxLifetime(connectionMaxLifetime time.Duration) *Connector {
	connector.maxConnLifetime = connectionMaxLifetime
	return connector
}

// Build connection string
func (connector *Connector) Build() string {
	var binaryParameters string
	if connector.binaryParameters {
		binaryParameters = " binary_parameters=yes"
	}

	var schema string
	if connector.schema != "" {
		schema = " search_path=" + connector.schema
	}

	return fmt.Sprintf(
		"host=%s port=%d user=%s password=%s dbname=%s sslmode=disable%s%s",
		connector.host, connector.port,
		connector.username, connector.password,
		connector.database,
		binaryParameters,
		schema,
	)
}

func (connector *Connector) BuildClickhouse() string {
	scheme := "https"
	if os.Getenv("LOCAL") == "true" {
		scheme = "http"
	}

	args := make([]any, 0)
	args = append(
		args,
		scheme, connector.username, connector.password,
		connector.host, connector.port,
		connector.database,
	)

	template := "%s://%s:%s@%s:%d/%s?"

	if connector.readTimeout > 0 {
		template += "read_timeout=%ds"
		args = append(args, connector.readTimeout)
	}

	if connector.writeTimeout > 0 {
		prefix := ""
		if connector.readTimeout > 0 {
			prefix = "&"
		}

		template += prefix + "write_timeout=%ds"
		args = append(args, connector.writeTimeout)
	}

	return fmt.Sprintf(template, args...)
}

// String calls Build method
func (connector *Connector) String() string {
	return connector.Build()
}

// Connect calls Build method and call Connect function
func (connector *Connector) Connect(
	driverName string,
	options ...func(connection *sqlx.DB),
) (*sqlx.DB, error) {
	options = append(
		options,
		MaxConnectionsOption(connector.maxOpenConnections, connector.maxIdleConnections),
		MaxTimeOption(connector.maxConnLifetime, connector.maxIdleTime),
	)

	connectionString := connector.Build()
	if driverName == ChDriver {
		connectionString = connector.BuildClickhouse()
	}

	return Connect(
		driverName,
		connectionString,
		connector.timeout,
		options...,
	)
}

// MustConnect calls MustConnect function
func (connector *Connector) MustConnect(
	driverName string,
	options ...func(connection *sqlx.DB),
) *sqlx.DB {
	connectionString := connector.Build()
	if driverName == ChDriver {
		connectionString = connector.BuildClickhouse()
	}

	return MustConnect(
		driverName,
		connectionString,
		connector.timeout,
		options...,
	)
}

// ConnectionString returns built connection string by provided params for sqlx lib
func ConnectionString(
	host string,
	port int,
	username, password string,
	database string,
	binaryParameters bool,
) string {
	return NewConnector().
		Host(host).
		Port(port).
		Username(username).
		Password(password).
		Database(database).
		BinaryParameters(binaryParameters).
		String()
}

// MaxConnectionsOption sets max open & idle connections
func MaxConnectionsOption(open, idle int) func(conn *sqlx.DB) {
	return func(conn *sqlx.DB) {
		if open == 0 || idle == 0 {
			return
		}

		conn.SetMaxOpenConns(open)
		conn.SetMaxIdleConns(idle)
	}
}

// MaxTimeOption sets connection max lifetime & max idle time settings
func MaxTimeOption(lifetime, idle time.Duration) func(conn *sqlx.DB) {
	return func(conn *sqlx.DB) {
		if lifetime == 0 || idle == 0 {
			return
		}

		conn.SetConnMaxLifetime(lifetime)
		conn.SetConnMaxIdleTime(idle)
	}
}

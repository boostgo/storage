package sql

import "github.com/boostgo/errorx"

var (
	ErrOpenConnect = errorx.New("sql.open_connect")
	ErrPing        = errorx.New("sql.ping")

	ErrConnectionStringEmpty     = errorx.New("sql.connection_string_empty")
	ErrConnectionStringDuplicate = errorx.New("sql.connection_string_duplicate")
	ErrConnectionIsNotShard      = errorx.New("sql.connection_is_not_shard")

	ErrMethodNotSuppoertedInSingle = errorx.New("sql.method_not_suppoerted_in_single")

	ErrMigrateOpenConn          = errorx.New("migrate.open_conn")
	ErrMigrateGetDriver         = errorx.New("migrate.get_driver")
	ErrMigrateLock              = errorx.New("migrate.lock")
	ErrMigrateReadMigrationsDir = errorx.New("migrate.read_migrations_dir")
	ErrMigrateUp                = errorx.New("migrate.up")

	ErrTransactorBegin    = errorx.New("transactor.begin")
	ErrTransactorCommit   = errorx.New("transactor.commit")
	ErrTransactorRollback = errorx.New("transactor.rollback")
)

type openConnectContext struct {
	Driver           string `json:"driver"`
	ConnectionString string `json:"connection_string"`
}

func NewOpenConnectError(err error, driver, connectionString string) error {
	return ErrOpenConnect.
		SetError(err).
		SetData(openConnectContext{
			Driver:           driver,
			ConnectionString: connectionString,
		})
}

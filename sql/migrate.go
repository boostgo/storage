package sql

import (
	"context"
	"errors"

	"github.com/boostgo/log"
	"github.com/golang-migrate/migrate/v4"
	"github.com/golang-migrate/migrate/v4/database/postgres"
	"github.com/jmoiron/sqlx"

	_ "github.com/golang-migrate/migrate/v4/source/file"
	_ "github.com/lib/pq"
)

// Migrate runs migration by provided connection & database name.
//
// Use by default ./migrations directory in the root of project.
func Migrate(ctx context.Context, conn *sqlx.DB, databaseName string, migrationsDir ...string) (err error) {
	nativeConn, err := conn.Conn(ctx)
	if err != nil {
		return ErrMigrateOpenConn.SetError(err)
	}

	driver, err := postgres.WithConnection(ctx, nativeConn, &postgres.Config{})
	if err != nil {
		return ErrMigrateGetDriver.SetError(err)
	}

	_, err = nativeConn.ExecContext(ctx, "SET lock_timeout = '60s';")
	if err != nil {
		return ErrMigrateLock.SetError(err)
	}

	const defaultMigrationsDir = "./migrations"
	migrationsDirectoryPath := defaultMigrationsDir
	if len(migrationsDir) > 0 {
		migrationsDirectoryPath = migrationsDir[0]
	}

	migrator, err := migrate.NewWithDatabaseInstance("file://"+migrationsDirectoryPath, databaseName, driver)
	if err != nil {
		return ErrMigrateReadMigrationsDir.SetError(err)
	}
	defer migrator.Close()

	if err = migrator.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			log.
				Info().
				Ctx(ctx).
				Str("database_name", databaseName).
				Msg("Migrate no change")
			return nil
		}

		return ErrMigrateUp.SetError(err)
	}

	return nil
}

// MustMigrate calls Migrate function and if error catch throws panic
func MustMigrate(ctx context.Context, conn *sqlx.DB, databaseName string) {
	if err := Migrate(ctx, conn, databaseName); err != nil {
		panic(err)
	}
}

// BackgroundMigrate calls Migrate function and if error catch print log
func BackgroundMigrate(ctx context.Context, conn *sqlx.DB, databaseName string) {
	if err := Migrate(ctx, conn, databaseName); err != nil {
		log.
			Error().
			Ctx(ctx).
			Err(err).
			Str("database_name", databaseName).
			Msg("Migration failed")
	}
}

func AsyncMigrate(ctx context.Context, conn *sqlx.DB, databaseName string) {
	go BackgroundMigrate(ctx, conn, databaseName)
}

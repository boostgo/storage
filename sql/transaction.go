package sql

import (
	"context"

	"github.com/boostgo/errorx"
	"github.com/jmoiron/sqlx"
)

const transactionKey = "storage_sql_tx"

// SetTx sets transaction key to new context
func SetTx(ctx context.Context, tx *sqlx.Tx) context.Context {
	return context.WithValue(ctx, transactionKey, tx)
}

// GetTx returns sql transaction object from context if it exist
func GetTx(ctx context.Context) (*sqlx.Tx, bool) {
	transaction := ctx.Value(transactionKey)
	if transaction == nil {
		return nil, false
	}

	tx, ok := transaction.(*sqlx.Tx)
	return tx, ok
}

// Transaction run "actions" by created transaction object
func Transaction(conn *sqlx.DB, transactionActions func(tx *sqlx.Tx) error) error {
	transaction, err := conn.Beginx()
	if err != nil {
		return err
	}
	defer func() {
		_ = transaction.Rollback()
	}()

	if err = transactionActions(transaction); err != nil {
		return err
	}

	return transaction.Commit()
}

func Atomic(ctx context.Context, conn *sqlx.DB, fn func(ctx context.Context) error) error {
	var tx *sqlx.Tx
	var ok bool
	var err error

	tx, ok = GetTx(ctx)
	if ok {
		return errorx.Try(func() error {
			return fn(ctx)
		})
	}

	tx, err = conn.Beginx()
	if err != nil {
		return err
	}

	defer func() {
		if err != nil {
			_ = tx.Rollback()
			return
		}

		_ = tx.Commit()
	}()

	return errorx.Try(func() error {
		return fn(context.WithValue(ctx, transactionKey, tx))
	})
}

package sql

import (
	"context"
	"database/sql"

	"github.com/boostgo/storage"
	"github.com/jmoiron/sqlx"
)

// TransactorConnectionProvider sql transaction interface
type TransactorConnectionProvider interface {
	BeginTxx(ctx context.Context, opts *sql.TxOptions) (*sqlx.Tx, error)
}

type sqlTransactor struct {
	provider TransactorConnectionProvider
}

// NewTransactor creates SQL transactor
func NewTransactor(provider TransactorConnectionProvider) storage.Transactor {
	return &sqlTransactor{
		provider: provider,
	}
}

func (st *sqlTransactor) Key() string {
	return transactionKey
}

func (st *sqlTransactor) Begin(ctx context.Context) (storage.Transaction, error) {
	tx, err := st.provider.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return nil, ErrTransactorBegin.SetError(err)
	}

	return newTransactorTx(ctx, tx), nil
}

func (st *sqlTransactor) BeginCtx(ctx context.Context) (context.Context, error) {
	tx, err := st.provider.BeginTxx(ctx, &sql.TxOptions{
		Isolation: sql.LevelReadCommitted,
		ReadOnly:  false,
	})
	if err != nil {
		return nil, ErrTransactorBegin.SetError(err)
	}

	return SetTx(ctx, tx), nil
}

func (st *sqlTransactor) CommitCtx(ctx context.Context) error {
	tx, ok := GetTx(ctx)
	if !ok {
		return nil
	}

	if err := tx.Commit(); err != nil {
		return ErrTransactorCommit.SetError(err)
	}

	return nil
}

func (st *sqlTransactor) RollbackCtx(ctx context.Context) error {
	tx, ok := GetTx(ctx)
	if !ok {
		return nil
	}

	if err := tx.Rollback(); err != nil {
		return ErrTransactorRollback.SetError(err)
	}

	return nil
}

func (st *sqlTransactor) IsTx(ctx context.Context) bool {
	if ctx == nil {
		return false
	}

	_, ok := GetTx(ctx)
	return ok
}

func (st *sqlTransactor) TryCommit(ctx context.Context, err *error) {
	if err != nil {
		_ = st.RollbackCtx(ctx)
		return
	}

	_ = st.CommitCtx(ctx)
}

type sqlTransaction struct {
	tx        *sqlx.Tx
	parentCtx context.Context
}

func newTransactorTx(ctx context.Context, tx *sqlx.Tx) storage.Transaction {
	return &sqlTransaction{
		tx:        tx,
		parentCtx: ctx,
	}
}

func (tx *sqlTransaction) Commit(_ context.Context) error {
	return tx.tx.Commit()
}

func (tx *sqlTransaction) Rollback(_ context.Context) error {
	return tx.tx.Rollback()
}

func (tx *sqlTransaction) Context() context.Context {
	return SetTx(tx.parentCtx, tx.tx)
}

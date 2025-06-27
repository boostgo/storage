package storage

import (
	"github.com/boostgo/errorx"
)

// ErrConnNotSelected returns if "shard client" does not choose connection to use
var ErrConnNotSelected = errorx.New("sql.connection_not_selected")

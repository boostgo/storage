// Package sql provide tools for manipulating connections and helper tools (sqlx extension).
// Features:
// - Client for single database and for sharding (common interface - DB).
// - More simple connecting.
// - Arguments tool. Helps to set arguments for multiple insert.
// - Connection builder. Building connection string or connecting.
// - Any driver support.
// - Migrations.
// - Transactor implementation. Implementation based on manipulating transaction from context.
package sql

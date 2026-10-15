//! SQLite-compatible SQL backed by broker-owned durable files.
//! The reusable adapter is in the `dekopon-turso-sql-io` package.
use dekopon_turso_sql_io as io;

mod component;
pub use component::{Args, Exec, ExecError, ExecInput, TursoSqlProvider};

//! SQLite-compatible SQL backed by broker-owned durable files.
//! The reusable adapter is in the standalone `dekopon-turso-sql-io` package.
pub use dekopon_turso_sql_io as io;

#[cfg(feature = "component")]
mod component;
#[cfg(feature = "component")]
pub use component::{Args, Exec, ExecError, ExecInput, TursoSqlProvider};

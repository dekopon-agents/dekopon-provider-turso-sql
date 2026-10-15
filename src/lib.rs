//! SQLite-compatible SQL backed by broker-owned durable files.
//! The component is optional for consumers linking only the shared storage adapter.
pub mod io;

#[cfg(feature = "component")]
mod component;
#[cfg(feature = "component")]
pub use component::{Args, Exec, ExecError, ExecInput, TursoSqlProvider};

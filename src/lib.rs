//! SQLite-compatible SQL for Dekopon providers, backed by broker-owned durable
//! files.
//!
//! The engine is `turso_core` — a pure-Rust SQLite rewrite — compiled to
//! `wasm32-unknown-unknown` and reaching storage, entropy, and both clocks only
//! through `dekopon:storage/durable-files@0.1.0`. The generated component
//! imports that interface and nothing else.
//!
//! Turso is WAL-only; it does not implement a rollback journal and never
//! requests shared memory on this target, so the host's five-level lock ladder
//! is available but unused. The database and its write-ahead log are two
//! ordinary durable files committed together by one invocation transaction.

mod io;

use std::sync::Arc;

use dekopon_provider_sdk::{
    CapabilityId, CommandInvocation, EffectKind, Idempotency, Provider, ProviderApiVersion,
    ProviderCapability, ProviderError, ProviderManifest, RiskLevel,
};
use serde_json::{Map, Value, json};
use turso_core::{Connection, Database, IO, SqliteDialect, StepResult, Value as SqlValue};

mod bindings {
    wit_bindgen::generate!({
        path: "wit",
        world: "provider",
        generate_all,
        pub_export_macro: true,
    });
}

/// The single database file in the invocation's namespace. Turso opens exactly
/// this and its `-wal` sidecar.
const DATABASE: &str = "main.db";

/// 64 KiB pages keep a page read comfortably inside the host's 256 KiB
/// `max_read_bytes_per_call` while minimising host calls per query. Only takes
/// effect before the first table is created.
const PAGE_SIZE: u32 = 65_536;

/// Turso's own wasm default is 100,000 pages — 6.4 GiB at this page size, which
/// traps on allocation rather than evicting. 256 pages is 16 MiB, the host's
/// `max_file_bytes`, so the cache can hold the largest legal database.
const CACHE_PAGES: u32 = 256;

/// Statements this provider refuses. `VACUUM INTO` opens its destination on a
/// hardcoded platform IO rather than the injected one, and plain `VACUUM`
/// rewrites the whole file inside a single invocation's byte budget.
const REFUSED: [&str; 1] = ["vacuum"];

struct TursoSqlProvider;

impl Provider for TursoSqlProvider {
    fn manifest() -> ProviderManifest {
        ProviderManifest {
            api_version: ProviderApiVersion::V1Alpha1,
            id: "turso".parse().expect("static provider identifier"),
            description: "SQLite-compatible SQL over broker-owned durable files".to_owned(),
            command_words: vec!["turso".to_owned()],
            capabilities: vec![ProviderCapability {
                id: "turso.exec".parse().expect("static capability identifier"),
                description: "Executes SQL statements against the namespace database".to_owned(),
                effect: EffectKind::LocalWrite,
                risk: RiskLevel::Medium,
                idempotency: Idempotency::Conditional,
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "statements": {
                            "type": "array",
                            "items": {"type": "string"},
                            "minItems": 1
                        }
                    },
                    "required": ["statements"],
                    "additionalProperties": false
                }),
            }],
        }
    }

    fn invoke(capability: &CapabilityId, input: Value) -> Result<Value, ProviderError> {
        if capability.as_str() != "turso.exec" {
            return Err(failure("invalid-input", "unknown capability"));
        }
        exec(input)
    }

    fn resolve_command(argv: &[String]) -> Result<CommandInvocation, ProviderError> {
        if argv.is_empty() {
            return Err(failure("invalid-input", "expected at least one statement"));
        }
        Ok(CommandInvocation {
            capability: "turso.exec".parse().expect("static capability identifier"),
            input: json!({"statements": argv}),
        })
    }
}

fn exec(input: Value) -> Result<Value, ProviderError> {
    io::trace_reset();
    let statements = statements_of(&input)?;

    let engine: Arc<dyn IO> = Arc::new(io::DekoponIo::new());
    let database = Database::open_file(Arc::clone(&engine), DATABASE, Arc::new(SqliteDialect))
        .map_err(|error| failure("open", &error.to_string()))?;
    let connection = database
        .connect()
        .map_err(|error| failure("connect", &error.to_string()))?;

    // Both pragmas are corrections to defaults that are wrong for this host,
    // and both must precede any schema statement.
    run(
        &connection,
        &engine,
        &format!("PRAGMA page_size = {PAGE_SIZE}"),
    )?;
    run(
        &connection,
        &engine,
        &format!("PRAGMA cache_size = {CACHE_PAGES}"),
    )?;

    let mut results = Vec::with_capacity(statements.len());
    for sql in &statements {
        results.push(run(&connection, &engine, sql)?);
    }

    // Turso never checkpoints on its own. Left alone the write-ahead log grows
    // without bound, and once it passes the host's `max_read_bytes_per_call`
    // every later invocation — including a pure read — fails with a terminal
    // quota error and the namespace becomes unreadable. Truncating here holds
    // it at zero bytes across the invocation boundary.
    run(&connection, &engine, "PRAGMA wal_checkpoint(TRUNCATE)")
        .map_err(|error| failure("checkpoint", error.message()))?;

    let trace = io::trace_snapshot();
    Ok(json!({
        "results": results,
        "storage": {
            "opened": trace.opens,
            "hostCalls": {
                "open": trace.open_calls,
                "readAt": trace.read_at,
                "writeAt": trace.write_at,
                "sync": trace.sync,
                "truncate": trace.truncate,
                "size": trace.size,
                "lock": trace.lock,
                "unlock": trace.unlock,
                "remove": trace.remove,
                "stat": trace.stat,
                "randomBytes": trace.random,
                "monotonicTimeNs": trace.monotonic,
                "wallTimeMs": trace.wall,
            },
            "readBytes": trace.read_bytes,
            "writeBytes": trace.write_bytes,
            "shortReads": trace.short_reads,
            "zeroFilledBytes": trace.zero_filled_bytes,
        }
    }))
}

fn statements_of(input: &Value) -> Result<Vec<String>, ProviderError> {
    let object = input
        .as_object()
        .ok_or_else(|| failure("invalid-input", "expected an object"))?;
    let array = object
        .get("statements")
        .and_then(Value::as_array)
        .ok_or_else(|| failure("invalid-input", "expected a statements array"))?;
    if array.is_empty() {
        return Err(failure("invalid-input", "expected at least one statement"));
    }
    let mut statements = Vec::with_capacity(array.len());
    for value in array {
        let sql = value
            .as_str()
            .ok_or_else(|| failure("invalid-input", "expected a statement string"))?;
        let leading = sql
            .trim_start()
            .split(|character: char| character.is_whitespace() || character == '(')
            .next()
            .unwrap_or_default()
            .to_ascii_lowercase();
        if REFUSED.contains(&leading.as_str()) {
            return Err(failure("refused", &format!("{leading} is not permitted")));
        }
        statements.push(sql.to_owned());
    }
    Ok(statements)
}

fn run(
    connection: &Arc<Connection>,
    engine: &Arc<dyn IO>,
    sql: &str,
) -> Result<Value, ProviderError> {
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| failure("prepare", &error.to_string()))?;
    let mut rows: Vec<Value> = Vec::new();
    loop {
        match statement
            .step()
            .map_err(|error| failure("step", &error.to_string()))?
        {
            StepResult::Row => {
                let row = statement
                    .row()
                    .ok_or_else(|| failure("step", "row signalled but absent"))?;
                rows.push(Value::Array(row.get_values().map(sql_to_json).collect()));
            }
            // Every host call completes inline, so this only ever drains
            // already-finished completions.
            StepResult::IO => engine
                .step()
                .map_err(|error| failure("io", &error.to_string()))?,
            StepResult::Done => break,
            StepResult::Interrupt => return Err(failure("interrupted", sql)),
            StepResult::Busy => return Err(failure("busy", sql)),
            _ => {}
        }
    }
    let mut object = Map::new();
    object.insert("sql".to_owned(), Value::String(sql.to_owned()));
    object.insert("rows".to_owned(), Value::Array(rows));
    Ok(Value::Object(object))
}

fn sql_to_json(value: &SqlValue) -> Value {
    match value {
        SqlValue::Null => Value::Null,
        SqlValue::Numeric(numeric) => match numeric {
            turso_core::Numeric::Integer(integer) => json!(integer),
            turso_core::Numeric::Float(float) => json!(f64::from(*float)),
        },
        SqlValue::Text(text) => Value::String(text.as_str().to_owned()),
        SqlValue::Blob(blob) => json!({"blob": blob.as_slice().len()}),
    }
}

fn failure(code: &str, detail: &str) -> ProviderError {
    ProviderError::new(code, detail)
}

dekopon_provider_sdk::export_provider_with_commands!(TursoSqlProvider, bindings);

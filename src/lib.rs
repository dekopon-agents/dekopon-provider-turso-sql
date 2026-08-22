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

#[cfg(test)]
mod tests {
    use super::*;

    fn statements(values: &[&str]) -> Value {
        json!({"statements": values})
    }

    #[test]
    fn accepts_an_ordered_statement_array() {
        let parsed = statements_of(&statements(&["CREATE TABLE t(a)", "SELECT 1"]))
            .expect("a plain statement array is accepted");
        assert_eq!(parsed, vec!["CREATE TABLE t(a)", "SELECT 1"]);
    }

    #[test]
    fn refuses_vacuum_in_any_casing_or_form() {
        // `VACUUM INTO` is the dangerous one — it opens its destination on a hardcoded platform
        // IO rather than the injected one — but plain `VACUUM` also rewrites the whole file
        // inside a single invocation's byte budget. Both are refused at the same boundary.
        for sql in [
            "VACUUM",
            "vacuum",
            "VaCuUm",
            "  \t VACUUM ",
            "VACUUM INTO 'copy.db'",
            "vacuum(1)",
        ] {
            let error =
                statements_of(&statements(&[sql])).expect_err("vacuum in any form is refused");
            assert_eq!(error.code(), "refused", "{sql}");
        }
    }

    #[test]
    fn a_refusal_names_the_statement_that_caused_it() {
        let error = statements_of(&statements(&["SELECT 1", "VACUUM"]))
            .expect_err("the second statement is refused");
        assert_eq!(error.code(), "refused");
        assert!(error.message().contains("vacuum"), "{}", error.message());
    }

    /// Documents a known limitation rather than asserting a desirable behavior.
    ///
    /// The refusal check reads the first whitespace- or `(`-delimited token, so a leading SQL
    /// comment hides the keyword from it. This is a robustness gap, not a sandbox escape: the
    /// statement reaches the engine and fails there, and the capability is confined to its own
    /// namespace either way. Tightening it means writing a comment stripper, which changes the
    /// shipped component's bytes; that is a deliberate decision, not a drive-by fix.
    #[test]
    fn a_leading_block_comment_currently_evades_the_refusal_check() {
        let parsed = statements_of(&statements(&["/* hidden */ VACUUM"]))
            .expect("the leading-token check does not see past a comment");
        assert_eq!(parsed, vec!["/* hidden */ VACUUM"]);
    }

    #[test]
    fn rejects_input_that_is_not_a_statement_array() {
        let cases = [
            json!([]),
            json!({}),
            json!({"statements": []}),
            json!({"statements": "SELECT 1"}),
            json!({"statements": [1]}),
        ];
        for input in cases {
            let error = statements_of(&input).expect_err(&format!("{input} is invalid"));
            assert_eq!(error.code(), "invalid-input", "{input}");
        }
    }

    #[test]
    fn maps_every_sql_value_kind_to_json() {
        assert_eq!(sql_to_json(&SqlValue::Null), Value::Null);
        assert_eq!(
            sql_to_json(&SqlValue::Numeric(turso_core::Numeric::Integer(7))),
            json!(7)
        );
        assert_eq!(sql_to_json(&SqlValue::build_text("hello")), json!("hello"));
    }

    #[test]
    fn a_blob_degrades_to_its_length_and_cannot_round_trip() {
        // Binary is deliberately not returned: the capability's output is model-facing JSON, and
        // a caller that needs bytes back has to encode them itself. Pinned so the lossiness is a
        // decision rather than a surprise.
        let blob = sql_to_json(&SqlValue::from_slice(&[1, 2, 3]).expect("small blob allocates"));
        assert_eq!(blob, json!({"blob": 3}));
    }

    #[test]
    fn resolve_command_rewrites_argv_into_one_proposal() {
        let resolved =
            TursoSqlProvider::resolve_command(&["SELECT 1".to_owned(), "SELECT 2".to_owned()])
                .expect("argv becomes a proposal");
        assert_eq!(resolved.capability.as_str(), "turso.exec");
        assert_eq!(
            resolved.input,
            json!({"statements": ["SELECT 1", "SELECT 2"]})
        );
    }

    #[test]
    fn resolve_command_refuses_empty_argv() {
        let error = TursoSqlProvider::resolve_command(&[]).expect_err("no statement to run");
        assert_eq!(error.code(), "invalid-input");
    }

    #[test]
    fn the_manifest_declares_exactly_one_local_write_capability() {
        let manifest = TursoSqlProvider::manifest();
        assert_eq!(manifest.id.as_str(), "turso");
        assert_eq!(manifest.command_words, vec!["turso".to_owned()]);
        assert_eq!(manifest.capabilities.len(), 1);

        let capability = &manifest.capabilities[0];
        assert_eq!(capability.id.as_str(), "turso.exec");
        assert_eq!(capability.effect, EffectKind::LocalWrite);
        assert_eq!(capability.input_schema["required"], json!(["statements"]));
        assert_eq!(
            capability.input_schema["additionalProperties"],
            json!(false)
        );
        assert_eq!(
            capability.input_schema["properties"]["statements"]["minItems"],
            json!(1)
        );
    }

    #[test]
    fn an_unknown_capability_is_refused_before_any_host_call() {
        // Reached without touching `io`, whose bindings are `unreachable!()` off wasm32 — which is
        // exactly why this is the only `invoke` path a native unit test can exercise.
        let error = TursoSqlProvider::invoke(
            &"turso.nope".parse().expect("valid capability"),
            json!({"statements": ["SELECT 1"]}),
        )
        .expect_err("only turso.exec is declared");
        assert_eq!(error.code(), "invalid-input");
    }
}

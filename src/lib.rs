//! SQLite-compatible SQL backed by broker-owned durable files. Storage, entropy and
//! both clocks are reached only through the SDK's durable-files import; stdin and
//! stdout are broker-owned streams. No WASI or ambient filesystem access.
mod io;

use clap::Parser;
use dekopon_provider_sdk::provider::{
    self, Capability, Code, DurableFiles, Failure, Proposal, Provider, Stdout, Storage, Usage,
};
use dekopon_provider_sdk::{EffectKind, RiskLevel};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    fmt,
    io::{Read, Write},
    sync::Arc,
};
use turso_core::{Connection, Database, IO, SqliteDialect, StepResult, Value as SqlValue};

const DATABASE: &str = "main.db";
const PAGE_SIZE: u32 = 65_536;
const CACHE_PAGES: u32 = 256;
const REFUSED: [&str; 1] = ["vacuum"];
const MAX_STDIN_BYTES: u64 = 1024 * 1024;

#[derive(Parser)]
#[command(
    name = "turso",
    about = "Run SQL against the namespace database",
    after_help = "One argument is one statement. `turso -` runs one piped statement. Wrap bulk writes in BEGIN/COMMIT."
)]
pub struct Args {
    #[arg(value_name = "STATEMENT", required = true)]
    statements: Vec<String>,
}

#[derive(Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct ExecInput {
    #[schemars(length(min = 0))]
    statements: Vec<String>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    stdin_statement: bool,
}

#[derive(Debug)]
pub struct ExecError {
    code: Code,
    message: String,
}
impl fmt::Display for ExecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}
impl Failure for ExecError {
    fn code(&self) -> Code {
        self.code
    }
}
fn failure(code: &'static str, detail: &str) -> ExecError {
    ExecError {
        code: if code == "usage" {
            Code::USAGE
        } else {
            Code::new(code)
        },
        message: detail.to_owned(),
    }
}

pub struct TursoSqlProvider;
pub struct Exec;
impl Provider for TursoSqlProvider {
    const ID: &'static str = "turso";
    const COMMAND_WORDS: &'static [&'static str] = &["turso"];
    const DESCRIPTION: &'static str = "SQLite-compatible SQL over broker-owned durable files";
    type Args = Args;
    type Capabilities = (Exec,);
    fn propose(args: Args, stdin_piped: bool) -> Result<Proposal<Self>, Usage> {
        let piped = args.statements.len() == 1 && args.statements[0] == "-";
        if piped && !stdin_piped {
            return Err(Usage::new("turso -: nothing was piped in"));
        }
        Ok(Proposal::to::<Exec>(ExecInput {
            statements: if piped { vec![] } else { args.statements },
            stdin_statement: piped,
        }))
    }
}
impl Capability for Exec {
    type Provider = TursoSqlProvider;
    const NAME: &'static str = "exec";
    const DESCRIPTION: &'static str = "Executes SQL statements against the namespace database";
    const EFFECT: EffectKind = EffectKind::LocalWrite;
    const RISK: RiskLevel = RiskLevel::Medium;
    type Input = ExecInput;
    type Needs = Storage<DurableFiles>;
    type Error = ExecError;
    fn run(
        mut input: ExecInput,
        storage: Storage<DurableFiles>,
        out: &mut Stdout,
    ) -> Result<(), ExecError> {
        if input.stdin_statement {
            if !input.statements.is_empty() {
                return Err(failure(
                    "invalid-input",
                    "stdin marker cannot carry statements",
                ));
            }
            let mut statement = String::new();
            provider::stdin()
                .ok_or_else(|| failure("usage", "turso -: nothing was piped in"))?
                .take(MAX_STDIN_BYTES + 1)
                .read_to_string(&mut statement)
                .map_err(|_| failure("invalid-input", "piped SQL must be UTF-8"))?;
            if statement.len() as u64 > MAX_STDIN_BYTES {
                return Err(failure("invalid-input", "piped SQL exceeds limit"));
            }
            if statement.trim().is_empty() {
                return Err(failure("usage", "turso -: nothing was piped in"));
            }
            input.statements.push(statement);
        }
        io::with_storage(storage, || exec(input, out))
    }
}

fn exec(input: ExecInput, out: &mut Stdout) -> Result<(), ExecError> {
    io::trace_reset();
    let statements = statements_of(&input)?;
    let engine: Arc<dyn IO> = Arc::new(io::DekoponIo::new());
    let database = Database::open_file(Arc::clone(&engine), DATABASE, Arc::new(SqliteDialect))
        .map_err(|error| failure("open", &error.to_string()))?;
    let connection = database
        .connect()
        .map_err(|error| failure("connect", &error.to_string()))?;
    run(
        &connection,
        &engine,
        &format!("PRAGMA page_size = {PAGE_SIZE}"),
        None,
    )?;
    run(
        &connection,
        &engine,
        &format!("PRAGMA cache_size = {CACHE_PAGES}"),
        None,
    )?;
    write(out, b"{\"results\":[")?;
    for (index, sql) in statements.iter().enumerate() {
        if index != 0 {
            write(out, b",")?;
        }
        run(&connection, &engine, sql, Some(out))?;
    }
    // Without this checkpoint, the WAL eventually exceeds the host read quota and
    // permanently renders even SELECT unusable after a series of writes.
    run(
        &connection,
        &engine,
        "PRAGMA wal_checkpoint(TRUNCATE)",
        None,
    )
    .map_err(|error| failure("checkpoint", &error.message))?;
    let trace = io::trace_snapshot();
    write(out, b"],\"storage\":")?;
    serde_json::to_writer(
        &mut *out,
        &json!({
                "opened": trace.opens,
                "hostCalls": {
                    "open": trace.open_calls, "readAt": trace.read_at,
                    "writeAt": trace.write_at, "sync": trace.sync,
                    "truncate": trace.truncate, "size": trace.size,
                    "remove": trace.remove, "stat": trace.stat,
                    "randomBytes": trace.random, "monotonicTimeNs": trace.monotonic,
                    "wallTimeMs": trace.wall,
                },
                "readBytes": trace.read_bytes, "writeBytes": trace.write_bytes,
                "shortReads": trace.short_reads, "zeroFilledBytes": trace.zero_filled_bytes,
        }),
    )
    .map_err(|_| failure("output", "cannot write SQL results"))?;
    write(out, b"}")
}
fn write(out: &mut Stdout, bytes: &[u8]) -> Result<(), ExecError> {
    out.write_all(bytes)
        .map_err(|_| failure("output", "cannot write SQL results"))
}

fn statements_of(input: &ExecInput) -> Result<Vec<String>, ExecError> {
    if input.statements.is_empty() {
        return Err(failure("invalid-input", "expected at least one statement"));
    }
    let mut statements = Vec::with_capacity(input.statements.len());
    for sql in &input.statements {
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
    mut out: Option<&mut Stdout>,
) -> Result<(), ExecError> {
    let mut statement = connection
        .prepare(sql)
        .map_err(|error| failure("prepare", &error.to_string()))?;
    if let Some(ref mut out) = out {
        write(out, b"{\"sql\":")?;
        serde_json::to_writer(&mut **out, sql)
            .map_err(|_| failure("output", "cannot write SQL results"))?;
        write(out, b",\"rows\":[")?;
    }
    let mut first = true;
    loop {
        match statement
            .step()
            .map_err(|error| failure("step", &error.to_string()))?
        {
            StepResult::Row => {
                let row = statement
                    .row()
                    .ok_or_else(|| failure("step", "row signalled but absent"))?;
                if let Some(ref mut out) = out {
                    if !first {
                        write(out, b",")?;
                    }
                    first = false;
                    serde_json::to_writer(
                        &mut **out,
                        &row.get_values().map(sql_to_json).collect::<Vec<_>>(),
                    )
                    .map_err(|_| failure("output", "cannot write SQL results"))?;
                }
            }
            StepResult::IO => engine
                .step()
                .map_err(|error| failure("io", &error.to_string()))?,
            StepResult::Done => break,
            StepResult::Interrupt => return Err(failure("interrupted", sql)),
            StepResult::Busy => return Err(failure("busy", sql)),
            _ => {}
        }
    }
    if let Some(out) = out {
        write(out, b"]}")?;
    }
    Ok(())
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

dekopon_provider_sdk::export!(TursoSqlProvider);

#[cfg(test)]
mod tests {
    use super::*;
    fn input(values: &[&str]) -> ExecInput {
        ExecInput {
            statements: values.iter().map(|s| (*s).into()).collect(),
            stdin_statement: false,
        }
    }
    #[test]
    fn accepts_an_ordered_statement_array() {
        assert_eq!(
            statements_of(&input(&["CREATE TABLE t(a)", "SELECT 1"])).unwrap(),
            ["CREATE TABLE t(a)", "SELECT 1"]
        );
    }
    #[test]
    fn refuses_vacuum_in_any_casing_or_form() {
        for sql in [
            "VACUUM",
            "vacuum",
            "VaCuUm",
            "  \t VACUUM ",
            "VACUUM INTO 'copy.db'",
            "vacuum(1)",
        ] {
            assert_eq!(
                statements_of(&input(&[sql])).unwrap_err().code().as_str(),
                "refused"
            );
        }
        assert!(
            statements_of(&input(&["SELECT 1", "VACUUM"]))
                .unwrap_err()
                .to_string()
                .contains("vacuum")
        );
    }
    #[test]
    fn leading_comment_limitation_is_preserved() {
        assert_eq!(
            statements_of(&input(&["/* hidden */ VACUUM"])).unwrap(),
            ["/* hidden */ VACUUM"]
        );
    }
    #[test]
    fn rejects_empty_input() {
        assert_eq!(
            statements_of(&input(&[])).unwrap_err().code().as_str(),
            "invalid-input"
        );
    }
    #[test]
    fn sql_values_preserve_text_and_blob_length() {
        assert_eq!(sql_to_json(&SqlValue::Null), Value::Null);
        assert_eq!(
            sql_to_json(&SqlValue::Numeric(turso_core::Numeric::Integer(7))),
            json!(7)
        );
        assert_eq!(sql_to_json(&SqlValue::build_text("hello")), json!("hello"));
        assert_eq!(
            sql_to_json(&SqlValue::from_slice(&[1, 2, 3]).unwrap()),
            json!({"blob": 3})
        );
    }
    #[test]
    fn manifest_keeps_one_local_write_grant() {
        let m = provider::manifest::<TursoSqlProvider>().unwrap();
        assert_eq!(m.id.as_str(), "turso");
        assert_eq!(m.command_words, ["turso"]);
        assert_eq!(m.capabilities.len(), 1);
        assert_eq!(m.capabilities[0].id.as_str(), "turso.exec");
        assert_eq!(m.capabilities[0].effect, EffectKind::LocalWrite);
        assert_eq!(m.capabilities[0].risk, RiskLevel::Medium);
        assert_eq!(
            m.capabilities[0].input_schema["additionalProperties"],
            false
        );
    }
}

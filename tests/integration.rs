//! The turso-sql provider, exercised as the component it ships as.
//!
//! Everything here runs the real 11 MB component against a real `StorageHost` under the same
//! `StorageLimits` a deployment runs — 256 KiB per read, 256 B of entropy per call, 16 MiB per
//! file. The engine's behavior against this host is the thing under test; nothing in `src/` is
//! reachable natively, because its host imports expand to `unreachable!()` off `wasm32`.
//!
//! Multi-thread runtimes throughout: the storage path dispatches to `spawn_blocking`, and a
//! current-thread runtime deadlocks on a namespace lease.

mod support;

use dekopon_provider_sdk::{CommandRunOutcome, provider};
use dekopon_provider_sdk_testkit::conformance;
use dekopon_turso_sql_provider::TursoSqlProvider;
use serde_json::{Value, json};
use support::broker;

fn exec(statements: &[&str]) -> Value {
    json!({ "statements": statements })
}

/// Pulls the rows of the n-th statement out of the response envelope.
fn rows(output: &Value, index: usize) -> &Vec<Value> {
    output["results"][index]["rows"]
        .as_array()
        .unwrap_or_else(|| panic!("statement {index} has no rows array in {output}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn creates_a_table_writes_a_row_and_reads_it_back() {
    let broker = broker().await;

    let output = broker
        .invoke(
            "turso.exec",
            exec(&[
                "CREATE TABLE note(id INTEGER PRIMARY KEY, body TEXT)",
                "INSERT INTO note(body) VALUES('first')",
                "SELECT id, body FROM note",
            ]),
        )
        .await
        .expect("the statement batch runs");

    assert_eq!(rows(&output, 2).len(), 1, "{output}");
    assert_eq!(rows(&output, 2)[0], json!([1, "first"]));
}

/// The durability claim: a commit is the invocation transaction, not the guest's `sync`.
#[tokio::test(flavor = "multi_thread")]
async fn separate_invocations_share_one_database() {
    let broker = broker().await;

    broker
        .invoke(
            "turso.exec",
            exec(&["CREATE TABLE note(id INTEGER PRIMARY KEY, body TEXT)"]),
        )
        .await
        .expect("schema commits");
    broker
        .invoke(
            "turso.exec",
            exec(&["INSERT INTO note(body) VALUES('first')"]),
        )
        .await
        .expect("first row commits");
    broker
        .invoke(
            "turso.exec",
            exec(&["INSERT INTO note(body) VALUES('second')"]),
        )
        .await
        .expect("second row commits");

    let output = broker
        .invoke("turso.exec", exec(&["SELECT body FROM note ORDER BY id"]))
        .await
        .expect("a fourth invocation reads the first three");

    assert_eq!(rows(&output, 0).len(), 2, "{output}");
    assert_eq!(rows(&output, 0)[0], json!(["first"]));
    assert_eq!(rows(&output, 0)[1], json!(["second"]));
}

#[tokio::test(flavor = "multi_thread")]
async fn deleting_every_row_leaves_an_empty_table_for_the_next_invocation() {
    let broker = broker().await;

    broker
        .invoke(
            "turso.exec",
            exec(&[
                "CREATE TABLE note(id INTEGER PRIMARY KEY, body TEXT)",
                "INSERT INTO note(body) VALUES('first')",
                "INSERT INTO note(body) VALUES('second')",
            ]),
        )
        .await
        .expect("two rows commit");

    let counted = broker
        .invoke("turso.exec", exec(&["SELECT count(*) FROM note"]))
        .await
        .expect("count before delete");
    assert_eq!(rows(&counted, 0)[0], json!([2]), "{counted}");

    broker
        .invoke("turso.exec", exec(&["DELETE FROM note"]))
        .await
        .expect("delete commits");

    // Asserted from the provider's own output rather than from storage evidence, which reports
    // byte counts only as coarse powers-of-two buckets and can never show an exact zero.
    let emptied = broker
        .invoke("turso.exec", exec(&["SELECT count(*) FROM note"]))
        .await
        .expect("count after delete, in a later invocation");
    assert_eq!(rows(&emptied, 0)[0], json!([0]), "{emptied}");
}

/// The regression test this suite exists for.
///
/// Turso never checkpoints on its own. Without the `PRAGMA wal_checkpoint(TRUNCATE)` that ends
/// `exec`, the write-ahead log grows across invocations, and once it passes the host's
/// `max_read_bytes_per_call` every later invocation — including this pure `SELECT` — fails with a
/// terminal quota error and the namespace is permanently unreadable.
///
/// The page size is 64 KiB, so four un-truncated frames already exceed the 256 KiB ceiling. The
/// twenty write invocations below clear it by a wide margin.
#[tokio::test(flavor = "multi_thread")]
async fn the_write_ahead_log_is_truncated_before_each_invocation_ends() {
    let broker = broker().await;

    broker
        .invoke(
            "turso.exec",
            exec(&["CREATE TABLE note(id INTEGER PRIMARY KEY, body TEXT)"]),
        )
        .await
        .expect("schema commits");

    for index in 0..20 {
        broker
            .invoke(
                "turso.exec",
                exec(&[&format!("INSERT INTO note(body) VALUES('row-{index}')")]),
            )
            .await
            .unwrap_or_else(|error| panic!("write {index} must not brick the namespace: {error}"));
    }

    let output = broker
        .invoke("turso.exec", exec(&["SELECT count(*) FROM note"]))
        .await
        .expect("a plain read still works after twenty write invocations");
    assert_eq!(rows(&output, 0)[0], json!([20]), "{output}");

    // Corroboration on disk: the namespace holds the database and its log, and their combined
    // size stays far below what twenty un-truncated 64 KiB frames would have produced.
    let bytes = broker.storage_bytes_added();
    assert!(
        bytes < 20 * 64 * 1024,
        "durable bytes grew to {bytes}, which is WAL accumulation"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn vacuum_is_refused_with_the_provider_s_own_code() {
    let broker = broker().await;

    for sql in ["VACUUM", "VACUUM INTO 'copy.db'"] {
        let error = broker.invoke("turso.exec", exec(&[sql])).await.unwrap_err();
        let (status, message) = error.provider_failure().expect("provider refusal");
        assert_eq!(status, 1, "{sql}: {error}");
        assert!(
            message.contains("vacuum is not permitted"),
            "{sql}: {error}"
        );
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn malformed_sql_surfaces_the_engine_s_own_message() {
    let broker = broker().await;

    let error = broker
        .invoke("turso.exec", exec(&["SELECT FROM WHERE"]))
        .await
        .expect_err("a parse error is a failure");

    let (status, message) = error
        .provider_failure()
        .unwrap_or_else(|| panic!("expected a provider failure, got {error}"));
    assert_eq!(status, 1, "{message}");
    // The engine's own diagnostic, not a generic one: a caller has to be able to fix the SQL.
    assert!(!message.is_empty(), "{message}");
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_capability_is_refused() {
    let broker = broker().await;

    let error = broker
        .invoke("turso.nope", exec(&["SELECT 1"]))
        .await
        .expect_err("only turso.exec is declared");
    assert_eq!(error.provider_failure(), None, "{error}");
}

/// The `turso` command word, exercised across the `run-command` export the component actually
/// ships — the only place the new CLI world is proven on the wire rather than natively.
///
/// A proposal is the rewrite, not its result, so the last leg runs the proposal through `invoke`
/// to close the loop: the word and the capability have to agree about the input shape.
#[tokio::test(flavor = "multi_thread")]
async fn the_command_word_renders_help_proposes_and_reads_a_pipe() {
    let broker = broker().await;

    let help = broker
        .run_command("turso", &["--help".to_owned()], false)
        .await
        .expect("the component exports run-command");
    let CommandRunOutcome::Rendered {
        stdout,
        stderr,
        status,
    } = help
    else {
        panic!("--help renders rather than proposing: {help:?}");
    };
    assert_eq!(status, 0, "{stdout}");
    assert!(stderr.is_empty(), "{stderr}");
    assert!(stdout.contains("Usage: turso <STATEMENT>..."), "{stdout}");

    let empty = broker
        .run_command("turso", &[], false)
        .await
        .expect("empty argv is answered, not trapped");
    let CommandRunOutcome::Rendered { status, .. } = empty else {
        panic!("empty argv is a usage error: {empty:?}");
    };
    assert_eq!(status, 2);

    let missing_pipe = broker
        .run_command("turso", &["-".to_owned()], false)
        .await
        .expect("usage refusal");
    let CommandRunOutcome::Failed { error } = missing_pipe else {
        panic!("missing pipe must not propose: {missing_pipe:?}");
    };
    assert_eq!(error.code, "usage");
    assert!(error.message.contains("nothing was piped in"));

    let piped = broker
        .run_command("turso", &["-".to_owned()], true)
        .await
        .expect("a piped statement is answered");
    let CommandRunOutcome::Proposed {
        capability,
        input,
        secret_use: _,
    } = piped
    else {
        panic!("a piped statement becomes a proposal: {piped:?}");
    };
    assert_eq!(capability.as_str(), "turso.exec");
    assert_eq!(input, json!({"statements": [], "stdin_statement": true}));

    // The proposal is authorized on the ordinary path; running it proves the word and the
    // capability agree about the input shape.
    let output = broker
        .invoke_with_stdin(capability.as_str(), input, Some(b"SELECT 17"))
        .await
        .expect("the word's own proposal invokes");
    assert_eq!(rows(&output, 0)[0], json!([17]), "{output}");
    // The proposal carries only a marker; the statement comes from the later pipe.
    // Empty pipe is rejected before opening the database.
    let bytes_before = broker.storage_bytes_added();
    let proposal = json!({"statements": [], "stdin_statement": true});
    let error = broker
        .invoke_with_stdin("turso.exec", proposal, Some(b" \n "))
        .await
        .unwrap_err();
    assert!(
        error
            .provider_failure()
            .is_some_and(|(status, stderr)| status == 2 && stderr.contains("nothing was piped in")),
        "{error}"
    );
    assert_eq!(
        broker.storage_bytes_added(),
        bytes_before,
        "empty pipe did no storage work"
    );
    let error = broker
        .invoke_with_stdin(
            "turso.exec",
            json!({"statements": [], "stdin_statement": true}),
            Some(b""),
        )
        .await
        .unwrap_err();
    assert!(
        error
            .provider_failure()
            .is_some_and(|(status, _)| status == 2)
    );
    assert_eq!(
        broker.storage_bytes_added(),
        bytes_before,
        "zero-byte pipe did no storage work"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn component_registry_is_compiled_once_per_test_binary() {
    let _broker = broker().await;
    assert_eq!(support::registry_loads(), 1);
}

#[test]
fn actual_component_conforms_to_typed_storage_contract() {
    conformance::<TursoSqlProvider>(support::component())
        .expect("typed manifest, help and imports agree with the real component");
    let manifest = provider::manifest::<TursoSqlProvider>().expect("typed manifest");
    assert_eq!(manifest.capabilities.len(), 1);
    assert_eq!(manifest.capabilities[0].id.as_str(), "turso.exec");
    assert_eq!(
        manifest.capabilities[0].effect,
        dekopon_provider_sdk::EffectKind::LocalWrite
    );
}

/// The cheap regression gate that runs on every CI run, in place of the benchmarks.
///
/// Host calls cross the component boundary, so this is the per-row cost that matters. Note it is
/// *not* what bounds a batch: measured against 64 KiB page writes, `max_write_bytes_per_invocation`
/// trips first, at around a thousand single-row inserts, while the host-call count is still under
/// a seventh of its own 4096 ceiling.
///
/// The bounds are loose on purpose. They exist to catch a change that makes the engine flush per
/// row — the difference between two calls and twenty — not to pin a number that will drift.
#[tokio::test(flavor = "multi_thread")]
async fn host_calls_stay_near_two_per_inserted_row() {
    let broker = broker().await;

    broker
        .invoke(
            "turso.exec",
            exec(&["CREATE TABLE note(id INTEGER PRIMARY KEY, body TEXT)"]),
        )
        .await
        .expect("schema commits");

    let one = host_calls(&insert_rows(&broker, 1).await);
    let many = host_calls(&insert_rows(&broker, 200).await);
    let per_row = (many.saturating_sub(one)) as f64 / 199.0;

    println!("host calls: floor+1 row = {one}, 200 rows = {many}, marginal = {per_row:.2}/row");

    // The per-invocation floor — open, two pragmas, the checkpoint — is paid whatever the batch.
    assert!(one < 64, "the fixed floor grew to {one} host calls");
    assert!(
        per_row < 4.0,
        "{per_row:.2} host calls per row; a flush per row would look like this"
    );
    assert!(
        many < 4096,
        "{many} host calls is within reach of the host's 4096 per-invocation ceiling"
    );
}

async fn insert_rows(broker: &support::Broker, rows: usize) -> Value {
    let statements: Vec<String> = (0..rows)
        .map(|index| format!("INSERT INTO note(body) VALUES('row-{index}')"))
        .collect();
    let borrowed: Vec<&str> = statements.iter().map(String::as_str).collect();
    broker
        .invoke("turso.exec", exec(&borrowed))
        .await
        .unwrap_or_else(|error| panic!("{rows} inserts must succeed: {error}"))
}

fn host_calls(output: &Value) -> u64 {
    output["storage"]["hostCalls"]
        .as_object()
        .expect("hostCalls object")
        .values()
        .map(|value| value.as_u64().unwrap_or_default())
        .sum()
}

/// What happens at the write ceiling, and what the namespace looks like afterwards.
///
/// A thousand single-row inserts in one invocation amplify to roughly 16 MiB of 64 KiB page
/// writes and trip `max_write_bytes_per_invocation`. That is the real bound on a batch — the host
/// call count is still under a seventh of its own ceiling when this fires.
///
/// **This is destructive, and it did not used to be.** Through `dekopon-storage-host` 0.12 a
/// failed invocation was rolled back whole, so an oversized batch left a database that still
/// read. 0.13.0 applies writes per host call and has no invocation rollback, so the frames the
/// batch managed to commit stay in the write-ahead log — and the log is then larger than
/// `max_read_bytes_per_call`, which is the terminal condition the closing
/// `PRAGMA wal_checkpoint(TRUNCATE)` exists to prevent. That checkpoint never runs on a batch
/// that dies partway, so the namespace is unreadable from the next invocation on and no later
/// invocation can recover it: reading the log is itself the call that is refused.
///
/// Pinned rather than fixed. The provider cannot see the host's remaining write budget, and the
/// committed frames are real data that a guest-side recovery would have to throw away. Keep bulk
/// loads inside one `BEGIN`/`COMMIT`, as the test below this one does.
#[tokio::test(flavor = "multi_thread")]
async fn an_oversized_batch_is_refused_and_leaves_the_namespace_unreadable() {
    let broker = broker().await;

    broker
        .invoke(
            "turso.exec",
            exec(&[
                "CREATE TABLE note(id INTEGER PRIMARY KEY, body TEXT)",
                "INSERT INTO note(body) VALUES('survivor')",
            ]),
        )
        .await
        .expect("schema and one row commit");

    let statements: Vec<String> = (0..1000)
        .map(|index| format!("INSERT INTO note(body) VALUES('row-{index}')"))
        .collect();
    let borrowed: Vec<&str> = statements.iter().map(String::as_str).collect();
    let error = broker
        .invoke("turso.exec", exec(&borrowed))
        .await
        .expect_err("a thousand inserts exceed the invocation write budget");

    // A host-imposed quota refusal, not a provider-declared failure.
    assert_eq!(error.provider_failure(), None, "{error}");
    assert!(
        error.to_string().contains("quota"),
        "expected a quota refusal, got {error}"
    );

    // What the batch did commit before the refusal is still on disk — there is no rollback — and
    // it is an order of magnitude more than the one surviving row needs.
    let bytes = broker.storage_bytes_added();
    assert!(bytes > 8 * 1024 * 1024, "only {bytes} durable bytes remain");

    // And that is what makes the namespace terminal: the log now exceeds
    // `max_read_bytes_per_call`, so a plain read is refused for quota rather than answered, by
    // the host and not by the provider.
    let error = broker
        .invoke("turso.exec", exec(&["SELECT body FROM note"]))
        .await
        .expect_err("an un-checkpointed log this size cannot be read back");
    assert_eq!(error.provider_failure(), None, "{error}");
    assert!(
        error.to_string().contains("quota"),
        "expected a quota refusal, got {error}"
    );
}

/// The workaround for the ceiling above, and the reason it is a ceiling on *statements*.
///
/// Each statement outside an explicit transaction commits on its own, and a commit writes a whole
/// 64 KiB page — so the cost is per statement, not per row, and roughly 256 of them exhaust the
/// 16 MiB invocation write budget whatever they contain. Inside one `BEGIN`/`COMMIT` they share
/// pages, and the same thousand rows fit comfortably.
#[tokio::test(flavor = "multi_thread")]
async fn one_explicit_transaction_fits_a_bulk_load_that_implicit_commits_cannot() {
    let broker = broker().await;

    let mut statements = vec![
        "CREATE TABLE note(id INTEGER PRIMARY KEY, body TEXT)".to_owned(),
        "BEGIN".to_owned(),
    ];
    statements
        .extend((0..1000).map(|index| format!("INSERT INTO note(body) VALUES('row-{index}')")));
    statements.push("COMMIT".to_owned());
    let borrowed: Vec<&str> = statements.iter().map(String::as_str).collect();

    broker
        .invoke("turso.exec", exec(&borrowed))
        .await
        .expect("a thousand rows in one transaction fit the write budget");

    let output = broker
        .invoke("turso.exec", exec(&["SELECT count(*) FROM note"]))
        .await
        .expect("the bulk load committed");
    assert_eq!(rows(&output, 0)[0], json!([1000]), "{output}");
}

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
    let bytes = data_bytes(broker.storage_root());
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
        assert_eq!(
            error.provider_failure().map(|(code, _)| code),
            Some("refused"),
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

    let (code, message) = error
        .provider_failure()
        .unwrap_or_else(|| panic!("expected a provider failure, got {error}"));
    assert_eq!(code, "prepare", "{message}");
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

/// Pins the README's claim that the host's five-level lock ladder is available but unused.
///
/// `turso_core` never calls `File::lock_file` on this target, because its shared-memory WAL
/// backend is compiled out on `wasm32`. If a future engine bump starts locking, this goes red and
/// the coarse-lock claim in the README needs revisiting rather than quietly becoming false.
#[tokio::test(flavor = "multi_thread")]
async fn the_lock_ladder_is_never_walked() {
    let broker = broker().await;

    let output = broker
        .invoke(
            "turso.exec",
            exec(&[
                "CREATE TABLE note(id INTEGER PRIMARY KEY, body TEXT)",
                "INSERT INTO note(body) VALUES('first')",
                "SELECT * FROM note",
            ]),
        )
        .await
        .expect("a read-write batch runs");

    let calls = &output["storage"]["hostCalls"];
    assert_eq!(calls["lock"], json!(0), "{output}");
    assert_eq!(calls["unlock"], json!(0), "{output}");
    // The interesting counters are non-zero, so the zeroes above are not an empty trace.
    assert_ne!(calls["open"], json!(0), "{output}");
    assert_ne!(calls["readAt"], json!(0), "{output}");
    assert_ne!(calls["writeAt"], json!(0), "{output}");
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

async fn insert_rows(broker: &dekopon_provider_sdk_testkit::FakeBroker, rows: usize) -> Value {
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

/// What happens at the write ceiling, and whether the namespace survives it.
///
/// A thousand single-row inserts in one invocation amplify to roughly 16 MiB of 64 KiB page
/// writes and trip `max_write_bytes_per_invocation`. That is the real bound on a batch — the host
/// call count is still under a seventh of its own ceiling when this fires. The part worth pinning
/// is the recovery: the invocation is refused as a whole, and the database it was writing into is
/// still readable afterwards.
#[tokio::test(flavor = "multi_thread")]
async fn an_oversized_batch_is_refused_without_damaging_the_database() {
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

    // The transaction was refused whole: the pre-existing row is intact and nothing from the
    // rejected batch landed.
    let output = broker
        .invoke("turso.exec", exec(&["SELECT body FROM note"]))
        .await
        .expect("the namespace is still readable after a quota refusal");
    assert_eq!(rows(&output, 0), &vec![json!(["survivor"])], "{output}");
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

/// Sums every durable file in the namespace. Path components are HMAC tokens, so this walks
/// rather than naming `main.db` and `main.db-wal`.
fn data_bytes(root: &std::path::Path) -> u64 {
    fn visit(path: &std::path::Path, total: &mut u64) {
        let Ok(entries) = std::fs::read_dir(path) else {
            return;
        };
        for entry in entries.flatten() {
            let Ok(metadata) = entry.metadata() else {
                continue;
            };
            if metadata.is_dir() {
                visit(&entry.path(), total);
            } else {
                *total += metadata.len();
            }
        }
    }
    let mut total = 0;
    visit(&root.join("namespaces"), &mut total);
    total
}

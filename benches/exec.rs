//! Timings for `turso.exec`, measured through the same fake broker the tests use.
//!
//! The question these answer is "is this dog slow?", and the useful shape of the answer is a
//! fixed/marginal split: an agent workload pays the per-invocation floor — component
//! instantiation, opening the database, two pragmas, and the closing WAL checkpoint — on every
//! call, and the per-row cost only in proportion to what it asks for.
//!
//! Not run in CI. `cargo bench` after `./build.sh`. The host-call ceiling that *is* enforced on
//! every CI run lives in `tests/integration.rs`, because a count is a assertion, not a timing.

use std::{path::PathBuf, sync::OnceLock};

use dekopon_provider_sdk_testkit::{FakeBroker, StorageAccess, StorageInterface};
use serde_json::json;

fn main() {
    divan::main();
}

fn runtime() -> &'static tokio::runtime::Runtime {
    static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .expect("multi-thread runtime")
    })
}

fn component() -> PathBuf {
    if let Some(path) = std::env::var_os("TURSO_SQL_COMPONENT") {
        return PathBuf::from(path);
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("turso-sql-provider.wasm");
    assert!(
        path.exists(),
        "{} is missing. Run ./build.sh first.",
        path.display()
    );
    path
}

fn compile_cache() -> PathBuf {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/testkit-compile-cache");
    std::fs::create_dir_all(&directory).expect("compile cache directory");
    directory
}

fn broker() -> FakeBroker {
    runtime().block_on(async {
        FakeBroker::builder()
            .component(component())
            .provider("turso")
            .storage(StorageInterface::DurableFiles, StorageAccess::ReadWrite)
            .compile_cache(compile_cache())
            .build()
            .await
            .expect("the turso component loads")
    })
}

fn run(broker: &FakeBroker, statements: Vec<String>) {
    runtime()
        .block_on(broker.invoke("turso.exec", json!({"statements": statements})))
        .expect("statements run");
}

fn seeded(rows: usize) -> FakeBroker {
    let broker = broker();
    let mut statements = vec!["CREATE TABLE note(id INTEGER PRIMARY KEY, body TEXT)".to_owned()];
    if rows > 0 {
        // One transaction: seeding is setup, and per-statement commits would exhaust the
        // invocation write budget long before a thousand rows landed.
        statements.push("BEGIN".to_owned());
        statements.extend(inserts(rows));
        statements.push("COMMIT".to_owned());
    }
    run(&broker, statements);
    broker
}

fn inserts(rows: usize) -> Vec<String> {
    (0..rows)
        .map(|index| format!("INSERT INTO note(body) VALUES('row-{index}')"))
        .collect()
}

/// Loading and compiling the component, with a warm content-addressed cache.
///
/// This is what a broker pays once at startup, not per invocation — reported separately so it
/// never gets folded into a per-call number.
#[divan::bench(sample_count = 5, sample_size = 1)]
fn instantiate() -> FakeBroker {
    broker()
}

/// The per-invocation floor: open, `PRAGMA page_size`, `PRAGMA cache_size`, one trivial statement,
/// and the closing `PRAGMA wal_checkpoint(TRUNCATE)`. Everything else is this plus work.
#[divan::bench(sample_count = 20, sample_size = 1)]
fn exec_floor(bencher: divan::Bencher) {
    let broker = seeded(0);
    bencher.bench_local(|| run(&broker, vec!["SELECT 1".to_owned()]));
}

/// Marginal write cost. A fresh namespace per iteration, built untimed, so the tree the rows land
/// in is the same size every time.
///
/// It stops at 200 because these are implicit transactions: each statement commits on its own, and
/// a commit writes a whole 64 KiB page, so about 256 of them exhaust `max_write_bytes_per_invocation`
/// whatever they contain. `insert_rows_in_one_transaction` below is the same work without that.
#[divan::bench(args = [1, 100, 200], sample_count = 10, sample_size = 1)]
fn insert_rows(bencher: divan::Bencher, rows: usize) {
    bencher
        .with_inputs(|| (seeded(0), inserts(rows)))
        .bench_values(|(broker, statements)| run(&broker, statements));
}

/// The same rows inside one explicit transaction, which is how a bulk load should actually be sent.
/// Comparing this against `insert_rows` is the cost of per-statement commits.
#[divan::bench(args = [100, 1000], sample_count = 10, sample_size = 1)]
fn insert_rows_in_one_transaction(bencher: divan::Bencher, rows: usize) {
    bencher
        .with_inputs(|| {
            let mut statements = vec!["BEGIN".to_owned()];
            statements.extend(inserts(rows));
            statements.push("COMMIT".to_owned());
            (seeded(0), statements)
        })
        .bench_values(|(broker, statements)| run(&broker, statements));
}

/// Read path over a table that already holds a thousand rows.
#[divan::bench(sample_count = 10, sample_size = 1)]
fn select_scan_1000(bencher: divan::Bencher) {
    let broker = seeded(1000);
    bencher.bench_local(|| {
        run(
            &broker,
            vec!["SELECT id, body FROM note ORDER BY id".to_owned()],
        );
    });
}

/// The same scan reduced to one row, isolating the cost of returning a thousand rows as JSON from
/// the cost of visiting them.
#[divan::bench(sample_count = 10, sample_size = 1)]
fn select_count_over_1000(bencher: divan::Bencher) {
    let broker = seeded(1000);
    bencher.bench_local(|| run(&broker, vec!["SELECT count(*) FROM note".to_owned()]));
}

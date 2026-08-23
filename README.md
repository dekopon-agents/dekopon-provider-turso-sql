# Turso SQL provider

SQLite-compatible SQL inside a Wasm component, backed by broker-owned durable files.

The engine is [`turso_core`](https://github.com/tursodatabase/turso) — a pure-Rust SQLite rewrite —
compiled to `wasm32-unknown-unknown` from the [`dekopon-agents/turso`](https://github.com/dekopon-agents/turso)
fork. It reaches storage, entropy, and both clocks only through
`dekopon:storage/durable-files@0.1.0`. The generated component imports that interface and nothing
else: no WASI, no JS interop, no C.

| Capability | Effect | Behavior |
|---|---|---|
| `turso.exec` | `local-write` | Executes an ordered array of SQL statements against the namespace database and returns each statement's rows plus the host calls the engine produced. |

```json
{"statements":["CREATE TABLE IF NOT EXISTS note(id INTEGER PRIMARY KEY, body TEXT)","INSERT INTO note(body) VALUES('first')","SELECT id, body FROM note"]}
```

**Wrap bulk writes in a transaction.** A statement sent outside `BEGIN` commits on its own, and a
commit writes a whole 64 KiB page — so the write budget is spent per *statement*, not per row, and
roughly 256 of them exhaust `max_write_bytes_per_invocation` (16 MiB) no matter how little each one
writes. Two hundred single-row inserts fit; four hundred do not, and the evidence is identical to a
thousand because the ceiling is reached at the same statement either way. The same thousand rows
between one `BEGIN` and one `COMMIT` land comfortably, since they share pages:

```json
{"statements":["BEGIN","INSERT INTO note(body) VALUES('a')","INSERT INTO note(body) VALUES('b')","COMMIT"]}
```

Host calls are not what binds — about two per inserted row over a fixed floor of roughly thirty,
still under a seventh of the 4096 ceiling when the write budget runs out. An invocation that does
trip a ceiling is refused whole and leaves the database readable.

## What the engine actually does

Turso is WAL-only. It does not implement a rollback journal, `PRAGMA journal_mode = DELETE` is a
silent no-op that returns `wal`, and upstream has stated it does not plan to add one. It opens
exactly two durable files, `main.db` and `main.db-wal`.

It never requests shared memory on this target. Turso's shared-memory WAL backend is gated behind a
`cfg` that requires a 64-bit Unix or Windows host, so on `wasm32-unknown-unknown` the module is not
compiled at all and the in-process coordination backend — whose WAL index is an ordinary heap
hashmap — is selected unconditionally. `IO::supports_shared_wal_coordination` is additionally
pinned to `false` here, and `open_file` refuses any `-shm`/`-tshm` name outright, so an upstream
change cannot silently introduce a sidecar this adapter does not maintain.

Consequently the host's five-level lock ladder is **available but unused**. Turso's own lock surface
is two-state, `turso_core` never calls `File::lock_file`, and no `durable-files` I/O path consults
lock state. `lock`, `unlock`, and `check-reserved-lock` are called zero times in normal operation.
The adapter still walks the ladder one rung at a time when the engine does ask, because the host
rejects a skipped promotion.

Durability is the invocation transaction, not the guest's `sync`. The host commits the database and
its write-ahead log together, so there is no torn-WAL state to recover from.

## Three things that will bite a modification

**The WAL must be truncated before the invocation ends.** Turso never checkpoints on its own. Left
alone the log grows without bound, and once it exceeds the host's `max_read_bytes_per_call`
(256 KiB by default) *every* later invocation — including a pure `SELECT` — fails with a terminal
quota error and the namespace becomes permanently unreadable. `PRAGMA wal_checkpoint(TRUNCATE)` at
the end of `exec` holds it at zero bytes. Removing that line ships something that passes its tests
and dies in week two.

**Reads must be zero-filled by the adapter.** `durable-files` returns only the bytes that exist and
the contract delegates the zero-fill to its caller. Turso treats any short read as a hard
`CompletionError::ShortRead`, so `pread` fills the tail itself.

**Entropy must be chunked.** The host caps one `random-bytes` call at `max_entropy_bytes_per_call`
(256 bytes) and rejects anything larger rather than returning a short buffer, so an unchunked
request leaves the destination silently unfilled.

## What is not available

No full-text search. `tantivy` is `cfg(not(target_family = "wasm"))`-gated upstream, so `MATCH` and
the `fts` module are absent. A provider that needs search maintains its own inverted-index table.

No `VACUUM`. Plain `VACUUM` rewrites the whole file inside one invocation's byte budget, and
`VACUUM INTO` opens its destination on a hardcoded platform IO rather than the injected one. Both
are refused at the SQL boundary.

`datetime('now','localtime')` returns UTC. The fork drops chrono's `wasmbind`, which is what a
sandbox with no timezone database should do, but it is a silent divergence from SQLite.

## Using it

The component grants nothing on its own. An operator points `dekopon-brokerd` at it and writes a
constraint set for `turso.exec` — the namespace it may open, its byte and entropy budgets, and the
storage quota. See [the broker's configuration reference](https://github.com/dekopon-agents/dekopon/blob/main/crates/dekopon-brokerd/README.md).

Drop `turso-sql-provider.wasm` into a provider directory the broker loads:

```yaml
providers:
  - /opt/dekopon/providers
```

It is not shipped in the Dekopon container image. An 11 MB SQL engine is not something every
deployment should carry, so fetch the release asset for the tag you want and stage it yourself.

## Building

Requires the pinned toolchain and `wasm-tools`, because component encoding is not stable across
versions and the build is meant to be reproducible:

```console
rustup toolchain install 1.97.0 --profile minimal
cargo install wasm-tools --version 1.236.1 --locked
./build.sh
```

`build.sh` is a self-contained port of dekopon's `examples/providers/build-component.sh`. It pins
`rustc` and `wasm-tools` exactly, normalizes `-Cmetadata`, remaps the source root, `CARGO_HOME`, and
the sysroot out of the output, builds with `-Ccodegen-units=1`, and then fails if any local path
survived into the component. It keeps the `dekopon-provider-repro-v1` metadata domain the in-tree
build used, so a component built here is byte-identical to the one dekopon used to ship.

Native checks must not see `rustflags`; the wasm ones must:

```console
cargo fmt --check
cargo clippy --all-targets --locked -- -D warnings
RUSTFLAGS="$(cat rustflags)" cargo check --locked --target wasm32-unknown-unknown
```

The cfg in [`rustflags`](rustflags) cannot be declared in `Cargo.toml`, and a local
`.cargo/config.toml` would be ignored during the real build — `build.sh` sets
`CARGO_ENCODED_RUSTFLAGS`, which outranks `target.<triple>.rustflags` entirely. Without it
`getrandom 0.4` stops the compile with its `wasm_js` error. `build.sh` and CI both read that one
file, so they cannot skew.

Verify the import surface:

```console
wasm-tools component wit turso-sql-provider.wasm
```

`cc` cannot be gated out of the dependency graph and no check should try. It is an ungated
`[build-dependencies]` of `aegis`, so no downstream feature reaches it, and it is in `Cargo.lock`
regardless via `loom`, `shuttle`, and `iana-time-zone-haiku` because the lockfile is target- and
`cfg`-agnostic. Both a `cargo tree -i cc` gate and a lockfile grep fail forever.

Gate the artifact instead. With `pure-rust` selected, aegis's build script returns before it
invokes `cc::Build`, so no C ever enters the module — and that is directly observable:

```console
wasm-tools metadata show turso-sql-provider.wasm
```

Every `processed-by` row must be `rustc`, `wit-component`, or `wit-bindgen-rust`. A `clang` row
means the C aegis was linked in and the pure-rust cfg regressed. CI asserts this, along with the
absence of `wasm-bindgen`, `js-sys`, and `wasi` from the dependency graph, and that the only core
import is `dekopon:storage/durable-files@0.1.0`.

## Testing

Two suites, split because one of them needs an artifact the other one builds.

```console
cargo test --lib              # unit tests; no component required
./build.sh
cargo test --test integration # runs the component
cargo bench                   # timings; not run in CI
```

The unit tests cover what is reachable natively: statement refusal, the SQL-to-JSON mapping, the
argv rewrite, and the manifest. That is all of it — every host import expands to `unreachable!()`
off `wasm32`, so `DekoponIo` and `DekoponFile` cannot be driven from a native test at all.

Everything else is covered by running the real component against a real storage host, using
[`dekopon-provider-sdk-testkit`](https://docs.rs/dekopon-provider-sdk-testkit). It is a fake
*broker* — it mints authorization directly instead of consulting Cedar and a constraint catalog —
but the host below that is the deployment's, at the deployment's `StorageLimits`. So the zero-fill
and entropy-chunking paths are exercised under the same 256 KiB and 256-byte ceilings that make
them fragile, rather than against a mock that would have to restate them.

The load-bearing one is `the_write_ahead_log_is_truncated_before_each_invocation_ends`. It writes
twenty times and then reads, which an un-truncated log cannot survive. If you are changing `exec`,
delete the `PRAGMA wal_checkpoint(TRUNCATE)` line and confirm that test goes red before trusting
it — a WAL regression is silent until the namespace is already unreadable.

`the_lock_ladder_is_never_walked` pins the claim above that `lock` and `unlock` are called zero
times. An engine bump that starts locking turns it red, which is the intended way to find out.

## The fork is not optional, and it is coupled to this crate

[`dekopon-agents/turso`](https://github.com/dekopon-agents/turso) (branch `dekopon`) carries the
`wasm32-unknown-unknown` support this provider needs. The coupling runs both ways and is invisible
from either side alone:

- The fork **declares** `__dekopon_monotonic_time_ns` and `__dekopon_wall_time_ms` as
  `unsafe extern "Rust"` in `core/io/clock.rs`. This crate **defines** them in `src/io.rs`. The fork
  does not link for wasm32 without an embedder supplying both symbols.
- The fork flips the wasm32 `getrandom` backend to `custom`. This crate registers the custom backend
  for all three `getrandom` majors in the graph, also in `src/io.rs`.

So the pinned `turso_core` rev and this crate's version move together. Bumping the fork without
checking `src/io.rs` produces a link error at best and a wrong clock at worst.

## Releases

Each tag publishes `turso-sql-provider.wasm` two ways:

- a **release asset** with a `.sha256` alongside it and a provenance attestation, verifiable with
  `gh attestation verify turso-sql-provider.wasm --repo dekopon-agents/dekopon-provider-turso-sql`;
- an **OCI artifact** at `ghcr.io/dekopon-agents/provider-turso-sql`, pullable by tag or digest.

The release workflow rebuilds the component a second time and byte-compares before publishing, so a
tag that ships is a tag that reproduced.

## License

MIT or Apache-2.0, at your option.

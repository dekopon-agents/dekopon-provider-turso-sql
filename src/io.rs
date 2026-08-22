//! `turso_core::IO` and `turso_core::File` implemented over
//! `dekopon:storage/durable-files@0.1.0`.
//!
//! The engine reaches nothing else. Storage, entropy, and both clocks all
//! arrive through this module's host imports, which is what keeps the
//! generated component free of WASI and of any platform dependency.
//!
//! Two properties are load-bearing and easy to lose in a refactor:
//!
//! * `pread` zero-fills. `durable-files` returns only the bytes that exist,
//!   and the contract explicitly delegates the zero-fill to the adapter. Turso
//!   treats any short read as a hard `CompletionError::ShortRead`.
//! * Entropy is chunked. The host caps a single `random-bytes` call at
//!   `max_entropy_bytes_per_call` (256 bytes) and rejects anything larger
//!   outright, so an unchunked request returns an error rather than a short
//!   buffer — which would otherwise leave a caller's buffer silently unfilled.

use std::cell::{Cell, RefCell};
use std::sync::Arc;

use dekopon_provider_storage::durable_files as df;
use turso_core::io::FileSyncType;
use turso_core::{
    Buffer, Clock, Completion, CompletionError, File, IO, LimboError, MonotonicInstant, OpenFlags,
    Result, WallClockInstant,
};

/// The host rejects a `random-bytes` request above this outright.
const MAX_ENTROPY_PER_CALL: usize = 256;

// ---------- call trace ----------

/// Per-invocation host-call counters, reported in the capability's response so
/// an operator can see the exact I/O an engine produced.
#[derive(Default, Clone)]
pub struct Trace {
    pub opens: Vec<String>,
    pub open_calls: u64,
    pub read_at: u64,
    pub read_bytes: u64,
    pub write_at: u64,
    pub write_bytes: u64,
    pub sync: u64,
    pub truncate: u64,
    pub size: u64,
    pub lock: u64,
    pub unlock: u64,
    pub remove: u64,
    pub stat: u64,
    pub random: u64,
    pub monotonic: u64,
    pub wall: u64,
    pub short_reads: u64,
    pub zero_filled_bytes: u64,
}

thread_local! {
    static TRACE: RefCell<Trace> = RefCell::new(Trace::default());
}

pub fn trace_reset() {
    TRACE.with(|trace| *trace.borrow_mut() = Trace::default());
}

pub fn trace_snapshot() -> Trace {
    TRACE.with(|trace| trace.borrow().clone())
}

fn bump(update: impl FnOnce(&mut Trace)) {
    TRACE.with(|trace| update(&mut trace.borrow_mut()));
}

// ---------- error mapping ----------

fn completion_error(error: df::StorageError, scope: &'static str) -> CompletionError {
    use std::io::ErrorKind;
    let kind = match error {
        df::StorageError::NotFound => ErrorKind::NotFound,
        df::StorageError::AlreadyExists => ErrorKind::AlreadyExists,
        df::StorageError::InvalidName | df::StorageError::InvalidArgument => {
            ErrorKind::InvalidInput
        }
        df::StorageError::PermissionDenied => ErrorKind::PermissionDenied,
        df::StorageError::QuotaExceeded => ErrorKind::StorageFull,
        df::StorageError::Busy => ErrorKind::WouldBlock,
        df::StorageError::Timeout => ErrorKind::TimedOut,
        df::StorageError::Unsupported => ErrorKind::Unsupported,
        df::StorageError::Corrupt => ErrorKind::InvalidData,
        df::StorageError::Io => ErrorKind::Other,
    };
    CompletionError::IOError(kind, scope)
}

fn map_err(error: df::StorageError) -> LimboError {
    completion_error(error, "durable-files").into()
}

// ---------- entropy ----------

/// Fills `dest` from the broker's entropy import, one bounded call at a time.
/// Returns the number of bytes actually written, which is `dest.len()` unless
/// the invocation's entropy budget ran out.
fn fill_entropy(dest: &mut [u8]) -> usize {
    let mut filled = 0;
    while filled < dest.len() {
        let want = (dest.len() - filled).min(MAX_ENTROPY_PER_CALL);
        bump(|trace| trace.random += 1);
        let Ok(bytes) = df::random_bytes(want as u32) else {
            return filled;
        };
        let taken = bytes.len().min(want);
        if taken == 0 {
            return filled;
        }
        dest[filled..filled + taken].copy_from_slice(&bytes[..taken]);
        filled += taken;
    }
    filled
}

// ---------- clock seam consumed by the patched turso_core ----------

#[unsafe(no_mangle)]
extern "Rust" fn __dekopon_monotonic_time_ns() -> u64 {
    bump(|trace| trace.monotonic += 1);
    df::monotonic_time_ns().unwrap_or(0)
}

#[unsafe(no_mangle)]
extern "Rust" fn __dekopon_wall_time_ms() -> u64 {
    bump(|trace| trace.wall += 1);
    df::wall_time_ms().unwrap_or(0)
}

// ---------- IO ----------

pub struct DekoponIo {
    base_ns: Cell<u64>,
}

// SAFETY: a provider invocation is a single-threaded component instance, and
// wit-bindgen resource handles are instance-local indices that never escape it.
unsafe impl Send for DekoponIo {}
unsafe impl Sync for DekoponIo {}

impl Default for DekoponIo {
    fn default() -> Self {
        Self::new()
    }
}

impl DekoponIo {
    pub fn new() -> Self {
        bump(|trace| trace.monotonic += 1);
        Self {
            base_ns: Cell::new(df::monotonic_time_ns().unwrap_or(0)),
        }
    }
}

impl Clock for DekoponIo {
    fn current_time_monotonic(&self) -> MonotonicInstant {
        bump(|trace| trace.monotonic += 1);
        let now = df::monotonic_time_ns().unwrap_or(0);
        MonotonicInstant::from_nanos(u128::from(now.saturating_sub(self.base_ns.get())))
    }

    fn current_time_wall_clock(&self) -> WallClockInstant {
        bump(|trace| trace.wall += 1);
        let ms = df::wall_time_ms().unwrap_or(0);
        WallClockInstant {
            secs: (ms / 1000) as i64,
            micros: ((ms % 1000) * 1000) as u32,
        }
    }
}

impl IO for DekoponIo {
    fn open_file(&self, path: &str, flags: OpenFlags, _direct: bool) -> Result<Arc<dyn File>> {
        // The shared-memory WAL backend is compiled out on this target, so a
        // request for its sidecar means the engine took a path this adapter
        // does not implement. Fail loudly rather than silently creating a file
        // nothing will maintain.
        if path.ends_with("-shm") || path.ends_with("-tshm") {
            return Err(LimboError::InvalidArgument(format!(
                "shared-memory WAL is not available on this target: {path}"
            )));
        }
        bump(|trace| {
            trace.open_calls += 1;
            trace.opens.push(path.to_owned());
        });
        let read_only = flags.contains(OpenFlags::ReadOnly);
        let options = df::OpenOptions::new()
            .read(true)
            .write(!read_only)
            .create(!read_only && flags.contains(OpenFlags::Create));
        let file = df::open(path, options).map_err(map_err)?;
        Ok(Arc::new(DekoponFile {
            file,
            level: Cell::new(df::LockLevel::None),
            no_lock: flags.contains(OpenFlags::NoLock) || read_only,
        }))
    }

    fn remove_file(&self, path: &str) -> Result<()> {
        bump(|trace| trace.remove += 1);
        df::remove(path, df::Durability::DataAndMetadata).map_err(map_err)
    }

    /// Keeps the `{db}-tshm` shared-memory path out of the engine entirely.
    /// This is already the trait default; it is stated so an upstream bump
    /// cannot flip it silently.
    fn supports_shared_wal_coordination(&self) -> bool {
        false
    }

    /// Every `durable-files` call completes inline before it returns, so there
    /// is never outstanding I/O to pump.
    fn step(&self) -> Result<()> {
        Ok(())
    }

    fn generate_random_number(&self) -> i64 {
        let mut bytes = [0_u8; 8];
        fill_entropy(&mut bytes);
        i64::from_le_bytes(bytes)
    }

    fn fill_bytes(&self, dest: &mut [u8]) {
        fill_entropy(dest);
    }

    fn yield_now(&self) {}

    /// `std::thread::sleep` panics "can't sleep" on wasm32-unknown-unknown and
    /// is reachable from the WAL read-lock backoff.
    fn sleep(&self, _duration: std::time::Duration) {}

    fn file_id(&self, path: &str) -> Result<turso_core::io::FileId> {
        bump(|trace| trace.stat += 1);
        match df::stat(path) {
            Ok(Some(stat)) => Ok(turso_core::io::FileId {
                dev: 0,
                ino: stat.identity,
            }),
            _ => Ok(turso_core::io::FileId::from_path_hash(path)),
        }
    }
}

// ---------- File ----------

pub struct DekoponFile {
    file: df::File,
    level: Cell<df::LockLevel>,
    no_lock: bool,
}

// SAFETY: as for `DekoponIo`.
unsafe impl Send for DekoponFile {}
unsafe impl Sync for DekoponFile {}

const LADDER: [df::LockLevel; 5] = [
    df::LockLevel::None,
    df::LockLevel::Shared,
    df::LockLevel::Reserved,
    df::LockLevel::Pending,
    df::LockLevel::Exclusive,
];

impl DekoponFile {
    /// Walks the host's five-level ladder one rung at a time. Turso's own lock
    /// surface is two-state, and the host rejects a skipped promotion, so the
    /// intermediate rungs are this adapter's job.
    fn promote_to(&self, target: df::LockLevel) -> Result<()> {
        let current = LADDER
            .iter()
            .position(|level| *level == self.level.get())
            .expect("lock level is in the ladder");
        let wanted = LADDER
            .iter()
            .position(|level| *level == target)
            .expect("lock level is in the ladder");
        if wanted < current {
            bump(|trace| trace.unlock += 1);
            self.file.unlock(target).map_err(map_err)?;
            self.level.set(target);
            return Ok(());
        }
        for level in LADDER.iter().take(wanted + 1).skip(current + 1) {
            bump(|trace| trace.lock += 1);
            self.file.lock(*level).map_err(map_err)?;
            self.level.set(*level);
        }
        Ok(())
    }
}

impl File for DekoponFile {
    fn lock_file(&self, exclusive: bool) -> Result<()> {
        if self.no_lock {
            return Ok(());
        }
        self.promote_to(if exclusive {
            df::LockLevel::Exclusive
        } else {
            df::LockLevel::Shared
        })
    }

    fn unlock_file(&self) -> Result<()> {
        if self.no_lock || self.level.get() == df::LockLevel::None {
            return Ok(());
        }
        bump(|trace| trace.unlock += 1);
        self.file.unlock(df::LockLevel::None).map_err(map_err)?;
        self.level.set(df::LockLevel::None);
        Ok(())
    }

    fn pread(&self, pos: u64, completion: Completion) -> Result<Completion> {
        let want = completion.as_read().buf().len();
        if want == 0 {
            completion.complete(0);
            return Ok(completion);
        }
        bump(|trace| trace.read_at += 1);
        match self.file.read_at(pos, want as u32) {
            Ok(bytes) => {
                let read = bytes.len().min(want);
                bump(|trace| {
                    trace.read_bytes += read as u64;
                    if read < want {
                        trace.short_reads += 1;
                        trace.zero_filled_bytes += (want - read) as u64;
                    }
                });
                let buffer = completion.as_read().buf();
                let slice = buffer.as_mut_slice();
                slice[..read].copy_from_slice(&bytes[..read]);
                // `durable-files` returns available bytes only. The contract
                // delegates the zero-fill to the adapter, and turso treats an
                // unfilled tail as a hard short-read error.
                slice[read..want].fill(0);
                completion.complete(read as i32);
            }
            Err(error) => completion.error(completion_error(error, "durable-files read-at")),
        }
        Ok(completion)
    }

    fn pwrite(&self, pos: u64, buffer: Arc<Buffer>, completion: Completion) -> Result<Completion> {
        let len = buffer.len();
        bump(|trace| {
            trace.write_at += 1;
            trace.write_bytes += len as u64;
        });
        match self.file.write_at(pos, buffer.as_slice()) {
            Ok(()) => completion.complete(len as i32),
            Err(error) => completion.error(completion_error(error, "durable-files write-at")),
        }
        Ok(completion)
    }

    fn sync(&self, completion: Completion, sync_type: FileSyncType) -> Result<Completion> {
        bump(|trace| trace.sync += 1);
        let mode = match sync_type {
            FileSyncType::Fsync => df::Durability::DataAndMetadata,
            FileSyncType::FullFsync => df::Durability::Full,
        };
        match self.file.sync(mode) {
            Ok(()) => completion.complete(0),
            Err(error) => completion.error(completion_error(error, "durable-files sync")),
        }
        Ok(completion)
    }

    fn truncate(&self, len: u64, completion: Completion) -> Result<Completion> {
        bump(|trace| trace.truncate += 1);
        match self.file.truncate(len) {
            Ok(()) => completion.complete(0),
            Err(error) => completion.error(completion_error(error, "durable-files truncate")),
        }
        Ok(completion)
    }

    fn size(&self) -> Result<u64> {
        bump(|trace| trace.size += 1);
        self.file.size().map_err(map_err)
    }
}

// ---------- getrandom backends ----------
//
// Three majors are in the graph and each registers its custom backend
// differently: 0.2 through a macro, 0.3 and 0.4 through a named extern symbol.

fn dekopon_entropy(dest: &mut [u8]) -> std::result::Result<(), getrandom02::Error> {
    if fill_entropy(dest) == dest.len() {
        Ok(())
    } else {
        Err(getrandom02::Error::UNSUPPORTED)
    }
}
getrandom02::register_custom_getrandom!(dekopon_entropy);

#[unsafe(no_mangle)]
unsafe extern "Rust" fn __getrandom_v03_custom(
    dest: *mut u8,
    len: usize,
) -> std::result::Result<(), getrandom03::Error> {
    let slice = unsafe { std::slice::from_raw_parts_mut(dest, len) };
    if fill_entropy(slice) == len {
        Ok(())
    } else {
        Err(getrandom03::Error::UNSUPPORTED)
    }
}

#[unsafe(no_mangle)]
unsafe extern "Rust" fn __getrandom_v04_custom(
    dest: *mut u8,
    len: usize,
) -> std::result::Result<(), getrandom04::Error> {
    let slice = unsafe { std::slice::from_raw_parts_mut(dest, len) };
    if fill_entropy(slice) == len {
        Ok(())
    } else {
        Err(getrandom04::Error::UNSUPPORTED)
    }
}

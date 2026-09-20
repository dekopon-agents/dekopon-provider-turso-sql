//! Shared setup for the component-level suites.
//!
//! Included rather than imported: `tests/` files are separate crates, and this crate is a
//! `cdylib` with no `rlib`, so there is nothing to hang a shared module off.

use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
    time::{SystemTime, UNIX_EPOCH},
};

use dekopon_provider_sdk_testkit::{FakeBroker, StorageAccess, StorageInterface};

/// Locates the built component.
///
/// The component is a build artifact and is `.gitignore`d, so it is absent until `build.sh` runs.
/// This panics rather than skipping: a suite that quietly passes when the thing under test is
/// missing is the exact failure this crate's tests were added to end.
pub fn component() -> PathBuf {
    let path = PathBuf::from(
        std::env::var_os("DEKOPON_PROVIDER_COMPONENT")
            .expect("DEKOPON_PROVIDER_COMPONENT must point at the built component"),
    );
    assert!(path.exists(), "{} is missing", path.display());
    path
}

/// A private compiled-component directory for each independently built broker.
///
/// SDK 0.18 refuses concurrent publishers even when their component bytes match. Keep tests
/// parallel without sharing cold caches; directories live under target until build cleanup,
/// so no mapped artifact is removed while its broker is alive.
fn compile_cache() -> PathBuf {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/testkit-compile-cache");
    std::fs::create_dir_all(&root).expect("compile cache root");
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock after Unix epoch")
        .as_nanos();
    let directory = root.join(format!(
        "broker-{}-{nonce}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&directory).expect("private compile cache directory");
    directory
}

pub async fn broker() -> FakeBroker {
    FakeBroker::builder()
        .component(component())
        .provider("turso")
        .storage(StorageInterface::DurableFiles, StorageAccess::ReadWrite)
        .compile_cache(compile_cache())
        .build()
        .await
        .expect("the turso component loads")
}

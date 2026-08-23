//! Shared setup for the component-level suites.
//!
//! Included rather than imported: `tests/` files are separate crates, and this crate is a
//! `cdylib` with no `rlib`, so there is nothing to hang a shared module off.

use std::path::PathBuf;

use dekopon_provider_sdk_testkit::{FakeBroker, StorageAccess, StorageInterface};

/// Locates the built component.
///
/// The component is a build artifact and is `.gitignore`d, so it is absent until `build.sh` runs.
/// This panics rather than skipping: a suite that quietly passes when the thing under test is
/// missing is the exact failure this crate's tests were added to end.
pub fn component() -> PathBuf {
    if let Some(path) = std::env::var_os("TURSO_SQL_COMPONENT") {
        return PathBuf::from(path);
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("turso-sql-provider.wasm");
    assert!(
        path.exists(),
        "{} is missing. Run ./build.sh first, or set TURSO_SQL_COMPONENT.",
        path.display()
    );
    path
}

/// A cache directory shared by every test in the process.
///
/// Cranelift on an 11 MB component is the whole of a cold start, and it is otherwise paid once per
/// `FakeBroker`. Content-addressed, so sharing it across tests is safe.
pub fn compile_cache() -> PathBuf {
    let directory = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target/testkit-compile-cache");
    std::fs::create_dir_all(&directory).expect("compile cache directory");
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

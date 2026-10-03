//! Storage-authorized real broker fixture. One compiled registry per test binary;
//! distinct subject namespaces keep tests isolated while each test's invocations persist.
use dekopon_broker_host::{
    BrokerHostError, BrokerHostLimits, BrokerInvocationFailure, BrokerProviderRegistry,
    CommandRunOutcome, Streams, asset::AssetInputs,
};
use dekopon_capability::{
    ExecutionConstraints, ProposedInvocation, StorageAccess, StorageConstraints, StorageInterface,
    StorageScope, broker::AuthorizationGate,
};
use dekopon_core::{
    Actor, AgentId, CapabilityId, ExternalSubject, InvocationId, PrincipalId, ProviderId, TraceId,
};
use dekopon_storage_host::{ContinuityPolicy, StorageGrantRequest, StorageHost, StorageLimits};
use serde_json::Value;
use std::{
    path::{Path, PathBuf},
    sync::{
        Arc, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};

pub fn component() -> PathBuf {
    let path = PathBuf::from(
        std::env::var_os("DEKOPON_PROVIDER_COMPONENT")
            .expect("DEKOPON_PROVIDER_COMPONENT must point at the built component"),
    );
    assert!(path.exists(), "{} is missing", path.display());
    path.canonicalize().expect("canonical component")
}

struct Shared {
    _root: tempfile::TempDir,
    root: PathBuf,
    storage: StorageHost,
    registry: Arc<BrokerProviderRegistry>,
}
static SHARED: tokio::sync::OnceCell<Shared> = tokio::sync::OnceCell::const_new();
static SERIAL: OnceLock<Arc<tokio::sync::Mutex<()>>> = OnceLock::new();
static TEST: AtomicU64 = AtomicU64::new(0);
static REGISTRY_LOADS: AtomicU64 = AtomicU64::new(0);
pub fn registry_loads() -> u64 {
    REGISTRY_LOADS.load(Ordering::Relaxed)
}

pub struct Broker {
    shared: &'static Shared,
    subject: ExternalSubject,
    invocation: AtomicU64,
    _serial: tokio::sync::OwnedMutexGuard<()>,
    bytes_before: u64,
}

#[derive(Debug)]
pub enum TestError {
    Host(String),
    Invocation(Box<BrokerInvocationFailure>),
}
impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Host(msg) => f.write_str(msg),
            Self::Invocation(e) => write!(f, "{e}"),
        }
    }
}
impl TestError {
    pub fn provider_failure(&self) -> Option<(u8, &str)> {
        let Self::Invocation(e) = self else {
            return None;
        };
        match e.error.as_ref() {
            BrokerHostError::ProviderFailure { status, stderr, .. } => Some((*status, stderr)),
            _ => None,
        }
    }
}
fn host(e: impl std::fmt::Display) -> TestError {
    TestError::Host(e.to_string())
}

pub async fn broker() -> Broker {
    let serial = SERIAL
        .get_or_init(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone()
        .lock_owned()
        .await;
    let shared = SHARED
        .get_or_init(|| async {
            let root = tempfile::tempdir().expect("temporary storage root");
            let storage_root = root
                .path()
                .canonicalize()
                .expect("real path")
                .join("storage");
            let storage =
                StorageHost::open(&storage_root, StorageLimits::default()).expect("storage host");
            let registry = Arc::new(
                BrokerProviderRegistry::load_with_storage(
                    [component()],
                    BrokerHostLimits::default(),
                    Some(storage.clone()),
                )
                .await
                .expect("checked turso component loads"),
            );
            REGISTRY_LOADS.fetch_add(1, Ordering::Relaxed);
            // A test binary has one async initialization. All test tasks use this cached registry.
            Shared {
                _root: root,
                root: storage_root,
                storage,
                registry,
            }
        })
        .await;
    let id = TEST.fetch_add(1, Ordering::Relaxed);
    Broker {
        shared,
        subject: format!("slack.t0123abc.u{id:08x}")
            .parse()
            .expect("test subject"),
        invocation: AtomicU64::new(0),
        _serial: serial,
        bytes_before: data_bytes(&shared.root),
    }
}
fn data_bytes(root: &Path) -> u64 {
    fn visit(path: &Path, total: &mut u64) {
        if let Ok(entries) = std::fs::read_dir(path) {
            for entry in entries.flatten() {
                if let Ok(metadata) = entry.metadata() {
                    if metadata.is_dir() {
                        visit(&entry.path(), total);
                    } else {
                        *total += metadata.len();
                    }
                }
            }
        }
    }
    let mut total = 0;
    visit(&root.join("namespaces"), &mut total);
    total
}
impl Broker {
    pub fn storage_bytes_added(&self) -> u64 {
        data_bytes(&self.shared.root).saturating_sub(self.bytes_before)
    }
    pub async fn run_command(
        &self,
        word: &str,
        args: &[String],
        stdin_piped: bool,
    ) -> Result<CommandRunOutcome, TestError> {
        self.shared
            .registry
            .run_command(word, args, stdin_piped)
            .await
            .map_err(host)
    }
    pub async fn invoke(&self, capability: &str, input: Value) -> Result<Value, TestError> {
        self.invoke_with_stdin(capability, input, None).await
    }
    pub async fn invoke_with_stdin(
        &self,
        capability: &str,
        input: Value,
        stdin: Option<&[u8]>,
    ) -> Result<Value, TestError> {
        self.invoke_inner(capability, input, stdin, None).await
    }
    /// Storage-backed equivalent of the SDK testkit's `close_stdout_after`: use
    /// its bounded reader/early-drop strategy but keep the explicit storage grant.
    pub async fn invoke_close_stdout_after(
        &self,
        capability: &str,
        input: Value,
        bytes: usize,
    ) -> Result<Value, TestError> {
        self.invoke_inner(capability, input, None, Some(bytes))
            .await
    }
    async fn invoke_inner(
        &self,
        capability: &str,
        input: Value,
        stdin: Option<&[u8]>,
        close_after: Option<usize>,
    ) -> Result<Value, TestError> {
        let capability: CapabilityId = capability.parse().map_err(host)?;
        let invocation: InvocationId = format!(
            "turso-test-{}",
            self.invocation.fetch_add(1, Ordering::Relaxed) + TEST.load(Ordering::Relaxed) * 10000
        )
        .parse()
        .map_err(host)?;
        let provider: ProviderId = "turso".parse().map_err(host)?;
        let agent: AgentId = "testkit-agent".parse().map_err(host)?;
        let grant = self
            .shared
            .storage
            .grant(StorageGrantRequest::new(
                invocation.clone(),
                capability.clone(),
                provider.clone(),
                StorageInterface::DurableFiles,
                StorageAccess::ReadWrite,
                StorageScope::PrivateConversation,
                agent.clone(),
                self.subject.clone(),
                "slack",
                "testkit-transport",
                "c0123abc",
                "c0123abc:1712345678.000100",
                ContinuityPolicy::Stable,
                b"testkit-authority".to_vec(),
            ))
            .map_err(host)?;
        let proposed = ProposedInvocation::new(
            invocation,
            capability,
            Actor::Agent { agent },
            TraceId::new(*b"dekopon-testkit!").expect("trace id"),
            input,
        );
        let authorized = AuthorizationGate::new()
            .authorize(
                proposed,
                provider,
                "testkit-decision".into(),
                "testkit-broker".parse::<PrincipalId>().map_err(host)?,
                "testkit-policy".into(),
                ExecutionConstraints {
                    asset: None,
                    timeout_ms: BrokerHostLimits::default().max_timeout.as_millis() as u64,
                    http: None,
                    storage: Some(StorageConstraints {
                        interface: StorageInterface::DurableFiles,
                        access: StorageAccess::ReadWrite,
                        scope: StorageScope::PrivateConversation,
                        retention: Default::default(),
                    }),
                    secret_use: None,
                },
            )
            .map_err(host)?;
        let (writer, reader) = std::os::unix::net::UnixStream::pair().map_err(host)?;
        let (ready, started) = std::sync::mpsc::sync_channel(0);
        let captured = std::thread::spawn(move || {
            use std::io::Read as _;
            if close_after == Some(0) {
                drop(reader);
                let _ = ready.send(());
                return Ok::<_, std::io::Error>(Vec::new());
            }
            let _ = ready.send(());
            let mut bytes = Vec::new();
            reader
                .take(close_after.unwrap_or(16 * 1024 * 1024 + 1) as u64)
                .read_to_end(&mut bytes)
                .map(|_| bytes)
        });
        started.recv().map_err(host)?;
        let (stdin_end, feeder) = if let Some(bytes) = stdin {
            let (host_end, mut send) = std::os::unix::net::UnixStream::pair().map_err(host)?;
            let bytes = bytes.to_vec();
            let feeder = std::thread::spawn(move || {
                use std::io::Write as _;
                let _ = send.write_all(&bytes);
            });
            (Some(host_end.into()), Some(feeder))
        } else {
            (None, None)
        };
        let assets = AssetInputs {
            streams: Some(Streams {
                stdin: stdin_end,
                stdout: writer.into(),
            }),
            ..Default::default()
        };
        let result = self
            .shared
            .registry
            .invoke_with_storage(authorized, None, Some(grant), assets)
            .await;
        if let Some(feeder) = feeder {
            feeder.join().map_err(|_| host("stdin feeder panicked"))?;
        }
        let bytes = captured
            .join()
            .map_err(|_| host("stdout capture panicked"))?
            .map_err(host)?;
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(host("stdout capture exceeded bound"));
        }
        result.map_err(|e| TestError::Invocation(Box::new(e)))?;
        serde_json::from_slice(&bytes).map_err(host)
    }
}

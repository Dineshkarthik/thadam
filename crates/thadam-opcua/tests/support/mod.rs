//! An in-process OPC UA server and certificate fixtures for the source's
//! tests. The server offers every policy and mode the source supports, the
//! anonymous and username tokens, and an X.509 user token. It trusts the
//! client certificate only, so a rogue certificate is refused the way a
//! real server would refuse it.

#![allow(clippy::unwrap_used, clippy::expect_used, dead_code)]

use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU32, Ordering};
use std::time::Duration;

use opcua::crypto::{CertificateStore, SecurityPolicy, X509Data};
use opcua::server::address_space::{
    AccessLevel, AddressSpace, NodeType, ReferenceDirection, VariableBuilder,
};
use opcua::server::diagnostics::NamespaceMetadata;
use opcua::server::node_manager::memory::{
    InMemoryNodeManager, SimpleNodeManagerImpl, simple_node_manager,
};
use opcua::server::{ANONYMOUS_USER_TOKEN_ID, ServerBuilder, ServerHandle, ServerUserToken};
use opcua::types::{
    ApplicationDescription, Array, DataTypeId, DataValue, LocalizedText, MessageSecurityMode,
    NodeId, ObjectId, ReferenceTypeId, StatusCode, VariableTypeId, Variant, VariantScalarTypeId,
};
use thadam_core::SourceFactory;
use thadam_core::{
    BatchLimits, BuildContext, RawMessage, Shutdown, Source, SourceContext, SourceId, SystemClock,
};
use thadam_opcua::OpcUaSourceFactory;
use serde_json::{Value, json};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// The test namespace URI; variables live under it.
pub const NAMESPACE: &str = "urn:thadam:opcua:test";
/// The server's application URI, carried by its certificate.
pub const SERVER_APP_URI: &str = "urn:thadam:opcua:test-server";
/// The client's application URI. The source sets this itself, so the
/// client certificate must carry the same URI.
pub const CLIENT_APP_URI: &str = "urn:thadam:opcua";

/// The test username (`auth` with `kind: username`).
pub const USERNAME: &str = "thadam";
/// The test user's password.
pub const PASSWORD: &str = "correct-horse";
/// The token id of the X.509 user the server knows (`auth` with `kind:
/// certificate`, using `user_cert`).
const CERT_USER: &str = "certuser";

type TestNodeManager = InMemoryNodeManager<SimpleNodeManagerImpl>;

/// Where the source's config expects node ids.
#[derive(Clone)]
pub struct TestNodes {
    /// `ns=N;i=1` — Double, updated on the timer.
    pub temp: NodeId,
    /// `ns=N;i=2` — Boolean.
    pub running: NodeId,
    /// `ns=N;i=3` — String.
    pub label: NodeId,
    /// `ns=N;i=4` — Double array.
    pub accel: NodeId,
    /// `ns=N;i=5` — Int32, updated on the timer.
    pub counter: NodeId,
}

impl TestNodes {
    /// The `ns=N;i=<n>` spelling source configs take.
    pub fn id_str(node: &NodeId) -> String {
        node.to_string()
    }
}

/// Certificates shared by every test in the process. Generating RSA keys
/// is the slow part of these tests, so they are made once.
pub struct Material {
    /// The server's certificate and key (PEM).
    pub server_cert: PathBuf,
    pub server_key: PathBuf,
    /// The client application certificate, trusted by the server.
    pub client_cert: PathBuf,
    pub client_key: PathBuf,
    /// The X.509 user certificate the server accepts as a user token.
    pub user_cert: PathBuf,
    pub user_key: PathBuf,
    /// A certificate neither the server nor its user tokens know.
    pub rogue_cert: PathBuf,
    pub rogue_key: PathBuf,
}

impl Material {
    /// The process-wide certificate set.
    pub fn shared() -> &'static Material {
        static MATERIAL: OnceLock<Material> = OnceLock::new();
        MATERIAL.get_or_init(|| {
            let dir = tempfile::tempdir().expect("material dir").keep();
            let file = |name: &str| dir.join(name);

            let server_description = ApplicationDescription {
                application_uri: opcua::types::UAString::from(SERVER_APP_URI.to_owned()),
                application_name: LocalizedText::new("en", "thadam-opcua-test-server"),
                ..Default::default()
            };
            let server: X509Data = (
                server_description,
                Some(vec!["127.0.0.1".to_owned(), "localhost".to_owned()]),
            )
                .into();
            let (server_cert, server_key) = (file("server.der"), file("server-key.pem"));
            CertificateStore::create_certificate_and_key(&server, true, &server_cert, &server_key)
                .expect("server certificate");

            let (client_cert, client_key) = make_app_cert(&dir, "client", CLIENT_APP_URI);
            let (user_cert, user_key) = make_app_cert(&dir, "user", "urn:thadam:opcua:test-user");
            let (rogue_cert, rogue_key) = make_app_cert(&dir, "rogue", CLIENT_APP_URI);

            Material {
                server_cert,
                server_key,
                client_cert,
                client_key,
                user_cert,
                user_key,
                rogue_cert,
                rogue_key,
            }
        })
    }
}

fn make_app_cert(dir: &Path, name: &str, application_uri: &str) -> (PathBuf, PathBuf) {
    let cert = dir.join(format!("{name}.der"));
    let key = dir.join(format!("{name}-key.pem"));
    let description = ApplicationDescription {
        application_uri: opcua::types::UAString::from(application_uri.to_owned()),
        product_uri: opcua::types::UAString::from("urn:thadam:opcua:test".to_owned()),
        application_name: LocalizedText::new("en", "thadam test"),
        ..Default::default()
    };
    CertificateStore::create_certificate_and_key(&description.into(), true, &cert, &key)
        .expect("application certificate");
    (cert, key)
}

/// A per-test copy of the client's certificate files, so each test has its
/// own PKI directory (trusted and rejected entries are per test).
pub struct ClientFiles {
    _dir: tempfile::TempDir,
    /// The client's PKI directory.
    pub pki_dir: PathBuf,
    /// The client's application certificate and key, inside `pki_dir`.
    pub cert: PathBuf,
    pub key: PathBuf,
}

impl ClientFiles {
    /// A fresh PKI directory holding a copy of `cert` and `key`.
    pub fn new(cert: &Path, key: &Path) -> Self {
        let dir = tempfile::tempdir().expect("client dir");
        let pki_dir = dir.path().join("client-pki");
        std::fs::create_dir_all(&pki_dir).expect("client pki dir");
        let cert_copy = pki_dir.join("client.der");
        let key_copy = pki_dir.join("client-key.pem");
        std::fs::copy(cert, &cert_copy).expect("copy client cert");
        std::fs::copy(key, &key_copy).expect("copy client key");
        ClientFiles {
            _dir: dir,
            pki_dir,
            cert: cert_copy,
            key: key_copy,
        }
    }

    /// The `cert` block of a source config for this client, with the given
    /// explicit server trust entries.
    pub fn cert_block(&self, trust_server_certs: &[PathBuf]) -> Value {
        json!({
            "cert": self.cert,
            "key": self.key,
            "pki_dir": self.pki_dir,
            "trust_server_certs": trust_server_certs,
        })
    }
}

/// A cloneable writer that updates variables and notifies subscriptions.
#[derive(Clone)]
pub struct ServerControl {
    handle: ServerHandle,
    manager: Arc<TestNodeManager>,
}

impl ServerControl {
    /// Sets a variable's value, notifying subscriptions.
    pub fn set_value(&self, id: &NodeId, value: DataValue) {
        self.manager
            .set_value(self.handle.subscriptions(), id, None, value)
            .expect("set variable");
    }

    /// Sets `temp` and `counter` from one tick value.
    pub fn tick(&self, nodes: &TestNodes, i: i32) {
        self.set_value(&nodes.temp, DataValue::new_now(20.0 + f64::from(i)));
        self.set_value(&nodes.counter, DataValue::new_now(i));
    }
}

/// A running in-process server.
pub struct TestServer {
    /// The address the server listens on.
    pub addr: SocketAddr,
    /// The endpoint URL the server advertises, for source configs. This is
    /// the proxy's address when the server sits behind one.
    pub endpoint: String,
    handle: ServerHandle,
    task: JoinHandle<Result<(), String>>,
    /// Keeps the server's PKI dir alive until drop.
    _pki: tempfile::TempDir,
}

impl TestServer {
    /// Cancels the server and waits for its task.
    pub async fn stop(self) {
        self.handle.cancel();
        let _ = self.task.await;
    }

    /// A writer for the test's own updates.
    pub fn control(&self) -> ServerControl {
        ServerControl {
            handle: self.handle.clone(),
            manager: self
                .handle
                .node_managers()
                .get_of_type::<TestNodeManager>()
                .expect("test node manager"),
        }
    }
}

/// Binds a loopback listener, retrying briefly while an earlier server's
/// socket drains.
pub async fn bind(port: u16) -> TcpListener {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        match TcpListener::bind(("127.0.0.1", port)).await {
            Ok(listener) => return listener,
            Err(error) => {
                if tokio::time::Instant::now() > deadline {
                    panic!("cannot bind port {port}: {error}");
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
}

/// Starts a server on `port` (0 for any free port).
pub async fn start_server(port: u16) -> (TestServer, TestNodes) {
    let listener = bind(port).await;
    let advertised = listener.local_addr().expect("local addr").port();
    start_on(listener, advertised).await
}

/// Starts a server on an existing listener, advertising `advertised_port`
/// in its endpoint URLs. A different advertised port puts a proxy between
/// the client and the server.
pub async fn start_on(listener: TcpListener, advertised_port: u16) -> (TestServer, TestNodes) {
    let addr = listener.local_addr().expect("local addr");
    let endpoint = format!("opc.tcp://127.0.0.1:{advertised_port}");
    let pki = tempfile::tempdir().expect("server pki dir");
    let material = Material::shared();
    trust_certificate(pki.path(), &material.client_cert);

    let plain_tokens: &[&str] = &[ANONYMOUS_USER_TOKEN_ID, USERNAME];
    let secured_tokens: &[&str] = &[ANONYMOUS_USER_TOKEN_ID, USERNAME, CERT_USER];
    let mut builder = ServerBuilder::new()
        .application_name("thadam-opcua-test")
        .application_uri(SERVER_APP_URI)
        .product_uri("urn:thadam:opcua:test")
        .host("127.0.0.1")
        .port(advertised_port)
        .discovery_urls(vec![endpoint.clone()])
        .pki_dir(pki.path())
        .certificate_path(material.server_cert.clone())
        .private_key_path(material.server_key.clone())
        .trust_client_certs(false)
        .add_user_token(USERNAME, ServerUserToken::user_pass(USERNAME, PASSWORD))
        .add_user_token(
            CERT_USER,
            ServerUserToken::x509(CERT_USER, &material.user_cert),
        )
        .with_node_manager(simple_node_manager(
            NamespaceMetadata {
                namespace_uri: NAMESPACE.to_owned(),
                ..Default::default()
            },
            "test",
        ))
        .add_endpoint(
            "none",
            (
                "/",
                SecurityPolicy::None,
                MessageSecurityMode::None,
                plain_tokens,
            ),
        );
    for (id, policy, mode) in [
        (
            "basic256sha256-sign-encrypt",
            SecurityPolicy::Basic256Sha256,
            MessageSecurityMode::SignAndEncrypt,
        ),
        (
            "aes128-sign",
            SecurityPolicy::Aes128Sha256RsaOaep,
            MessageSecurityMode::Sign,
        ),
        (
            "aes128-sign-encrypt",
            SecurityPolicy::Aes128Sha256RsaOaep,
            MessageSecurityMode::SignAndEncrypt,
        ),
        (
            "aes256-sign",
            SecurityPolicy::Aes256Sha256RsaPss,
            MessageSecurityMode::Sign,
        ),
        (
            "aes256-sign-encrypt",
            SecurityPolicy::Aes256Sha256RsaPss,
            MessageSecurityMode::SignAndEncrypt,
        ),
    ] {
        builder = builder.add_endpoint(id, ("/", policy, mode, secured_tokens));
    }

    let (server, handle) = builder.build().expect("server builds");
    let task = tokio::spawn(server.run_with(listener));

    let ns = handle
        .get_namespace_index(NAMESPACE)
        .expect("test namespace registered");
    let nodes = TestNodes {
        temp: NodeId::new(ns, 1u32),
        running: NodeId::new(ns, 2u32),
        label: NodeId::new(ns, 3u32),
        accel: NodeId::new(ns, 4u32),
        counter: NodeId::new(ns, 5u32),
    };
    let manager = handle
        .node_managers()
        .get_of_type::<TestNodeManager>()
        .expect("test node manager");
    {
        let mut address_space = manager.address_space().write();
        add_variable(
            &mut address_space,
            &nodes.temp,
            "temp",
            20.0f64,
            DataTypeId::Double,
        );
        add_variable(
            &mut address_space,
            &nodes.running,
            "running",
            true,
            DataTypeId::Boolean,
        );
        add_variable(
            &mut address_space,
            &nodes.label,
            "label",
            "pump".to_owned(),
            DataTypeId::String,
        );
        add_variable(
            &mut address_space,
            &nodes.accel,
            "accel",
            Variant::Array(Box::new(Array {
                value_type: VariantScalarTypeId::Double,
                values: vec![Variant::Double(1.5), Variant::Double(-2.5)],
                dimensions: None,
            })),
            DataTypeId::Double,
        );
        add_variable(
            &mut address_space,
            &nodes.counter,
            "counter",
            0i32,
            DataTypeId::Int32,
        );
    }

    (
        TestServer {
            addr,
            endpoint,
            handle,
            task,
            _pki: pki,
        },
        nodes,
    )
}

/// Copies `cert` into `<pki>/trusted` under the name the server looks it
/// up by, as an operator would.
fn trust_certificate(pki: &Path, cert: &Path) {
    let trusted = pki.join("trusted");
    std::fs::create_dir_all(&trusted).expect("trusted dir");
    let x509 = CertificateStore::read_cert(cert).expect("read certificate");
    let name = CertificateStore::cert_file_name(&x509);
    std::fs::write(trusted.join(name), x509.to_der().expect("der")).expect("trust certificate");
}

/// Inserts a variable under the Objects folder.
fn add_variable(
    address_space: &mut AddressSpace,
    id: &NodeId,
    name: &str,
    value: impl Into<Variant>,
    data_type: DataTypeId,
) {
    let node: NodeType = VariableBuilder::new(id, name, name)
        .value(value.into())
        .data_type(data_type)
        .access_level(AccessLevel::CURRENT_READ | AccessLevel::CURRENT_WRITE)
        .user_access_level(AccessLevel::CURRENT_READ | AccessLevel::CURRENT_WRITE)
        .build()
        .into();
    address_space.insert(node, None::<&[(&NodeId, &NodeId, ReferenceDirection)]>);
    let objects: NodeId = ObjectId::ObjectsFolder.into();
    let type_def: NodeId = VariableTypeId::BaseDataVariableType.into();
    address_space.insert_reference(&objects, id, ReferenceTypeId::Organizes);
    address_space.insert_reference(id, &type_def, ReferenceTypeId::HasTypeDefinition);
}

/// A TCP relay that counts accepted connections. Every connection the
/// client makes to the advertised port passes through it, so a test can
/// observe reconnect attempts.
pub struct CountingProxy {
    /// The port the client connects to.
    pub port: u16,
    accepted: Arc<AtomicU32>,
    _task: JoinHandle<()>,
}

impl CountingProxy {
    /// Relays to `target`, listening on a free loopback port.
    pub async fn start(target: SocketAddr) -> Self {
        let listener = bind(0).await;
        let port = listener.local_addr().expect("proxy addr").port();
        let accepted = Arc::new(AtomicU32::new(0));
        let counter = Arc::clone(&accepted);
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    return;
                };
                counter.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    if let Ok(mut outbound) = TcpStream::connect(target).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
            }
        });
        CountingProxy {
            port,
            accepted,
            _task: task,
        }
    }

    /// Connections accepted so far.
    pub fn accepted(&self) -> u32 {
        self.accepted.load(Ordering::Relaxed)
    }
}

/// Tick task: updates `temp`/`counter` every `interval` until cancelled.
pub fn spin_ticks(
    control: ServerControl,
    nodes: TestNodes,
    interval: Duration,
) -> CancellationToken {
    let token = CancellationToken::new();
    let cancel = token.clone();
    tokio::spawn(async move {
        let mut i = 0i32;
        loop {
            tokio::select! {
                () = tokio::time::sleep(interval) => {}
                () = cancel.cancelled() => return,
            }
            i = i.wrapping_add(1);
            control.tick(&nodes, i);
        }
    });
    token
}

/// A Bad-status data value for the null-value test.
pub fn bad_sensor_value(v: f64) -> DataValue {
    let mut dv = DataValue::new_now(v);
    dv.status = Some(StatusCode::BadSensorFailure);
    dv
}

/// The shutdown-scoped context a source opens with.
pub fn ctx(shutdown: &Shutdown) -> SourceContext {
    SourceContext::new(
        SourceId::new("opcua-test"),
        shutdown.clone(),
        Arc::new(SystemClock::new()),
    )
}

/// Batch limits that keep the tests' latency low.
pub fn limits() -> BatchLimits {
    BatchLimits {
        max_messages: 16,
        linger: Duration::from_millis(5),
        ..BatchLimits::default()
    }
}

/// Builds and opens a source from a config JSON document.
pub async fn open_source(cfg: Value, shutdown: &Shutdown) -> Box<dyn Source> {
    let build = BuildContext::new(
        "opcua-1",
        Arc::new(SystemClock::new()),
        std::path::PathBuf::new(),
    );
    let mut source = OpcUaSourceFactory::new()
        .build(cfg, &build)
        .expect("config builds");
    source.open(&ctx(shutdown)).await.expect("open");
    source
}

/// Collects messages until `until` returns true or the timeout passes.
pub async fn collect_until(
    source: &Mutex<Box<dyn Source>>,
    within: Duration,
    mut until: impl FnMut(&RawMessage) -> bool,
) -> Vec<RawMessage> {
    let deadline = tokio::time::Instant::now() + within;
    let mut out = Vec::new();
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return out;
        }
        let batch = {
            let mut guard = source.lock().await;
            match tokio::time::timeout(remaining, guard.next_batch(limits())).await {
                Ok(Ok(Some(batch))) => batch.batch.messages,
                Ok(Ok(None)) => Vec::new(),
                Ok(Err(error)) => panic!("next_batch failed: {error}"),
                Err(_) => return out, // publish gap longer than the remaining budget
            }
        };
        let done = batch.iter().any(&mut until);
        out.extend(batch);
        if done {
            return out;
        }
    }
}

/// A message's JSON payload.
pub fn payload(message: &RawMessage) -> Value {
    serde_json::from_slice(&message.payload).expect("json payload")
}

/// A subscribe-mode config over every test node, with fast timing.
pub fn base_config(server: &TestServer, nodes: &TestNodes) -> Value {
    json!({
        "endpoint": server.endpoint,
        "security_policy": "none",
        "security_mode": "none",
        "subscribe": {"publishing_interval": "100ms", "sampling_interval": "25ms"},
        "reconnect": {"min_backoff": "100ms", "max_backoff": "500ms",
                      "max_handshakes_per_minute": 6000},
        "nodes": nodes_json(nodes)
    })
}

/// The five test nodes in the order the tests expect.
pub fn nodes_json(nodes: &TestNodes) -> Value {
    json!([
        {"node_id": TestNodes::id_str(&nodes.temp), "name": "temp"},
        {"node_id": TestNodes::id_str(&nodes.running), "name": "running"},
        {"node_id": TestNodes::id_str(&nodes.label), "name": "label"},
        {"node_id": TestNodes::id_str(&nodes.accel), "name": "accel"},
        {"node_id": TestNodes::id_str(&nodes.counter), "name": "counter"}
    ])
}

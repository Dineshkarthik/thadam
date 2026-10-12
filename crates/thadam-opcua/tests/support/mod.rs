//! An in-process OPC UA server for the source's tests, mirroring the
//! in-process Modbus test server: a handful of variables on a timer, a
//! controllable lifecycle, and username-token support for the
//! encrypted-policy test.

#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use opcua::crypto::{CertificateStore, SecurityPolicy};
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
use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// The test namespace URI; variables live under it.
pub const NAMESPACE: &str = "urn:thadam:opcua:test";

/// The test username (`auth` with `kind: username`).
pub const USERNAME: &str = "thadam";
/// The test user's password.
pub const PASSWORD: &str = "correct-horse";

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
    /// The bound address the source connects to.
    pub addr: SocketAddr,
    /// `opc.tcp://addr` — the endpoint for source configs.
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
async fn bind(port: u16) -> TcpListener {
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

/// What kind of endpoint the test server offers.
#[derive(Clone, Copy)]
pub enum ServerKind {
    /// `none`/`none` security; anonymous and username tokens.
    Plain,
    /// `basic256sha256`/`sign_and_encrypt` with a generated key pair.
    Encrypted,
}

/// Starts a server on port 0 (or `port`), with the test variables inserted.
pub async fn start_server(kind: ServerKind, port: u16) -> (TestServer, TestNodes) {
    let listener = bind(port).await;
    let addr = listener.local_addr().expect("local addr");
    let endpoint = format!("opc.tcp://127.0.0.1:{}", addr.port());
    let pki = tempfile::tempdir().expect("pki dir");

    let token_ids: &[&str] = &[ANONYMOUS_USER_TOKEN_ID, USERNAME];
    let mut builder = ServerBuilder::new()
        .application_name("thadam-opcua-test")
        .application_uri("urn:thadam:opcua:test-server")
        .product_uri("urn:thadam:opcua:test")
        .host("127.0.0.1")
        .port(addr.port())
        .discovery_urls(vec![endpoint.clone()])
        .pki_dir(pki.path())
        .add_user_token(USERNAME, ServerUserToken::user_pass(USERNAME, PASSWORD))
        .with_node_manager(simple_node_manager(
            NamespaceMetadata {
                namespace_uri: NAMESPACE.to_owned(),
                ..Default::default()
            },
            "test",
        ));
    builder = match kind {
        ServerKind::Plain => builder.create_sample_keypair(false).add_endpoint(
            "none",
            (
                "/",
                SecurityPolicy::None,
                MessageSecurityMode::None,
                token_ids,
            ),
        ),
        ServerKind::Encrypted => builder
            .create_sample_keypair(true)
            .trust_client_certs(true)
            .add_endpoint(
                "basic256",
                (
                    "/",
                    SecurityPolicy::Basic256Sha256,
                    MessageSecurityMode::SignAndEncrypt,
                    token_ids,
                ),
            ),
    };

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

/// Generates a self-signed client certificate pair in `dir` for the
/// encrypted-policy test. Returns `(cert, key, pki_dir)`.
pub fn make_client_cert(
    dir: &Path,
) -> (std::path::PathBuf, std::path::PathBuf, std::path::PathBuf) {
    let cert = dir.join("cert.der");
    let key = dir.join("key.pem");
    let description = ApplicationDescription {
        application_uri: opcua::types::UAString::from("urn:thadam:opcua:test-client".to_owned()),
        product_uri: opcua::types::UAString::from("urn:thadam:opcua:test".to_owned()),
        application_name: LocalizedText::new("en", "thadam test client"),
        ..Default::default()
    };
    CertificateStore::create_certificate_and_key(&description.into(), true, &cert, &key)
        .expect("client cert");
    (cert, key, dir.to_path_buf())
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

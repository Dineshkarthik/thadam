//! End-to-end tests of `ModbusSource` against an in-process Modbus TCP server.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::future::{self, Pending};
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use serde_json::{Value, json};
use thadam_core::{
    BatchLimits, Clock, RawMessage, Shutdown, Source, SourceContext, SourceId, SystemClock,
};
use thadam_modbus::{ModbusConfig, ModbusSource};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio::task::JoinHandle;
use tokio_modbus::server::Service;
use tokio_modbus::server::tcp::{Server, accept_tcp_connection};
use tokio_modbus::{ExceptionCode, Request, Response};

const WAIT: Duration = Duration::from_secs(10);

#[derive(Clone)]
struct Device {
    holding: Arc<Vec<u16>>,
    input: Arc<Vec<u16>>,
    coils: Arc<Vec<bool>>,
    discrete: Arc<Vec<bool>>,
}

fn device() -> Device {
    Device {
        holding: Arc::new(vec![0x4148, 0x0000, 7]),
        input: Arc::new(vec![1, 2, 3, 4]),
        coils: Arc::new(vec![true, false, true]),
        discrete: Arc::new(vec![false, true]),
    }
}

fn slice<T: Clone>(table: &[T], addr: u16, count: u16) -> Result<Vec<T>, ExceptionCode> {
    let start = usize::from(addr);
    table
        .get(start..start + usize::from(count))
        .map(<[T]>::to_vec)
        .ok_or(ExceptionCode::IllegalDataAddress)
}

impl Service for Device {
    type Request = Request<'static>;
    type Response = Response;
    type Exception = ExceptionCode;
    type Future = future::Ready<Result<Response, ExceptionCode>>;

    fn call(&self, req: Request<'static>) -> Self::Future {
        future::ready(match req {
            Request::ReadHoldingRegisters(a, n) => {
                slice(&self.holding, a, n).map(Response::ReadHoldingRegisters)
            }
            Request::ReadInputRegisters(a, n) => {
                slice(&self.input, a, n).map(Response::ReadInputRegisters)
            }
            Request::ReadCoils(a, n) => slice(&self.coils, a, n).map(Response::ReadCoils),
            Request::ReadDiscreteInputs(a, n) => {
                slice(&self.discrete, a, n).map(Response::ReadDiscreteInputs)
            }
            _ => Err(ExceptionCode::IllegalFunction),
        })
    }
}

/// Accepts connections and never answers, counting the requests it receives.
#[derive(Clone)]
struct Silent {
    requests: Arc<AtomicUsize>,
}

impl Service for Silent {
    type Request = Request<'static>;
    type Response = Response;
    type Exception = ExceptionCode;
    type Future = Pending<Result<Response, ExceptionCode>>;

    fn call(&self, _req: Request<'static>) -> Self::Future {
        self.requests.fetch_add(1, Ordering::SeqCst);
        future::pending()
    }
}

/// Serves `service` on `listener` until the returned sender is fired or dropped.
fn serve<S>(listener: TcpListener, service: S) -> (oneshot::Sender<()>, JoinHandle<()>)
where
    S: Service<Request = Request<'static>> + Clone + Send + Sync + 'static,
    S::Future: Send,
{
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        let on_connected = |stream, peer| {
            let service = service.clone();
            async move { accept_tcp_connection(stream, peer, |_| Ok(Some(service.clone()))) }
        };
        let _ = Server::new(listener)
            .serve_until(&on_connected, |_| {}, async {
                let _ = stop_rx.await;
            })
            .await;
    });
    (stop_tx, task)
}

async fn bind_free_port() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local address");
    (listener, addr)
}

async fn rebind(addr: SocketAddr) -> TcpListener {
    for _ in 0..50 {
        if let Ok(listener) = TcpListener::bind(addr).await {
            return listener;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("could not rebind {addr}");
}

fn config(addr: SocketAddr, reads: Value) -> ModbusConfig {
    ModbusConfig::from_value(&json!({
        "address": addr.to_string(),
        "interval": "50ms",
        "timeout": "500ms",
        "backoff_max": "200ms",
        "reads": reads,
    }))
    .expect("valid config")
}

async fn open(config: ModbusConfig) -> (ModbusSource, Shutdown) {
    let shutdown = Shutdown::new();
    let ctx = SourceContext::new(
        SourceId::new("modbus-test"),
        shutdown.clone(),
        Arc::new(SystemClock::new()) as Arc<dyn Clock>,
    );
    let mut source = ModbusSource::new(config);
    source.open(&ctx).await.expect("open");
    (source, shutdown)
}

async fn next_message(source: &mut ModbusSource) -> RawMessage {
    let limits = BatchLimits {
        max_messages: 1,
        max_bytes: usize::MAX,
        linger: Duration::from_millis(1),
    };
    let batch = tokio::time::timeout(WAIT, source.next_batch(limits))
        .await
        .expect("a poll should arrive")
        .expect("next_batch succeeds")
        .expect("the source is open");
    let mut messages = batch.batch.messages;
    assert_eq!(messages.len(), 1);
    messages.remove(0)
}

fn payload(message: &RawMessage) -> Value {
    serde_json::from_slice(&message.payload).expect("JSON payload")
}

#[tokio::test]
async fn every_table_becomes_one_json_message_per_poll() {
    let (listener, addr) = bind_free_port().await;
    let (stop, task) = serve(listener, device());

    let reads = json!([
        {"name": "setpoint", "kind": "holding", "start": 0, "count": 2, "data_type": "f32"},
        {"name": "count", "kind": "holding", "start": 2, "count": 1, "data_type": "u16"},
        {"name": "temps", "kind": "input", "start": 0, "count": 2, "data_type": "i16"},
        {"name": "pair", "kind": "input", "start": 2, "count": 2, "data_type": "u32",
         "word_order": "little"},
        {"name": "alarms", "kind": "coil", "start": 0, "count": 3, "data_type": "bool"},
        {"name": "lamp", "kind": "coil", "start": 0, "count": 1, "data_type": "bool"},
        {"name": "contacts", "kind": "discrete", "start": 0, "count": 2, "data_type": "bool"},
    ]);
    let (mut source, shutdown) = open(config(addr, reads)).await;

    let message = next_message(&mut source).await;
    let value = payload(&message);
    assert_eq!(value["setpoint"], json!(12.5));
    assert_eq!(value["count"], json!(7));
    assert_eq!(value["temps"], json!([1, 2]));
    assert_eq!(value["pair"], json!(262147));
    assert_eq!(value["alarms"], json!([true, false, true]));
    assert_eq!(value["lamp"], json!(true));
    assert_eq!(value["contacts"], json!([false, true]));
    assert!(value["ts_us"].as_i64().is_some_and(|t| t > 0), "{value}");

    assert_eq!(
        message.attrs.get("modbus.address"),
        Some(addr.to_string().as_str())
    );
    assert_eq!(message.attrs.get("modbus.unit_id"), Some("1"));
    assert_eq!(message.attrs.get("topic"), Some("modbus/1"));
    assert_eq!(message.attrs.get("modbus.exceptions"), None);

    source.close().await.expect("close");
    shutdown.trigger();
    let _ = stop.send(());
    let _ = task.await;
}

#[tokio::test]
async fn refused_read_is_null_and_named_while_the_session_stays_up() {
    let (listener, addr) = bind_free_port().await;
    let (stop, task) = serve(listener, device());

    let reads = json!([
        {"name": "good", "kind": "holding", "start": 0, "count": 1, "data_type": "u16"},
        {"name": "bad", "kind": "holding", "start": 100, "count": 1, "data_type": "u16"},
    ]);
    let (mut source, _shutdown) = open(config(addr, reads)).await;

    for _ in 0..2 {
        let message = next_message(&mut source).await;
        let value = payload(&message);
        assert_eq!(value["good"], json!(0x4148));
        assert_eq!(value["bad"], Value::Null);
        assert_eq!(message.attrs.get("modbus.exceptions"), Some("bad"));
    }

    source.close().await.expect("close");
    let _ = stop.send(());
    let _ = task.await;
}

#[tokio::test]
async fn server_down_then_recovery_keeps_polling() {
    let (listener, addr) = bind_free_port().await;
    drop(listener);

    let reads = json!([
        {"name": "count", "kind": "holding", "start": 2, "count": 1, "data_type": "u16"},
    ]);
    let (mut source, _shutdown) = open(config(addr, reads)).await;

    tokio::time::sleep(Duration::from_millis(400)).await;
    let (stop, task) = serve(rebind(addr).await, device());

    let value = payload(&next_message(&mut source).await);
    assert_eq!(value["count"], json!(7));

    source.close().await.expect("close");
    let _ = stop.send(());
    let _ = task.await;
}

#[tokio::test]
async fn unanswered_request_times_out_and_reconnects() {
    let (listener, addr) = bind_free_port().await;
    let requests = Arc::new(AtomicUsize::new(0));
    let (stop, task) = serve(
        listener,
        Silent {
            requests: Arc::clone(&requests),
        },
    );

    let reads = json!([
        {"name": "count", "kind": "holding", "start": 2, "count": 1, "data_type": "u16"},
    ]);
    let (mut source, _shutdown) = open(config(addr, reads)).await;

    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        requests.load(Ordering::SeqCst) >= 2,
        "each timed-out read should drop the session and retry"
    );

    let started = std::time::Instant::now();
    source.close().await.expect("close");
    assert!(started.elapsed() < Duration::from_secs(5));
    let _ = stop.send(());
    let _ = task.await;
}

#[tokio::test]
async fn close_before_any_poll_returns_none() {
    let (listener, addr) = bind_free_port().await;
    drop(listener);
    let reads = json!([
        {"name": "count", "kind": "holding", "start": 2, "count": 1, "data_type": "u16"},
    ]);
    let (mut source, _shutdown) = open(config(addr, reads)).await;
    source.close().await.expect("close");

    let limits = BatchLimits {
        max_messages: 1,
        max_bytes: usize::MAX,
        linger: Duration::from_millis(1),
    };
    let next = source.next_batch(limits).await.expect("next_batch");
    assert!(next.is_none());
}

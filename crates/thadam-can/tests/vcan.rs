//! End-to-end tests of `CanSource` on Linux.
//!
//! Most tests need the `vcan` kernel module and a `vcan0` interface, so they
//! are `#[ignore]`d. Run them on a Linux host (or a privileged container on a
//! Linux kernel with `CONFIG_CAN_VCAN`):
//!
//! ```sh
//! ip link add dev vcan0 type vcan && ip link set up vcan0
//! cargo test -p thadam-can --test vcan -- --include-ignored
//! # or: cargo nextest run -p thadam-can --run-ignored all
//! ```
//!
//! The CAN FD test raises the interface MTU to 72 itself (`ip link set vcan0
//! mtu 72`), so `iproute2` must be installed. Docker Desktop's Linux VM is
//! built without `CONFIG_CAN`, so there the ignored tests cannot run; the
//! non-ignored test below and `cargo check`/`clippy` still do.
#![cfg(target_os = "linux")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::fs;
use std::process::Command;
use std::sync::Arc;
use std::time::Duration;

use socketcan::tokio::{CanFdSocket, CanSocket};
use socketcan::{CanDataFrame, CanFdFrame, CanFrame, EmbeddedFrame as _, ExtendedId, StandardId};
use thadam_can::{CanConfig, CanSource};
use thadam_core::{
    BatchLimits, Clock as _, RawMessage, Shutdown, Source, SourceContext, SourceId, SystemClock,
    codes,
};

const IFACE: &str = "vcan0";
const WAIT: Duration = Duration::from_secs(10);

/// Capacity of the source's internal queue (`CHANNEL_CAPACITY` in
/// `src/source.rs`).
const QUEUE: usize = 4096;

const DBC: &str = r#"
VERSION ""

NS_ :

BS_:

BU_: TESTER

BO_ 291 EngineData: 8 TESTER
 SG_ Rpm : 0|16@1+ (0.25,0) [0|16383.75] "rpm" TESTER
 SG_ CoolantTemp : 16|8@1- (1,-40) [-40|215] "degC" TESTER
"#;

fn build_source(config: serde_json::Value) -> CanSource {
    let config = CanConfig::from_value(&config).expect("valid config");
    CanSource::new(config).expect("source builds")
}

async fn open_source(source: &mut CanSource) -> Shutdown {
    let shutdown = Shutdown::new();
    let ctx = SourceContext::new(
        SourceId::new("vcan-test"),
        shutdown.clone(),
        Arc::new(SystemClock::new()),
    );
    source.open(&ctx).await.expect("open on vcan0");
    shutdown
}

async fn finish(source: &mut CanSource, shutdown: &Shutdown) {
    shutdown.trigger();
    source.close().await.expect("close");
}

async fn collect(source: &mut CanSource, wanted: usize) -> Vec<RawMessage> {
    let limits = BatchLimits {
        max_messages: 64,
        linger: Duration::from_millis(50),
        ..BatchLimits::default()
    };
    let mut out = Vec::new();
    tokio::time::timeout(WAIT, async {
        while out.len() < wanted {
            match source.next_batch(limits).await.expect("batch") {
                Some(batch) => out.extend(batch.batch.messages),
                None => break,
            }
        }
    })
    .await
    .expect("wanted messages within WAIT");
    out
}

fn classic(id: u16, data: &[u8]) -> CanDataFrame {
    CanDataFrame::new(StandardId::new(id).expect("id"), data).expect("frame")
}

fn fd(id: impl Into<socketcan::Id>, data: &[u8]) -> CanFdFrame {
    CanFdFrame::new(id, data).expect("fd frame")
}

async fn send(socket: &CanSocket, frame: CanDataFrame) {
    socket
        .write_frame(CanFrame::Data(frame))
        .await
        .expect("send");
}

/// CAN FD frames only pass a virtual interface whose MTU is the FD MTU.
fn enable_fd_on_interface() {
    let status = Command::new("ip")
        .args(["link", "set", IFACE, "mtu", "72"])
        .status()
        .expect("`ip` (iproute2) must be installed to run the CAN FD test");
    assert!(status.success(), "ip link set {IFACE} mtu 72 failed");
}

#[tokio::test]
async fn a_missing_interface_is_a_transient_connect_error() {
    let mut source = build_source(serde_json::json!({"interface": "thadamnone0"}));
    let shutdown = Shutdown::new();
    let ctx = SourceContext::new(
        SourceId::new("vcan-test"),
        shutdown,
        Arc::new(SystemClock::new()),
    );
    let error = source.open(&ctx).await.expect_err("no such interface");
    assert_eq!(error.code, codes::CONNECTOR_CONNECT_FAILED);
    assert!(error.is_transient(), "{error}");
    assert!(error.message.contains("thadamnone0"), "{error}");
}

#[tokio::test]
#[ignore = "needs a Linux vcan0 interface"]
async fn raw_classic_frames_are_emitted_with_attributes() {
    let mut source = build_source(serde_json::json!({"interface": IFACE}));
    let shutdown = open_source(&mut source).await;

    let tx = CanSocket::open(IFACE).expect("writer");
    send(&tx, classic(0x7B2, &[1, 2, 3, 4])).await;

    let message = collect(&mut source, 1).await.remove(0);
    assert_eq!(message.payload.as_ref(), &[1, 2, 3, 4]);
    assert_eq!(message.attrs.get("can.id"), Some("7B2"));
    assert_eq!(message.attrs.get("can.extended"), Some("false"));
    assert_eq!(message.attrs.get("can.fd"), Some("false"));
    assert_eq!(message.attrs.get("can.dlc"), Some("4"));
    assert_eq!(message.attrs.get("can.interface"), Some(IFACE));
    assert_eq!(message.attrs.get("topic"), Some("can/vcan0/7B2"));

    finish(&mut source, &shutdown).await;
}

#[tokio::test]
#[ignore = "needs a Linux vcan0 interface"]
async fn kernel_filters_pass_only_matching_ids() {
    let config = serde_json::json!({
        "interface": IFACE,
        "filters": [{"id": "0x120", "mask": "0x7F0"}]
    });
    let mut source = build_source(config);
    let shutdown = open_source(&mut source).await;

    let tx = CanSocket::open(IFACE).expect("writer");
    // 0x120 and 0x121 share the masked id 0x120: accepted.
    send(&tx, classic(0x120, &[1])).await;
    send(&tx, classic(0x121, &[2])).await;
    // 0x110 and 0x200 do not match.
    send(&tx, classic(0x110, &[3])).await;
    send(&tx, classic(0x200, &[4])).await;
    // An extended frame with the same low bits does not match a standard
    // filter.
    let extended = CanDataFrame::new(ExtendedId::new(0x120).expect("id"), &[5]).expect("frame");
    send(&tx, extended).await;
    // A final matching frame proves the rejected ones were skipped, not slow.
    send(&tx, classic(0x12F, &[6])).await;

    let messages = collect(&mut source, 3).await;
    let ids: Vec<&str> = messages
        .iter()
        .filter_map(|m| m.attrs.get("can.id"))
        .collect();
    assert_eq!(ids, ["120", "121", "12F"]);

    finish(&mut source, &shutdown).await;
}

#[tokio::test]
#[ignore = "needs a Linux vcan0 interface (iproute2 for the MTU change)"]
async fn fd_frames_are_received_when_fd_is_on() {
    enable_fd_on_interface();
    let config = serde_json::json!({"interface": IFACE, "fd": true});
    let mut source = build_source(config);
    let shutdown = open_source(&mut source).await;

    let tx = CanFdSocket::open(IFACE).expect("fd writer");
    tx.write_frame(&classic(0x100, &[9]))
        .await
        .expect("classic");
    let payload: Vec<u8> = (0u8..24).collect();
    tx.write_frame(&fd(StandardId::new(0x101).expect("id"), &payload))
        .await
        .expect("fd");
    // 12 is a valid FD length; other sizes are padded up by the kernel.
    tx.write_frame(&fd(ExtendedId::new(0x1234).expect("id"), &[5u8; 12]))
        .await
        .expect("extended fd");

    let messages = collect(&mut source, 3).await;
    let find = |id: &str| {
        messages
            .iter()
            .find(|m| m.attrs.get("can.id") == Some(id))
            .unwrap_or_else(|| panic!("message {id} present"))
    };
    let classic_message = find("100");
    assert_eq!(classic_message.attrs.get("can.fd"), Some("false"));
    assert_eq!(classic_message.payload.as_ref(), &[9]);
    let fd_message = find("101");
    assert_eq!(fd_message.attrs.get("can.fd"), Some("true"));
    assert_eq!(fd_message.attrs.get("can.dlc"), Some("24"));
    assert_eq!(fd_message.payload.as_ref(), payload.as_slice());
    let ext = find("00001234");
    assert_eq!(ext.attrs.get("can.extended"), Some("true"));
    assert_eq!(ext.attrs.get("can.fd"), Some("true"));
    assert_eq!(ext.payload.as_ref(), &[5u8; 12]);

    finish(&mut source, &shutdown).await;
}

#[tokio::test]
#[ignore = "needs a Linux vcan0 interface"]
async fn decoded_frames_carry_signal_values_and_ts_us() {
    let dir = tempfile::tempdir().expect("temp dir");
    let dbc_path = dir.path().join("engine.dbc");
    fs::write(&dbc_path, DBC).expect("write dbc");

    let config = serde_json::json!({
        "interface": IFACE, "emit": "decoded", "dbc": dbc_path,
    });
    let mut source = build_source(config);
    let shutdown = open_source(&mut source).await;

    let before = SystemClock::new().now().as_micros();
    let tx = CanSocket::open(IFACE).expect("writer");
    // Rpm raw 12000 -> 3000.0 rpm; CoolantTemp raw 100 -> 60.0 degC.
    send(&tx, classic(291, &[0xE0, 0x2E, 100, 0, 0, 0, 0, 0])).await;
    // An id outside the DBC passes through raw under the default
    // `unknown: raw`.
    send(&tx, classic(0x777, &[7])).await;

    let messages = collect(&mut source, 2).await;
    let decoded = messages
        .iter()
        .find(|m| m.attrs.get("topic") == Some("can/vcan0/EngineData"))
        .expect("decoded message");
    let payload: serde_json::Value = serde_json::from_slice(&decoded.payload).expect("json");
    assert_eq!(payload["Rpm"], serde_json::json!(3000.0));
    assert_eq!(payload["CoolantTemp"], serde_json::json!(60.0));
    let ts = payload["ts_us"].as_i64().expect("ts_us");
    assert!(ts >= before && ts <= SystemClock::new().now().as_micros());

    let raw = messages
        .iter()
        .find(|m| m.attrs.get("topic") == Some("can/vcan0/777"))
        .expect("raw fallback");
    assert_eq!(raw.payload.as_ref(), &[7]);

    finish(&mut source, &shutdown).await;
}

#[tokio::test]
#[ignore = "needs a Linux vcan0 interface"]
async fn unknown_ids_are_dropped_when_configured() {
    let dir = tempfile::tempdir().expect("temp dir");
    let dbc_path = dir.path().join("engine.dbc");
    fs::write(&dbc_path, DBC).expect("write dbc");

    let config = serde_json::json!({
        "interface": IFACE, "emit": "decoded", "dbc": dbc_path, "unknown": "drop",
    });
    let mut source = build_source(config);
    let shutdown = open_source(&mut source).await;

    let tx = CanSocket::open(IFACE).expect("writer");
    send(&tx, classic(0x777, &[7])).await;
    send(&tx, classic(291, &[0xE0, 0x2E, 0, 0, 0, 0, 0, 0])).await;

    let messages = collect(&mut source, 1).await;
    assert_eq!(messages[0].attrs.get("topic"), Some("can/vcan0/EngineData"));

    finish(&mut source, &shutdown).await;
}

/// Backpressure: nobody pulls while the bus is flooded, so the source's queue
/// fills and further frames are dropped and counted. The socket keeps being
/// drained meanwhile (liveness does not depend on the consumer): the kernel's
/// own receive buffer holds only a few hundred frames, so counting thousands
/// of drops proves the receive task kept reading.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "needs a Linux vcan0 interface"]
async fn a_stalled_consumer_drops_and_counts_frames() {
    const SENT: usize = 20_000;

    let mut source = build_source(serde_json::json!({"interface": IFACE}));
    let shutdown = open_source(&mut source).await;

    let tx = CanSocket::open(IFACE).expect("writer");
    for i in 0..SENT {
        send(&tx, classic(0x55, &[(i % 256) as u8; 8])).await;
        if i % 128 == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Drain what the queue held; it never holds more than its capacity.
    let limits = BatchLimits {
        max_messages: 1024,
        linger: Duration::from_millis(20),
        ..BatchLimits::default()
    };
    let mut queued = 0usize;
    while let Ok(next) =
        tokio::time::timeout(Duration::from_millis(500), source.next_batch(limits)).await
    {
        queued += next.expect("batch").expect("open").batch.messages.len();
    }
    assert!(queued > 0, "the queue held the first frames");
    assert!(queued <= QUEUE, "queued {queued} > capacity {QUEUE}");

    // With room again, the next frame arrives stamped with the running total.
    send(&tx, classic(0x56, &[1])).await;
    let marker = collect(&mut source, 1).await.remove(0);
    let dropped: usize = marker
        .attrs
        .get("can.dropped")
        .expect("the marker carries the drop count")
        .parse()
        .expect("count");
    assert!(
        dropped > 1000,
        "only {dropped} drops: the receive task must keep draining the socket"
    );
    assert!(queued + dropped <= SENT);

    finish(&mut source, &shutdown).await;
}

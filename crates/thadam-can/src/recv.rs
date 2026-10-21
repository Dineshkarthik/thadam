//! The receive task: frames in, messages out, drops counted.
//!
//! CAN has no flow control and nothing to acknowledge, so the task never
//! waits on the runtime's channel: a frame that does not fit the queue is
//! dropped and counted in `dropped`, and the count travels on every message
//! as `can.dropped`. Liveness (draining the socket) therefore never depends
//! on how slowly the pipeline pulls.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use serde_json::Value;
use thadam_core::{Clock, RawMessage, Shutdown, Timestamp};
use tokio::sync::mpsc;

use crate::config::{CanConfig, Emitter, Unknown};
use crate::dbc::Decoder;
use crate::error::CanError;
use crate::frame::{CanFrameData, FrameSource};

/// A dropped-frame warning is logged for the first drop and then every this
/// many drops.
const WARN_EVERY: u64 = 10_000;

/// Runs until `stop` triggers, the frame source ends, or the receiver hangs
/// up. A transport failure ends the task with its error; the source reports
/// it from `next_batch` once the queued messages are drained, so the runtime
/// can reopen the interface.
pub(crate) async fn run(
    config: CanConfig,
    decoder: Option<Arc<Decoder>>,
    mut frames: Box<dyn FrameSource>,
    dropped: Arc<AtomicU64>,
    clock: Arc<dyn Clock>,
    tx: mpsc::Sender<RawMessage>,
    stop: Shutdown,
) -> Result<(), CanError> {
    loop {
        let frame = tokio::select! {
            frame = frames.next_frame() => frame,
            () = stop.triggered() => return Ok(()),
        };
        let frame = match frame {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(error) => {
                tracing::warn!(%error, interface = %config.interface, "CAN receive failed");
                return Err(error);
            }
        };
        let received = Timestamp::from_micros(
            frame
                .timestamp_us
                .unwrap_or_else(|| clock.now().as_micros()),
        )
        .unwrap_or_else(|| clock.now());
        let Some(message) = to_message(&config, decoder.as_deref(), &frame, received, &dropped)
        else {
            continue;
        };
        match tx.try_send(message) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                let total = dropped.fetch_add(1, Ordering::Relaxed) + 1;
                if total == 1 || total.is_multiple_of(WARN_EVERY) {
                    tracing::warn!(
                        interface = %config.interface,
                        total,
                        "CAN receive queue full; dropping frames"
                    );
                }
            }
            Err(mpsc::error::TrySendError::Closed(_)) => return Ok(()),
        }
    }
}

/// Maps one frame to zero or one message. `None` means skipped (a decoded
/// frame the DBC does not know with `unknown: drop`).
fn to_message(
    config: &CanConfig,
    decoder: Option<&Decoder>,
    frame: &CanFrameData,
    received: Timestamp,
    dropped: &AtomicU64,
) -> Option<RawMessage> {
    let decoded = match (config.emit, decoder) {
        (Emitter::Decoded, Some(decoder)) => Some(decoder.decode(frame)),
        _ => None,
    };
    match decoded {
        // Decoded message with a known id (payload may be an empty object).
        Some(Some(mut signals)) => {
            signals.insert("ts_us".into(), Value::from(received.as_micros()));
            let payload = serde_json::to_vec(&Value::Object(signals)).ok()?;
            let name = decoder
                .and_then(|d| d.message_name(frame))
                .unwrap_or_default();
            let mut message = RawMessage::new(received, Bytes::from(payload));
            attrs(config, frame, &mut message);
            message.attrs.insert("can.message", name.to_owned());
            message
                .attrs
                .insert("topic", format!("can/{}/{name}", config.interface));
            Some(with_drop_count(message, dropped))
        }
        // Unknown id under `unknown: drop`.
        Some(None) if config.unknown == Unknown::Drop => None,
        // Raw mode, or unknown id under `unknown: raw`, or decoded mode
        // without a decoder (rejected by validation; defensive).
        _ => {
            let mut message = RawMessage::new(received, Bytes::copy_from_slice(&frame.data));
            attrs(config, frame, &mut message);
            message.attrs.insert(
                "topic",
                format!("can/{}/{}", config.interface, frame.id_hex()),
            );
            Some(with_drop_count(message, dropped))
        }
    }
}

/// The attributes every message carries.
fn attrs(config: &CanConfig, frame: &CanFrameData, message: &mut RawMessage) {
    message.attrs.insert("can.id", frame.id_hex());
    message
        .attrs
        .insert("can.extended", frame.extended.to_string());
    message.attrs.insert("can.fd", frame.fd.to_string());
    message
        .attrs
        .insert("can.dlc", frame.data.len().to_string());
    message
        .attrs
        .insert("can.interface", config.interface.clone());
}

/// Stamps the running dropped-frame count, when nonzero.
fn with_drop_count(mut message: RawMessage, dropped: &AtomicU64) -> RawMessage {
    let total = dropped.load(Ordering::Relaxed);
    if total > 0 {
        message.attrs.insert("can.dropped", total.to_string());
    }
    message
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::frame::ChannelFrames;
    use thadam_core::{Shutdown, SystemClock};

    fn config(json: serde_json::Value) -> CanConfig {
        CanConfig::from_value(&json).expect("valid config")
    }

    const DBC: &str = r#"
VERSION ""

NS_ :

BS_:

BU_: ECU

BO_ 291 EngineData: 8 ECU
 SG_ Rpm : 0|16@1+ (0.25,0) [0|16383.75] "rpm" ECU

BO_ 2147488308 ExtendedMsg: 8 ECU
 SG_ ExtSignal : 0|8@1+ (1,0) [0|255] "" ECU
"#;

    fn decoded_decoder() -> Decoder {
        Decoder::from_text(DBC).expect("dbc")
    }

    fn frame(id: u32, data: &[u8]) -> CanFrameData {
        CanFrameData::classic(id, data)
    }

    #[test]
    fn raw_mode_emits_data_and_attributes() {
        let cfg = config(serde_json::json!({"interface": "vcan0"}));
        let dropped = AtomicU64::new(0);
        let received = Timestamp::from_micros(1_234_567).expect("ts");
        let message =
            to_message(&cfg, None, &frame(0x12A, &[1, 2, 3]), received, &dropped).expect("message");

        assert_eq!(message.payload.as_ref(), &[1, 2, 3]);
        assert_eq!(message.ingest_time, received);
        assert_eq!(message.attrs.get("can.id"), Some("12A"));
        assert_eq!(message.attrs.get("can.extended"), Some("false"));
        assert_eq!(message.attrs.get("can.fd"), Some("false"));
        assert_eq!(message.attrs.get("can.dlc"), Some("3"));
        assert_eq!(message.attrs.get("can.interface"), Some("vcan0"));
        assert_eq!(message.attrs.get("topic"), Some("can/vcan0/12A"));
        assert!(message.attrs.get("can.dropped").is_none());
    }

    #[test]
    fn drop_count_rides_along_once_positive() {
        let cfg = config(serde_json::json!({"interface": "vcan0"}));
        let dropped = AtomicU64::new(7);
        let received = Timestamp::from_micros(10).expect("ts");
        let message = to_message(&cfg, None, &frame(1, &[]), received, &dropped).expect("message");
        assert_eq!(message.attrs.get("can.dropped"), Some("7"));
    }

    #[test]
    fn decoded_mode_emits_signals_topic_and_ts() {
        let cfg = config(serde_json::json!({
            "interface": "vcan0", "emit": "decoded", "dbc": "test.dbc"
        }));
        let decoder = decoded_decoder();
        let dropped = AtomicU64::new(0);
        let received = Timestamp::from_micros(1_000_000).expect("ts");
        let message = to_message(
            &cfg,
            Some(&decoder),
            &frame(291, &[0xE0, 0x2E, 0, 0, 0, 0, 0, 0]),
            received,
            &dropped,
        )
        .expect("message");

        let payload: serde_json::Value = serde_json::from_slice(&message.payload).expect("json");
        assert_eq!(payload["Rpm"], serde_json::json!(3000.0));
        assert_eq!(payload["ts_us"], serde_json::json!(1_000_000));
        assert_eq!(message.attrs.get("topic"), Some("can/vcan0/EngineData"));
        assert_eq!(message.attrs.get("can.message"), Some("EngineData"));
        assert_eq!(message.attrs.get("can.id"), Some("123"));
    }

    #[test]
    fn decoded_mode_extended_id_uses_eight_hex_digits() {
        let cfg = config(serde_json::json!({
            "interface": "vcan0", "emit": "decoded", "dbc": "test.dbc"
        }));
        let decoder = decoded_decoder();
        let dropped = AtomicU64::new(0);
        let mut ext = frame(0x1234, &[9]);
        ext.extended = true;
        let message = to_message(
            &cfg,
            Some(&decoder),
            &ext,
            Timestamp::from_micros(1).expect("ts"),
            &dropped,
        )
        .expect("message");
        assert_eq!(message.attrs.get("can.id"), Some("00001234"));
        assert_eq!(message.attrs.get("can.extended"), Some("true"));
        assert_eq!(message.attrs.get("topic"), Some("can/vcan0/ExtendedMsg"));
    }

    #[test]
    fn unknown_ids_fall_back_to_raw_or_drop() {
        let dropped = AtomicU64::new(0);
        let received = Timestamp::from_micros(1).expect("ts");
        let decoder = decoded_decoder();

        let raw = config(serde_json::json!({
            "interface": "vcan0", "emit": "decoded", "dbc": "test.dbc"
        }));
        let message = to_message(
            &raw,
            Some(&decoder),
            &frame(0x777, &[5]),
            received,
            &dropped,
        )
        .expect("raw fallback");
        assert_eq!(message.payload.as_ref(), &[5]);
        assert_eq!(message.attrs.get("topic"), Some("can/vcan0/777"));

        let drop = config(serde_json::json!({
            "interface": "vcan0", "emit": "decoded", "dbc": "test.dbc", "unknown": "drop"
        }));
        assert!(
            to_message(
                &drop,
                Some(&decoder),
                &frame(0x777, &[5]),
                received,
                &dropped
            )
            .is_none()
        );
    }

    #[tokio::test]
    async fn the_loop_drops_and_counts_under_backpressure() {
        // Two slots in the message queue; ten frames arrive at once. Eight
        // are dropped and counted, and the count rides on the messages.
        let (frame_tx, frame_rx) = mpsc::channel(64);
        let (tx, mut rx) = mpsc::channel(2);
        let dropped = Arc::new(AtomicU64::new(0));
        let stop = Shutdown::new();
        let task = tokio::spawn(run(
            config(serde_json::json!({"interface": "vcan0"})),
            None,
            Box::new(ChannelFrames { rx: frame_rx }),
            Arc::clone(&dropped),
            Arc::new(SystemClock::new()),
            tx,
            stop.clone(),
        ));

        for id in 0..10u32 {
            frame_tx.send(frame(id, &[id as u8])).await.expect("send");
        }
        // Wait until the loop has drained the frame channel; the queue only
        // fits two, so the other eight are dropped.
        tokio::time::timeout(Duration::from_secs(5), async {
            while dropped.load(Ordering::Relaxed) != 8 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("eight frames dropped");

        let first = rx.try_recv().expect("first message");
        assert_eq!(first.payload.as_ref(), &[0]);
        let second = rx.try_recv().expect("second message");
        assert_eq!(second.payload.as_ref(), &[1]);
        // Both queued before any drop, so neither carries the count.
        assert!(first.attrs.get("can.dropped").is_none());
        assert!(second.attrs.get("can.dropped").is_none());

        // With slots free again the next frame fits and carries the total.
        frame_tx.send(frame(10, &[10])).await.expect("send");
        let stamped = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("frame arrives")
            .expect("message");
        assert_eq!(stamped.payload.as_ref(), &[10]);
        assert_eq!(stamped.attrs.get("can.dropped"), Some("8"));

        stop.trigger();
        task.await.expect("task ends").expect("clean end");
    }

    struct FailingFrames;

    #[async_trait::async_trait]
    impl FrameSource for FailingFrames {
        async fn next_frame(&mut self) -> Result<Option<CanFrameData>, CanError> {
            Err(CanError::Transport("network is down".into()))
        }
    }

    #[tokio::test]
    async fn a_transport_failure_ends_the_loop_with_its_error() {
        let (tx, _rx) = mpsc::channel(4);
        let result = run(
            config(serde_json::json!({"interface": "vcan0"})),
            None,
            Box::new(FailingFrames),
            Arc::new(AtomicU64::new(0)),
            Arc::new(SystemClock::new()),
            tx,
            Shutdown::new(),
        )
        .await;
        assert_eq!(result, Err(CanError::Transport("network is down".into())));
    }

    #[tokio::test]
    async fn end_of_frames_ends_the_loop() {
        let (frame_tx, frame_rx) = mpsc::channel::<CanFrameData>(1);
        let (tx, mut rx) = mpsc::channel(4);
        let stop = Shutdown::new();
        let task = tokio::spawn(run(
            config(serde_json::json!({"interface": "vcan0"})),
            None,
            Box::new(ChannelFrames { rx: frame_rx }),
            Arc::new(AtomicU64::new(0)),
            Arc::new(SystemClock::new()),
            tx,
            stop.clone(),
        ));
        frame_tx.send(frame(1, &[1])).await.expect("send");
        assert!(rx.recv().await.is_some());
        drop(frame_tx);
        task.await.expect("task ends").expect("clean end");
        // The message channel closes when the task ends.
        assert!(rx.recv().await.is_none());
    }
}

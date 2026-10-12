//! Publishes the evaluation scenario against a local broker: an edge node
//! lifecycle (NBIRTH, DBIRTH, alias-only DATA, a forced seq gap, an unknown
//! alias, NDEATH, rebirth with rebound aliases) plus one retained host STATE
//! message. The matching `subscribe` binary decodes and checks it.

use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use prost::Message as _;
use rumqttc::{Client, MqttOptions, Publish, QoS};
use sparkplug_eval::pb;

const GROUP: &str = "plantA";
const NODE: &str = "edge1";
const DEVICE: &str = "dev1";
const STEP: Duration = Duration::from_millis(60);

use pb::payload::metric::Value;

fn now_ms() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(d) => u64::try_from(d.as_millis()).unwrap_or(u64::MAX),
        Err(_) => 0,
    }
}

fn metric(
    name: Option<&str>,
    alias: Option<u64>,
    datatype: pb::DataType,
    value: Value,
) -> pb::payload::Metric {
    pb::payload::Metric {
        name: name.map(Into::into),
        alias,
        datatype: Some(datatype as i32 as u32),
        timestamp: Some(now_ms()),
        value: Some(value),
        ..Default::default()
    }
}

fn bd_seq(v: u64) -> pb::payload::Metric {
    metric(
        Some("bdSeq"),
        None,
        pb::DataType::Int64,
        Value::LongValue(v),
    )
}

fn payload(seq: u64, metrics: Vec<pb::payload::Metric>) -> Vec<u8> {
    pb::Payload {
        timestamp: Some(now_ms()),
        seq: Some(seq),
        metrics,
        ..Default::default()
    }
    .encode_to_vec()
}

fn endpoint() -> (String, u16) {
    let host = std::env::var("MQTT_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = std::env::var("MQTT_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(18883);
    (host, port)
}

fn send(client: &Client, label: &str, topic: &str, seq: Option<u64>, bytes: &[u8], retain: bool) {
    let qos = if retain {
        QoS::AtLeastOnce
    } else {
        QoS::AtMostOnce
    };
    let error = client
        .publish(topic, qos, retain, bytes.to_vec())
        .err()
        .map_or_else(String::new, |e| format!(" PUBLISH-ERROR {e}"));
    let seq_text = seq.map_or_else(|| "-".into(), |s| s.to_string());
    println!(
        "pub {:>12} {:<48} seq={seq_text:<3} {:>3}B{error}",
        label,
        topic,
        bytes.len()
    );
    thread::sleep(STEP);
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (host, port) = endpoint();
    let mut options = MqttOptions::new("spb-eval-publisher", &host, port);
    options.set_keep_alive(Duration::from_secs(10));
    let (client, mut connection) = Client::new(options, 16);
    println!("publisher {}:{port}", host);

    // Drives the socket so the queued publishes actually leave.
    let drain = thread::spawn(move || {
        for notification in connection.iter() {
            if notification.is_err() {
                break;
            }
        }
    });

    // Let the subscriber's SUBSCRIBE land before the scenario starts.
    thread::sleep(Duration::from_millis(400));

    let node_prefix = format!("spBv1.0/{GROUP}");
    let node = |kind: &str| format!("{node_prefix}/{kind}/{NODE}");
    let dev = |kind: &str| format!("{node_prefix}/{kind}/{NODE}/{DEVICE}");
    let f32v = Value::FloatValue;
    let u32v = Value::IntValue;

    // 1. NBIRTH: node metrics temp/1, pressure/2, rpm/3; session bdSeq 0.
    send(
        &client,
        "NBIRTH",
        &node("NBIRTH"),
        Some(0),
        &payload(
            0,
            vec![
                bd_seq(0),
                metric(Some("temp"), Some(1), pb::DataType::Float, f32v(21.4)),
                metric(Some("pressure"), Some(2), pb::DataType::Float, f32v(101.1)),
                metric(Some("rpm"), Some(3), pb::DataType::UInt32, u32v(1200)),
            ],
        ),
        false,
    );

    // 2. DBIRTH: device metrics motor.speed/10, motor.amps/11.
    send(
        &client,
        "DBIRTH",
        &dev("DBIRTH"),
        Some(1),
        &payload(
            1,
            vec![
                metric(
                    Some("motor.speed"),
                    Some(10),
                    pb::DataType::Float,
                    f32v(0.0),
                ),
                metric(Some("motor.amps"), Some(11), pb::DataType::Float, f32v(0.0)),
            ],
        ),
        false,
    );

    // 3. NDATA: alias-only, as Sparkplug optimizes.
    send(
        &client,
        "NDATA",
        &node("NDATA"),
        Some(2),
        &payload(
            2,
            vec![
                metric(None, Some(1), pb::DataType::Float, f32v(21.5)),
                metric(None, Some(2), pb::DataType::Float, f32v(101.3)),
            ],
        ),
        false,
    );

    // 4. DDATA: alias-only for the device.
    send(
        &client,
        "DDATA",
        &dev("DDATA"),
        Some(3),
        &payload(
            3,
            vec![
                metric(None, Some(10), pb::DataType::Float, f32v(1450.0)),
                metric(None, Some(11), pb::DataType::Float, f32v(7.243)),
            ],
        ),
        false,
    );

    // 5. NDATA with seq 5: seq 4 never sent -> the decoder must see a gap.
    send(
        &client,
        "NDATA-gap",
        &node("NDATA"),
        Some(5),
        &payload(
            5,
            vec![metric(None, Some(3), pb::DataType::UInt32, u32v(1180))],
        ),
        false,
    );

    // 6. DDATA with one unknown alias (99) beside a known one (10).
    send(
        &client,
        "DDATA-alias99",
        &dev("DDATA"),
        Some(6),
        &payload(
            6,
            vec![
                metric(None, Some(10), pb::DataType::Float, f32v(1455.5)),
                metric(None, Some(99), pb::DataType::Float, f32v(1.0)),
            ],
        ),
        false,
    );

    // 7. NDEATH: ends session bdSeq 0 -> everything of the node is stale.
    send(
        &client,
        "NDEATH",
        &node("NDEATH"),
        Some(7),
        &payload(7, vec![bd_seq(0)]),
        false,
    );

    // 8. NDATA after the death certificate: must come out flagged stale.
    send(
        &client,
        "NDATA-stale",
        &node("NDATA"),
        Some(8),
        &payload(
            8,
            vec![metric(None, Some(1), pb::DataType::Float, f32v(22.0))],
        ),
        false,
    );

    // 9. NBIRTH rebirth: new session bdSeq 1, alias 3 rebinds rpm -> vibration.
    send(
        &client,
        "NBIRTH-rebirth",
        &node("NBIRTH"),
        Some(0),
        &payload(
            0,
            vec![
                bd_seq(1),
                metric(Some("temp"), Some(1), pb::DataType::Float, f32v(21.4)),
                metric(Some("pressure"), Some(2), pb::DataType::Float, f32v(101.1)),
                metric(Some("vibration"), Some(3), pb::DataType::Float, f32v(0.4)),
            ],
        ),
        false,
    );

    // 10. DBIRTH after rebirth: aliases 10/11 swapped.
    send(
        &client,
        "DBIRTH-swap",
        &dev("DBIRTH"),
        Some(1),
        &payload(
            1,
            vec![
                metric(Some("motor.amps"), Some(10), pb::DataType::Float, f32v(0.0)),
                metric(
                    Some("motor.speed"),
                    Some(11),
                    pb::DataType::Float,
                    f32v(0.0),
                ),
            ],
        ),
        false,
    );

    // 11. DDATA resolves through the swapped table: alias 10 = motor.amps.
    send(
        &client,
        "DDATA-swapped",
        &dev("DDATA"),
        Some(2),
        &payload(
            2,
            vec![metric(None, Some(10), pb::DataType::Float, f32v(6.9))],
        ),
        false,
    );

    // 12. NDATA resolves through the rebound node table: alias 3 = vibration.
    send(
        &client,
        "NDATA-vibration",
        &node("NDATA"),
        Some(3),
        &payload(
            3,
            vec![metric(None, Some(3), pb::DataType::Float, f32v(0.42))],
        ),
        false,
    );

    // 13. A host application's retained STATE (JSON): the consumer observes
    // but must not decode it as protobuf — and must never publish one.
    let state_topic = format!("{node_prefix}/STATE/thadam-host-01");
    send(
        &client,
        "STATE",
        &state_topic,
        None,
        b"{\"online\":true}",
        true,
    );

    thread::sleep(Duration::from_millis(300));
    if let Err(e) = client.disconnect() {
        println!("disconnect: {e}");
    }
    let _ = drain.join();
    println!("scenario complete: 13 messages, 1 retained");
    Ok(())
}

/// Keeps `Publish` referenced: QoS 1 arrivals (STATE) are acked by the
/// subscriber with `client.ack(&publish)`; here only the type is needed.
#[allow(dead_code)]
fn _type_anchor(_: &Publish) {}

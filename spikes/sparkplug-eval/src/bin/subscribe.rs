//! Subscribes to `spBv1.0/#` on a local broker, decodes the scenario the
//! `publish` binary produces, checks the decoder's correctness invariants,
//! and measures per-message decode cost (full decode+state update and, as a
//! baseline, protobuf decode alone).

use std::time::{Duration, Instant};

use prost::Message as _;
use rumqttc::{Client, Event, Incoming, MqttOptions, QoS};
use sparkplug_eval::decode::{Change, Decoder, MetricValue, Outcome, ResolvedMetric, SeqGap};
use sparkplug_eval::{pb, topic};

/// The publisher's scenario has this many messages.
const EXPECTED: usize = 13;

struct Recorded {
    kind: topic::Kind,
    payload: Vec<u8>,
    decode_ns: u128,
    error: Option<String>,
    outcome: Option<Outcome>,
}

fn endpoint() -> (String, u16) {
    let host = std::env::var("MQTT_HOST").unwrap_or_else(|_| "127.0.0.1".into());
    let port = std::env::var("MQTT_PORT")
        .ok()
        .and_then(|v| v.parse::<u16>().ok())
        .unwrap_or(18883);
    (host, port)
}

fn names(recorded: &Recorded) -> Vec<&str> {
    recorded.outcome.as_ref().map_or(Vec::new(), |o| {
        o.resolved.iter().map(|m| &*m.name).collect()
    })
}

fn resolved(recorded: &Recorded, index: usize) -> Option<&ResolvedMetric> {
    recorded.outcome.as_ref()?.resolved.get(index)
}

fn approx_value(recorded: &Recorded, index: usize, expected: f32) -> bool {
    matches!(
        resolved(recorded, index).map(|m| &m.value),
        Some(MetricValue::F32(x)) if (x - expected).abs() < 0.01
    )
}

fn is_stale(recorded: &Recorded, index: usize) -> bool {
    resolved(recorded, index).is_some_and(|m| m.stale)
}

fn any_note(recorded: &Recorded, needle: &str) -> bool {
    recorded
        .outcome
        .as_ref()
        .is_some_and(|o| o.notes.iter().any(|n| n.contains(needle)))
}

fn stats_line(label: &str, mut samples_ns: Vec<u128>) {
    samples_ns.sort_unstable();
    let n = samples_ns.len();
    if n == 0 {
        println!("{label}: no samples");
        return;
    }
    let mean = samples_ns.iter().sum::<u128>() as f64 / n as f64;
    let pick = |q: f64| samples_ns[((n - 1) as f64 * q) as usize];
    println!(
        "{label}: n={n} mean {:.2}µs p50 {:.2}µs p95 {:.2}µs max {:.2}µs",
        mean / 1000.0,
        f64::from(pick(0.50) as u32) / 1000.0,
        f64::from(pick(0.95) as u32) / 1000.0,
        f64::from(samples_ns[n - 1] as u32) / 1000.0,
    );
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (host, port) = endpoint();
    let mut options = MqttOptions::new("spb-eval-subscriber", &host, port);
    options.set_keep_alive(Duration::from_secs(10));
    let (client, mut connection) = Client::new(options, 32);
    client.subscribe("spBv1.0/#", QoS::AtLeastOnce)?;
    println!("subscriber {host}:{port} filter spBv1.0/# qos 1");

    let mut decoder = Decoder::new();
    let mut records: Vec<Recorded> = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(30);

    while records.len() < EXPECTED && Instant::now() < deadline {
        let remaining = deadline.saturating_duration_since(Instant::now());
        let event = match connection.recv_timeout(remaining) {
            Ok(Ok(event)) => event,
            Ok(Err(e)) => return Err(format!("broker connection failed: {e}").into()),
            Err(_) => continue, // recv timeout; the outer deadline decides
        };
        let Event::Incoming(Incoming::Publish(publish)) = event else {
            continue;
        };
        if publish.qos != QoS::AtMostOnce
            && let Err(e) = client.ack(&publish)
        {
            println!("ack failed: {e}");
        }
        let started = Instant::now();
        let record = match topic::parse(&publish.topic) {
            Some(t) => {
                let payload = publish.payload.to_vec();
                let result = decoder.handle(&t, &payload);
                let elapsed = started.elapsed().as_nanos();
                let (error, outcome) = match result {
                    Ok(outcome) => (None, Some(outcome)),
                    Err(e) => (Some(e.to_string()), None),
                };
                Recorded {
                    kind: t.kind,
                    payload,
                    decode_ns: elapsed,
                    error,
                    outcome,
                }
            }
            None => {
                println!("skip: not a Sparkplug topic: {}", publish.topic);
                continue;
            }
        };
        let idx = records.len() + 1;
        let marker = record
            .outcome
            .as_ref()
            .and_then(|o| o.gap)
            .map_or_else(String::new, |g: SeqGap| format!("  [GAP {g}]"));
        let resolved_names = names(&record).join(", ");
        let notes = record
            .outcome
            .as_ref()
            .map_or_else(String::new, |o| o.notes.join(" | "));
        let change = record
            .outcome
            .as_ref()
            .and_then(|o| o.change)
            .map_or_else(String::new, |c| format!("{c:?}"));
        println!(
            "msg {idx:02} {:<16} {:<38} {:>3}B {:>6.2}µs {change} {}{}{} {notes}",
            record.kind.as_str(),
            publish.topic,
            record.payload.len(),
            record.decode_ns as f64 / 1000.0,
            resolved_names,
            if resolved_names.is_empty() { "" } else { " " },
            marker,
        );
        records.push(record);
    }

    println!("--- summary ---");
    println!(
        "messages: {}/{}  protobuf: {}  retained-state: {}",
        records.len(),
        EXPECTED,
        records
            .iter()
            .filter(|r| r.kind != topic::Kind::State)
            .count(),
        records
            .iter()
            .filter(|r| r.kind == topic::Kind::State)
            .count(),
    );
    stats_line(
        "decode+state update",
        records
            .iter()
            .filter(|r| r.kind != topic::Kind::State)
            .map(|r| r.decode_ns)
            .collect(),
    );

    // Baseline: bare protobuf decode of the same payloads, no state update.
    let mut protobuf_ns = Vec::new();
    for record in records.iter().filter(|r| r.kind != topic::Kind::State) {
        let started = Instant::now();
        let decoded = pb::Payload::decode(&record.payload[..]);
        protobuf_ns.push(started.elapsed().as_nanos());
        if decoded.is_err() {
            println!("baseline decode failed unexpectedly");
        }
    }
    stats_line("protobuf decode only", protobuf_ns);

    run_checks(&records);
    Ok(())
}

fn run_checks(records: &[Recorded]) {
    if records.len() < EXPECTED {
        println!(
            "checks: not run — only {}/{} messages arrived",
            records.len(),
            EXPECTED
        );
        println!("RESULT: FAIL");
        return;
    }
    let mut checks: Vec<(&str, bool)> = Vec::new();
    let outcome = |i: usize| records.get(i).and_then(|r| r.outcome.as_ref());
    let gaps: Vec<SeqGap> = records
        .iter()
        .filter_map(|r| r.outcome.as_ref().and_then(|o| o.gap))
        .collect();

    checks.push((
        "01 NBIRTH registers node table (3 metrics), node live",
        outcome(0).is_some_and(|o| {
            o.change == Some(Change::Birth) && o.registered == 3 && o.node_dead == Some(false)
        }),
    ));
    checks.push((
        "02 DBIRTH registers device table (2 metrics)",
        outcome(1).is_some_and(|o| o.change == Some(Change::DeviceBirth) && o.registered == 2),
    ));
    checks.push((
        "03 NDATA alias-only resolves to temp=21.5, pressure=101.3",
        names(&records[2]) == ["temp", "pressure"]
            && approx_value(&records[2], 0, 21.5)
            && approx_value(&records[2], 1, 101.3)
            && !is_stale(&records[2], 0),
    ));
    checks.push((
        "04 DDATA alias-only resolves to motor.speed=1450, motor.amps=7.243",
        names(&records[3]) == ["motor.speed", "motor.amps"] && approx_value(&records[3], 0, 1450.0),
    ));
    checks.push((
        "05 seq gap detected exactly once: expected 4, got 5",
        gaps == [SeqGap {
            expected: 4,
            got: 5,
        }],
    ));
    checks.push((
        "06 unknown alias 99 noted; known alias still decoded",
        any_note(&records[5], "alias 99") && names(&records[5]) == ["motor.speed", "alias:99"],
    ));
    checks.push((
        "07 NDEATH with matching bdSeq marks node stale",
        outcome(6).is_some_and(|o| o.change == Some(Change::Death) && o.node_dead == Some(true)),
    ));
    checks.push((
        "08 DATA after NDEATH comes out stale",
        any_note(&records[7], "past its DEATH") && is_stale(&records[7], 0),
    ));
    checks.push((
        "09 rebirth (new bdSeq) restores live state",
        outcome(8).is_some_and(|o| o.change == Some(Change::Rebirth) && o.node_dead == Some(false)),
    ));
    checks.push((
        "10 rebirth rebinds device aliases (10 = motor.amps)",
        names(&records[10]) == ["motor.amps"] && !is_stale(&records[10], 0),
    ));
    checks.push((
        "11 rebirth rebinds node aliases (3 = vibration)",
        names(&records[11]) == ["vibration"] && approx_value(&records[11], 0, 0.42),
    ));
    checks.push((
        "12 host STATE observed as passthrough, not decoded",
        records[12].kind == topic::Kind::State
            && any_note(&records[12], "passthrough")
            && records[12].error.is_none(),
    ));
    checks.push((
        "13 every protobuf payload decoded without errors",
        records.iter().all(|r| r.error.is_none()),
    ));

    let mut failed = 0;
    for (name, ok) in &checks {
        if !ok {
            failed += 1;
        }
        println!("check {name}: {}", if *ok { "PASS" } else { "FAIL" });
    }
    println!("checks: {}/{} PASSED", checks.len() - failed, checks.len());
    if failed > 0 || records.len() < EXPECTED {
        println!("RESULT: FAIL");
    } else {
        println!("RESULT: PASS");
    }
}

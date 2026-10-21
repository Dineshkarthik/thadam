//! `ModbusSource`'s poll task: one Modbus TCP session, one pass over every
//! read per interval, one message per pass.
//!
//! Polling is at-most-once. A pass whose message was not committed before a
//! crash is gone; the device is never told about delivery.

use std::hash::{BuildHasher, Hasher, RandomState};
use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use serde_json::{Map, Value};
use thadam_core::{Clock, RawMessage, Shutdown, Timestamp};
use tokio::sync::mpsc;
use tokio_modbus::client::{Context, Reader, tcp};
use tokio_modbus::{ExceptionCode, Slave};

use crate::config::{ModbusConfig, ReadKind, ReadSpec, TIMESTAMP_KEY};
use crate::decode::{Table, decode};
use crate::error::ModbusError;

/// The first retry after a failure waits one `interval`, doubling from there.
/// The jittered delay stays within the upper half of the computed window.
fn backoff(interval: Duration, max: Duration, failures: u32, unit: f64) -> Duration {
    let shift = failures.saturating_sub(1).min(20);
    let window = interval.saturating_mul(1 << shift).min(max);
    let half = window / 2;
    half + half.mul_f64(unit)
}

fn random_unit() -> f64 {
    (RandomState::new().build_hasher().finish() >> 11) as f64 / (1u64 << 53) as f64
}

/// Runs until `stop` triggers or the receiver is dropped.
pub(crate) async fn run(
    config: ModbusConfig,
    clock: Arc<dyn Clock>,
    tx: mpsc::Sender<RawMessage>,
    stop: Shutdown,
) {
    let interval = config.interval.0;
    let max = config.backoff_max.0;
    let mut session: Option<Context> = None;
    let mut wait = Duration::ZERO;
    let mut failures: u32 = 0;
    loop {
        tokio::select! {
            () = tokio::time::sleep(wait) => {}
            () = stop.triggered() => return,
        }

        let outcome = tokio::select! {
            outcome = poll_once(&config, clock.as_ref(), &mut session) => outcome,
            () = stop.triggered() => return,
        };
        match outcome {
            Ok(message) => {
                failures = 0;
                wait = interval;
                let sent = tokio::select! {
                    sent = tx.send(message) => sent.is_ok(),
                    () = stop.triggered() => false,
                };
                if !sent {
                    return;
                }
            }
            Err(error) => {
                session = None;
                failures = failures.saturating_add(1);
                wait = backoff(interval, max, failures, random_unit());
                tracing::warn!(
                    address = %config.address,
                    failures,
                    retry_in = ?wait,
                    error = %error,
                    "Modbus poll failed"
                );
            }
        }
    }
}

/// Performs one pass over every read, connecting first if there is no session.
///
/// A read refused with a Modbus exception becomes a `null` value and is named
/// in `modbus.exceptions`. Any other failure returns an error, and the caller
/// drops the session.
pub(crate) async fn poll_once(
    config: &ModbusConfig,
    clock: &dyn Clock,
    session: &mut Option<Context>,
) -> Result<RawMessage, ModbusError> {
    if session.is_none() {
        *session = Some(connect(config).await?);
    }
    let ctx = session
        .as_mut()
        .ok_or(ModbusError::State("the Modbus session is missing"))?;

    let now = clock.now();
    let timeout = config.timeout.0;
    let mut values = Map::new();
    let mut exceptions: Vec<&str> = Vec::new();
    for read in &config.reads {
        match read_one(ctx, read, timeout).await? {
            Ok(table) => {
                values.insert(read.name.clone(), decode(read, &table)?);
            }
            Err(code) => {
                let error = ModbusError::Exception {
                    read: read.name.clone(),
                    code: u8::from(code),
                };
                tracing::warn!(error = %error, "Modbus read refused by the device");
                values.insert(read.name.clone(), Value::Null);
                exceptions.push(&read.name);
            }
        }
    }
    values.insert(TIMESTAMP_KEY.to_owned(), Value::from(now.as_micros()));

    let payload = serde_json::to_vec(&Value::Object(values))
        .map_err(|e| ModbusError::Protocol(e.to_string()))?;
    Ok(message(config, now, payload, &exceptions))
}

fn message(
    config: &ModbusConfig,
    now: Timestamp,
    payload: Vec<u8>,
    exceptions: &[&str],
) -> RawMessage {
    let mut message = RawMessage::new(now, Bytes::from(payload));
    message
        .attrs
        .insert("modbus.address", config.address.clone());
    message
        .attrs
        .insert("modbus.unit_id", config.unit_id.to_string());
    message
        .attrs
        .insert("topic", format!("modbus/{}", config.unit_id));
    if !exceptions.is_empty() {
        message
            .attrs
            .insert("modbus.exceptions", exceptions.join(","));
    }
    message
}

/// Opens a TCP connection to the device and attaches the unit identifier.
pub(crate) async fn connect(config: &ModbusConfig) -> Result<Context, ModbusError> {
    let timeout = config.timeout.0;
    let attempt = async {
        let mut addrs = tokio::net::lookup_host(config.address.as_str()).await?;
        let addr = addrs
            .next()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "the name has no addresses"))?;
        tcp::connect_slave(addr, Slave(config.unit_id)).await
    };
    match tokio::time::timeout(timeout, attempt).await {
        Ok(Ok(ctx)) => Ok(ctx),
        Ok(Err(e)) => Err(ModbusError::Connect {
            address: config.address.clone(),
            detail: e.to_string(),
        }),
        Err(_) => Err(ModbusError::Timeout(timeout)),
    }
}

/// The outer `Err` is a session failure; the inner `Err` is a per-read
/// exception from the device.
async fn read_one(
    ctx: &mut Context,
    read: &ReadSpec,
    timeout: Duration,
) -> Result<Result<Table, ExceptionCode>, ModbusError> {
    let answer = tokio::time::timeout(timeout, async {
        match read.kind {
            ReadKind::Holding => ctx
                .read_holding_registers(read.start, read.count)
                .await
                .map(|r| r.map(Table::Words)),
            ReadKind::Input => ctx
                .read_input_registers(read.start, read.count)
                .await
                .map(|r| r.map(Table::Words)),
            ReadKind::Coil => ctx
                .read_coils(read.start, read.count)
                .await
                .map(|r| r.map(Table::Bits)),
            ReadKind::Discrete => ctx
                .read_discrete_inputs(read.start, read.count)
                .await
                .map(|r| r.map(Table::Bits)),
        }
    })
    .await
    .map_err(|_| ModbusError::Timeout(timeout))?;
    answer.map_err(|e| match e {
        tokio_modbus::Error::Protocol(p) => ModbusError::Protocol(p.to_string()),
        tokio_modbus::Error::Transport(t) => ModbusError::Transport(t.to_string()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_within_jitter_and_caps() {
        let interval = Duration::from_millis(100);
        let max = Duration::from_secs(1);
        assert_eq!(backoff(interval, max, 1, 0.0), Duration::from_millis(50));
        assert_eq!(backoff(interval, max, 1, 1.0), Duration::from_millis(100));
        assert_eq!(backoff(interval, max, 3, 1.0), Duration::from_millis(400));
        assert_eq!(backoff(interval, max, 30, 1.0), max);
    }

    #[test]
    fn random_unit_stays_in_the_unit_interval() {
        for _ in 0..100 {
            let unit = random_unit();
            assert!((0.0..1.0).contains(&unit), "{unit}");
        }
    }
}

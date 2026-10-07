//! MQTT 5 module smoke test (rumqttc::v5): is it usable for the MVP source?
//!
//! A. `receive_maximum` is honoured by the broker as the incoming window (v3.1.1 has no
//!    equivalent): withhold acks and observe the maximum unacked deliveries.
//! B. Manual acks with a payload-stripped v5 Publish, persistent session (clean_start=false,
//!    session expiry), abrupt drop, reconnect: session_present and DUP redelivery.
//! C. Keepalive: PINGRESPs keep flowing while polling and withholding.

use std::collections::BTreeSet;
use std::time::{Duration, Instant};

use anyhow::Result;
use bytes::Bytes;
use clap::Args as ClapArgs;
use rumqttc::v5::mqttbytes::v5::Publish;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::{AsyncClient, Event, Incoming, MqttOptions};
use serde_json::json;
use tokio::time::{sleep, timeout};

use crate::common::*;
use crate::session::publish_n;

#[derive(ClapArgs, Clone, Debug)]
pub struct Args {
    #[arg(long)]
    pub broker: String,
    #[arg(long)]
    pub broker_inflight: u32,
    #[arg(long, default_value_t = 10)]
    pub receive_max: u16,
}

fn opts(
    id: &str,
    t: &Target,
    clean_start: bool,
    recv_max: Option<u16>,
    session_expiry: Option<u32>,
) -> MqttOptions {
    let mut o = MqttOptions::new(id, t.host.clone(), t.port);
    o.set_keep_alive(Duration::from_secs(5));
    o.set_clean_start(clean_start);
    o.set_manual_acks(true);
    o.set_max_packet_size(Some(1 << 20));
    if let Some(r) = recv_max {
        o.set_receive_maximum(Some(r));
    }
    if let Some(s) = session_expiry {
        o.set_session_expiry_interval(Some(s));
    }
    o
}

fn strip5(p: &Publish) -> Publish {
    Publish {
        dup: false,
        qos: p.qos,
        retain: false,
        topic: Bytes::new(),
        pkid: p.pkid,
        payload: Bytes::new(),
        properties: None,
    }
}

pub async fn run(t: &Target, a: Args) -> Result<()> {
    // ---- A + C: receive_maximum and keepalive
    let topic = rand_id("s2/v5a");
    let (client, mut el) = AsyncClient::new(
        opts(&rand_id("s2-v5a"), t, true, Some(a.receive_max), None),
        64,
    );
    client.subscribe(&topic, QoS::AtLeastOnce).await?;
    let mut subscribed = false;
    let mut pub_started = false;
    let mut held: Vec<Publish> = Vec::new();
    let mut pingresp = 0u64;
    let mut connack_props = None;
    let mut errors = 0u64;
    let start = Instant::now();
    let mut last_rx = Instant::now();
    while start.elapsed() < Duration::from_secs(16) {
        match timeout(Duration::from_millis(100), el.poll()).await {
            Ok(Ok(Event::Incoming(Incoming::ConnAck(c)))) => {
                connack_props = c.properties.as_ref().map(|p| {
                    format!(
                        "receive_max={:?} max_qos={:?} topic_alias_max={:?}",
                        p.receive_max, p.max_qos, p.topic_alias_max
                    )
                });
            }
            Ok(Ok(Event::Incoming(Incoming::SubAck(_)))) => subscribed = true,
            Ok(Ok(Event::Incoming(Incoming::Publish(p)))) => {
                held.push(strip5(&p));
                last_rx = Instant::now();
            }
            Ok(Ok(Event::Incoming(Incoming::PingResp(_)))) => pingresp += 1,
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                errors += 1;
                eprintln!("v5 poll error: {e}");
                sleep(Duration::from_millis(200)).await;
            }
            Err(_) => {}
        }
        if subscribed && !pub_started {
            pub_started = true;
            let (t2, topic2) = (t.clone(), topic.clone());
            tokio::spawn(async move {
                let _ = publish_n(&t2, &topic2, 0, 300, 64).await;
            });
        }
        let _ = last_rx;
    }
    let max_unacked_held = held.len();
    for p in &held {
        client.ack(p).await?;
    }
    let mut delivered_after_release = 0usize;
    let end = Instant::now() + Duration::from_secs(5);
    while Instant::now() < end && held.len() + delivered_after_release < 300 {
        if let Ok(Ok(Event::Incoming(Incoming::Publish(p)))) =
            timeout(Duration::from_millis(100), el.poll()).await
        {
            delivered_after_release += 1;
            client.ack(&strip5(&p)).await?;
        }
    }
    json_line(json!({
        "test": "v5_receive_maximum",
        "broker": a.broker,
        "broker_inflight": a.broker_inflight,
        "client_receive_maximum": a.receive_max,
        "connack_properties": connack_props,
        "unacked_held_when_broker_stopped_sending": max_unacked_held,
        "pingresp_during_16s_with_keepalive_5s": pingresp,
        "poll_errors": errors,
        "delivered_after_release": delivered_after_release,
    }));
    drop(el);
    drop(client);

    // ---- B: session persistence and DUP redelivery
    let topic = rand_id("s2/v5b");
    let cid = rand_id("s2-v5b");
    let (client, mut el) = AsyncClient::new(opts(&cid, t, false, None, Some(300)), 64);
    client.subscribe(&topic, QoS::AtLeastOnce).await?;
    let mut sp1 = None;
    let mut subscribed = false;
    let mut pub_started = false;
    let mut first: BTreeSet<u64> = BTreeSet::new();
    let end = Instant::now() + Duration::from_secs(8);
    while Instant::now() < end && first.len() < 20 {
        match timeout(Duration::from_millis(100), el.poll()).await {
            Ok(Ok(Event::Incoming(Incoming::ConnAck(c)))) => sp1 = Some(c.session_present),
            Ok(Ok(Event::Incoming(Incoming::SubAck(_)))) => subscribed = true,
            Ok(Ok(Event::Incoming(Incoming::Publish(p)))) => {
                if let Some(s) = parse_seq(&p.payload) {
                    first.insert(s);
                }
            }
            _ => {}
        }
        if subscribed && !pub_started {
            pub_started = true;
            publish_n(t, &topic, 0, 20, 64).await?;
        }
    }
    drop(el);
    drop(client);
    sleep(Duration::from_millis(500)).await;
    let (client, mut el) = AsyncClient::new(opts(&cid, t, false, None, Some(300)), 64);
    let mut sp2 = None;
    let mut redelivered: BTreeSet<u64> = BTreeSet::new();
    let mut dup_flagged = 0;
    let end = Instant::now() + Duration::from_secs(4);
    while Instant::now() < end {
        match timeout(Duration::from_millis(100), el.poll()).await {
            Ok(Ok(Event::Incoming(Incoming::ConnAck(c)))) => sp2 = Some(c.session_present),
            Ok(Ok(Event::Incoming(Incoming::Publish(p)))) => {
                if let Some(s) = parse_seq(&p.payload) {
                    redelivered.insert(s);
                }
                if p.dup {
                    dup_flagged += 1;
                }
                client.ack(&strip5(&p)).await?;
            }
            _ => {}
        }
    }
    drop(el);
    drop(client);
    sleep(Duration::from_millis(500)).await;
    let (_c3, mut el3) = AsyncClient::new(opts(&cid, t, false, None, Some(300)), 64);
    let mut again = 0;
    let end = Instant::now() + Duration::from_secs(3);
    while Instant::now() < end {
        if let Ok(Ok(Event::Incoming(Incoming::Publish(_)))) =
            timeout(Duration::from_millis(100), el3.poll()).await
        {
            again += 1;
        }
    }
    json_line(json!({
        "test": "v5_session_redelivery",
        "broker": a.broker,
        "first_connect_session_present": sp1,
        "received_unacked_on_first_connection": first.len(),
        "reconnect_session_present": sp2,
        "redelivered_unique": redelivered.len(),
        "redelivered_dup_flagged": dup_flagged,
        "redelivered_again_after_stripped_acks": again,
    }));
    Ok(())
}

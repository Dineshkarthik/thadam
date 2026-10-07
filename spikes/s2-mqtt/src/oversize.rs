//! Extra E2: an oversized QoS 1 message with rumqttc's default 10 KiB incoming packet limit.
//! The codec enforces the limit by failing the connection, not by skipping the packet; with a
//! persistent session the broker redelivers the same message after every reconnect.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use anyhow::Result;
use clap::Args as ClapArgs;
use rumqttc::{AsyncClient, Event, Incoming, MqttOptions, QoS};
use serde_json::json;
use tokio::time::{sleep, timeout};

use crate::common::*;
use crate::session::publish_n_qos;

#[derive(ClapArgs, Clone, Debug)]
pub struct Args {
    #[arg(long)]
    pub broker: String,
    #[arg(long)]
    pub broker_inflight: u32,
    #[arg(long, default_value_t = 12)]
    pub secs: u64,
    #[arg(long, default_value_t = 64)]
    pub big_kib: usize,
}

pub async fn run(t: &Target, a: Args) -> Result<()> {
    let topic = rand_id("s2/oversize");
    let cid = rand_id("s2-oversize-sub");
    // rumqttc defaults: max_incoming_packet_size = 10 KiB. Built by hand, not OptsBuilder.
    let mut o = MqttOptions::new(&cid, t.host.clone(), t.port);
    o.set_keep_alive(Duration::from_secs(5));
    o.set_clean_session(false);
    o.set_manual_acks(true);
    let (client, mut el) = AsyncClient::new(o, 64);
    client.subscribe(&topic, QoS::AtLeastOnce).await?;

    let mut subscribed = false;
    let mut published = false;
    let mut deliveries: BTreeMap<u64, (u32, u32)> = BTreeMap::new(); // seq -> (total, dup-flagged)
    let (mut connacks, mut errors) = (0u64, 0u64);
    let mut err_samples: Vec<String> = Vec::new();
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(a.secs) {
        if subscribed && !published {
            published = true;
            publish_n_qos(t, &topic, 0, 1, 64, 1).await?;
            publish_n_qos(t, &topic, 1, 1, a.big_kib * 1024, 1).await?;
            publish_n_qos(t, &topic, 2, 1, 64, 1).await?;
        }
        match timeout(Duration::from_millis(200), el.poll()).await {
            Ok(Ok(Event::Incoming(Incoming::ConnAck(_)))) => connacks += 1,
            Ok(Ok(Event::Incoming(Incoming::SubAck(_)))) => subscribed = true,
            Ok(Ok(Event::Incoming(Incoming::Publish(p)))) => {
                if let Some(s) = parse_seq(&p.payload) {
                    let e = deliveries.entry(s).or_default();
                    e.0 += 1;
                    if p.dup {
                        e.1 += 1;
                    }
                }
                client.ack(&strip(&p)).await?;
            }
            Ok(Ok(_)) => {}
            Ok(Err(e)) => {
                errors += 1;
                if err_samples.len() < 2 {
                    err_samples.push(e.to_string());
                }
                sleep(Duration::from_millis(100)).await;
            }
            Err(_) => {}
        }
    }
    json_line(json!({
        "test": "oversize_poison_message",
        "broker": a.broker,
        "broker_inflight": a.broker_inflight,
        "max_incoming_packet_default_kib": 10,
        "big_message_kib": a.big_kib,
        "seconds": a.secs,
        "connacks": connacks,
        "poll_errors": errors,
        "error_samples": err_samples,
        "deliveries_seq_total_dupflagged": deliveries,
        "seq2_small_message_behind_the_big_one_delivered": deliveries.contains_key(&2),
    }));
    Ok(())
}

//! Minimal MQTT 3.1.1-aware TCP proxy used as an independent wire-level oracle:
//! it records every packet in both directions with a timestamp, and which side
//! closed the socket first. The client under test connects to the proxy; the
//! proxy forwards bytes untouched.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::Result;
use bytes::BytesMut;
use serde::Serialize;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum Dir {
    ClientToBroker,
    BrokerToClient,
}

#[derive(Clone, Debug, Serialize)]
pub enum Pkt {
    Connect {
        clean: bool,
        keepalive: u16,
        client_id: String,
    },
    ConnAck {
        session_present: bool,
        rc: u8,
    },
    Publish {
        pkid: u16,
        qos: u8,
        dup: bool,
        payload_len: usize,
    },
    PubAck(u16),
    PubRec(u16),
    PubRel(u16),
    PubComp(u16),
    Subscribe(u16),
    SubAck(u16),
    PingReq,
    PingResp,
    Disconnect,
    Other(u8),
    Closed,
}

#[derive(Clone, Debug, Serialize)]
pub struct WireEvent {
    pub t_ms: u64,
    pub conn: u32,
    pub dir: Dir,
    pub pkt: Pkt,
}

#[derive(Clone)]
pub struct WireLog {
    start: Instant,
    events: Arc<Mutex<Vec<WireEvent>>>,
}

impl WireLog {
    fn push(&self, conn: u32, dir: Dir, pkt: Pkt) {
        let t_ms = self.start.elapsed().as_millis() as u64;
        self.events.lock().unwrap().push(WireEvent {
            t_ms,
            conn,
            dir,
            pkt,
        });
    }
    pub fn snapshot(&self) -> Vec<WireEvent> {
        self.events.lock().unwrap().clone()
    }
    pub fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }
}

pub async fn start(broker: String) -> Result<(u16, WireLog)> {
    let (port, log, _) = start_with_stall(broker).await?;
    Ok((port, log))
}

/// Like `start`, plus a switch that makes the proxy stop reading from the client
/// (client->broker direction) while set, so the client's TCP send buffer fills up.
pub async fn start_with_stall(broker: String) -> Result<(u16, WireLog, Arc<AtomicBool>)> {
    let stall = Arc::new(AtomicBool::new(false));
    let stall2 = stall.clone();
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let log = WireLog {
        start: Instant::now(),
        events: Arc::new(Mutex::new(Vec::new())),
    };
    let l2 = log.clone();
    tokio::spawn(async move {
        let stall = stall2;
        let mut conn_id = 0u32;
        loop {
            let Ok((client, _)) = listener.accept().await else {
                break;
            };
            conn_id += 1;
            let log = l2.clone();
            let broker = broker.clone();
            let stall = stall.clone();
            tokio::spawn(async move {
                let _ = client.set_nodelay(true);
                let Ok(server) = TcpStream::connect(&broker).await else {
                    return;
                };
                let _ = server.set_nodelay(true);
                let (cr, cw) = client.into_split();
                let (sr, sw) = server.into_split();
                let a = tokio::spawn(pump(
                    cr,
                    sw,
                    Dir::ClientToBroker,
                    conn_id,
                    log.clone(),
                    Some(stall),
                ));
                let b = tokio::spawn(pump(sr, cw, Dir::BrokerToClient, conn_id, log, None));
                let _ = tokio::join!(a, b);
            });
        }
    });
    Ok((port, log, stall))
}

async fn pump(
    mut rd: tokio::net::tcp::OwnedReadHalf,
    mut wr: tokio::net::tcp::OwnedWriteHalf,
    dir: Dir,
    conn: u32,
    log: WireLog,
    stall: Option<Arc<AtomicBool>>,
) {
    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut chunk = vec![0u8; 64 * 1024];
    loop {
        if let Some(st) = &stall {
            while st.load(Ordering::Relaxed) {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        }
        let n = match rd.read(&mut chunk).await {
            Ok(0) | Err(_) => {
                log.push(conn, dir, Pkt::Closed);
                let _ = wr.shutdown().await;
                return;
            }
            Ok(n) => n,
        };
        if wr.write_all(&chunk[..n]).await.is_err() {
            log.push(conn, dir, Pkt::Closed);
            return;
        }
        buf.extend_from_slice(&chunk[..n]);
        parse(&mut buf, dir, conn, &log);
    }
}

fn parse(buf: &mut BytesMut, dir: Dir, conn: u32, log: &WireLog) {
    loop {
        if buf.len() < 2 {
            return;
        }
        let b0 = buf[0];
        let mut mult = 1usize;
        let mut rl = 0usize;
        let mut i = 1usize;
        loop {
            if i >= buf.len() {
                return;
            }
            let d = buf[i];
            rl += (d & 0x7f) as usize * mult;
            mult *= 128;
            i += 1;
            if d & 0x80 == 0 {
                break;
            }
            if i > 4 {
                buf.clear();
                return;
            }
        }
        if buf.len() < i + rl {
            return;
        }
        let pkt = buf.split_to(i + rl);
        let body = &pkt[i..];
        let u16at = |o: usize| u16::from_be_bytes([body[o], body[o + 1]]);
        let p = match b0 >> 4 {
            1 => {
                let nl = u16at(0) as usize;
                let flags = body[2 + nl + 1];
                let ka = u16at(2 + nl + 2);
                let cl = u16at(2 + nl + 4) as usize;
                let cid = String::from_utf8_lossy(&body[2 + nl + 6..2 + nl + 6 + cl]).into_owned();
                Pkt::Connect {
                    clean: flags & 0x02 != 0,
                    keepalive: ka,
                    client_id: cid,
                }
            }
            2 => Pkt::ConnAck {
                session_present: body[0] & 1 != 0,
                rc: body[1],
            },
            3 => {
                let qos = (b0 >> 1) & 3;
                let tl = u16at(0) as usize;
                let (pkid, hdr) = if qos > 0 {
                    (u16at(2 + tl), 4 + tl)
                } else {
                    (0, 2 + tl)
                };
                Pkt::Publish {
                    pkid,
                    qos,
                    dup: b0 & 0x08 != 0,
                    payload_len: rl - hdr,
                }
            }
            4 => Pkt::PubAck(u16at(0)),
            5 => Pkt::PubRec(u16at(0)),
            6 => Pkt::PubRel(u16at(0)),
            7 => Pkt::PubComp(u16at(0)),
            8 => Pkt::Subscribe(u16at(0)),
            9 => Pkt::SubAck(u16at(0)),
            12 => Pkt::PingReq,
            13 => Pkt::PingResp,
            14 => Pkt::Disconnect,
            t => Pkt::Other(t),
        };
        log.push(conn, dir, p);
    }
}

#[derive(Debug, Serialize)]
pub struct OrderReport {
    pub publishes_seen: usize,
    pub acks_seen: usize,
    pub unmatched_acks: usize,
    pub acks_in_receive_order: bool,
    pub descents: usize,
    pub first_descent_at_ack_index: Option<usize>,
}

/// Compare the order of PUBACKs (or PUBRECs for QoS 2) on the wire with the order in
/// which the matching PUBLISH packets were delivered by the broker.
pub fn order_report(ev: &[WireEvent], qos2: bool) -> OrderReport {
    use std::collections::{HashMap, VecDeque};
    let mut pos: HashMap<u16, VecDeque<usize>> = HashMap::new();
    let mut publishes = 0usize;
    let mut acks = 0usize;
    let mut unmatched = 0usize;
    let mut last: Option<usize> = None;
    let mut descents = 0usize;
    let mut first_descent = None;
    for e in ev {
        match (&e.dir, &e.pkt) {
            (Dir::BrokerToClient, Pkt::Publish { pkid, qos, dup, .. }) if *qos >= 1 => {
                let _ = dup;
                pos.entry(*pkid).or_default().push_back(publishes);
                publishes += 1;
            }
            (Dir::ClientToBroker, Pkt::PubAck(p)) if !qos2 => {
                match_ack(
                    &mut pos,
                    *p,
                    &mut last,
                    &mut descents,
                    &mut first_descent,
                    acks,
                    &mut unmatched,
                );
                acks += 1;
            }
            (Dir::ClientToBroker, Pkt::PubRec(p)) if qos2 => {
                match_ack(
                    &mut pos,
                    *p,
                    &mut last,
                    &mut descents,
                    &mut first_descent,
                    acks,
                    &mut unmatched,
                );
                acks += 1;
            }
            _ => {}
        }
    }
    OrderReport {
        publishes_seen: publishes,
        acks_seen: acks,
        unmatched_acks: unmatched,
        acks_in_receive_order: descents == 0,
        descents,
        first_descent_at_ack_index: first_descent,
    }
}

fn match_ack(
    pos: &mut std::collections::HashMap<u16, std::collections::VecDeque<usize>>,
    pkid: u16,
    last: &mut Option<usize>,
    descents: &mut usize,
    first_descent: &mut Option<usize>,
    ack_idx: usize,
    unmatched: &mut usize,
) {
    match pos.get_mut(&pkid).and_then(|q| q.pop_front()) {
        Some(p) => {
            if let Some(l) = *last {
                if p < l {
                    *descents += 1;
                    first_descent.get_or_insert(ack_idx);
                }
            }
            *last = Some(p);
        }
        None => *unmatched += 1,
    }
}

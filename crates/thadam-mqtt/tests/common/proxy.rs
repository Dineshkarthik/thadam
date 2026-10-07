//! An MQTT 3.1.1 aware TCP proxy used as an independent wire-level oracle.
//!
//! The source under test connects to the proxy, which forwards bytes untouched
//! and records every packet with a timestamp. Tests read the log to see what
//! the broker really received (PUBACK order, PINGREQs, who closed a socket)
//! and use the controls to cut connections, refuse new ones, or delay a
//! connect.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Dir {
    ClientToBroker,
    BrokerToClient,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CloseBy {
    Client,
    Broker,
    Proxy,
}

#[derive(Clone, Debug)]
pub enum Pkt {
    Connect {
        clean: bool,
        keepalive: u16,
    },
    ConnAck {
        session_present: bool,
        rc: u8,
    },
    Publish {
        pkid: u16,
        qos: u8,
        dup: bool,
        payload: Bytes,
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
    Closed(CloseBy),
}

#[derive(Clone, Debug)]
pub struct WireEvent {
    pub t: Duration,
    pub conn: u32,
    pub dir: Dir,
    pub pkt: Pkt,
}

#[derive(Default)]
struct ConnInfo {
    last_c2s: Option<Instant>,
    max_gap: Duration,
    closed: bool,
    v5: bool,
}

struct State {
    start: Instant,
    events: Mutex<Vec<WireEvent>>,
    conns: Mutex<HashMap<u32, ConnInfo>>,
    killer: Mutex<CancellationToken>,
    blocked: AtomicBool,
    connect_delay_ms: AtomicU64,
    accepted: AtomicU32,
}

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Clone)]
pub struct Proxy {
    pub port: u16,
    state: Arc<State>,
}

impl Proxy {
    pub async fn start(broker: String) -> Proxy {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy");
        let port = listener.local_addr().expect("proxy addr").port();
        let state = Arc::new(State {
            start: Instant::now(),
            events: Mutex::new(Vec::new()),
            conns: Mutex::new(HashMap::new()),
            killer: Mutex::new(CancellationToken::new()),
            blocked: AtomicBool::new(false),
            connect_delay_ms: AtomicU64::new(0),
            accepted: AtomicU32::new(0),
        });
        let st = state.clone();
        tokio::spawn(async move {
            loop {
                let Ok((client, _)) = listener.accept().await else {
                    break;
                };
                let conn = st.accepted.fetch_add(1, Ordering::Relaxed) + 1;
                let token = lock(&st.killer).clone();
                let st = st.clone();
                let broker = broker.clone();
                tokio::spawn(async move {
                    if st.blocked.load(Ordering::Relaxed) {
                        return;
                    }
                    let _ = client.set_nodelay(true);
                    let delay = st.connect_delay_ms.load(Ordering::Relaxed);
                    if delay > 0 {
                        tokio::select! {
                            () = tokio::time::sleep(Duration::from_millis(delay)) => {}
                            () = token.cancelled() => return,
                        }
                    }
                    let Ok(server) = TcpStream::connect(&broker).await else {
                        return;
                    };
                    let _ = server.set_nodelay(true);
                    lock(&st.conns).insert(conn, ConnInfo::default());
                    let (cr, cw) = client.into_split();
                    let (sr, sw) = server.into_split();
                    let first = Arc::new(AtomicBool::new(false));
                    let a = tokio::spawn(pump(
                        cr,
                        sw,
                        Dir::ClientToBroker,
                        conn,
                        st.clone(),
                        token.clone(),
                        first.clone(),
                    ));
                    let b = tokio::spawn(pump(
                        sr,
                        cw,
                        Dir::BrokerToClient,
                        conn,
                        st.clone(),
                        token,
                        first,
                    ));
                    let _ = tokio::join!(a, b);
                });
            }
        });
        Proxy { port, state }
    }

    pub fn url(&self) -> String {
        format!("mqtt://127.0.0.1:{}", self.port)
    }

    /// Cuts every live connection from the proxy side.
    pub fn kill_connections(&self) {
        let mut killer = lock(&self.state.killer);
        killer.cancel();
        *killer = CancellationToken::new();
    }

    /// While blocked, new connections are accepted and immediately closed.
    pub fn set_blocked(&self, blocked: bool) {
        self.state.blocked.store(blocked, Ordering::Relaxed);
    }

    /// Delays dialing the broker for every new connection.
    pub fn set_connect_delay(&self, delay: Duration) {
        self.state.connect_delay_ms.store(
            u64::try_from(delay.as_millis()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    pub fn events(&self) -> Vec<WireEvent> {
        lock(&self.state.events).clone()
    }

    /// Connections that reached the broker.
    pub fn broker_connections(&self) -> usize {
        lock(&self.state.conns).len()
    }

    pub fn connects_seen(&self) -> usize {
        self.events()
            .iter()
            .filter(|e| matches!(e.pkt, Pkt::Connect { .. }))
            .count()
    }

    pub fn pingreqs(&self) -> usize {
        self.events()
            .iter()
            .filter(|e| e.dir == Dir::ClientToBroker && matches!(e.pkt, Pkt::PingReq))
            .count()
    }

    pub fn broker_closes(&self) -> usize {
        self.events()
            .iter()
            .filter(|e| matches!(e.pkt, Pkt::Closed(CloseBy::Broker)))
            .count()
    }

    /// Times the broker would have dropped (or did drop) the connection for
    /// missing pings: a client-to-broker silence above 1.5 keepalives, or a
    /// close the broker started.
    pub fn keepalive_violations(&self, keepalive: Duration) -> u64 {
        let limit = keepalive.mul_f64(1.5);
        let silent = lock(&self.state.conns)
            .values()
            .filter(|c| {
                let open_gap = if c.closed {
                    Duration::ZERO
                } else {
                    c.last_c2s.map_or(Duration::ZERO, |t| t.elapsed())
                };
                c.max_gap.max(open_gap) > limit
            })
            .count() as u64;
        silent + self.broker_closes() as u64
    }

    /// Payloads the broker saw acknowledged, in the order PUBACKs arrived.
    pub fn acked_payloads(&self) -> Vec<Bytes> {
        let mut map: HashMap<(u32, u16), Bytes> = HashMap::new();
        let mut out = Vec::new();
        for e in self.events() {
            match (e.dir, e.pkt) {
                (
                    Dir::BrokerToClient,
                    Pkt::Publish {
                        pkid, qos, payload, ..
                    },
                ) if qos > 0 => {
                    map.insert((e.conn, pkid), payload);
                }
                (Dir::ClientToBroker, Pkt::PubAck(pkid) | Pkt::PubRec(pkid)) => {
                    if let Some(p) = map.get(&(e.conn, pkid)) {
                        out.push(p.clone());
                    }
                }
                _ => {}
            }
        }
        out
    }

    pub fn pubacks_seen(&self) -> usize {
        self.events()
            .iter()
            .filter(|e| e.dir == Dir::ClientToBroker && matches!(e.pkt, Pkt::PubAck(_)))
            .count()
    }

    /// PUBACK order against PUBLISH delivery order: descents are acks that
    /// went out before an earlier delivery's ack.
    pub fn ack_order(&self) -> AckOrder {
        let mut positions: HashMap<(u32, u16), std::collections::VecDeque<usize>> = HashMap::new();
        let (mut publishes, mut acks, mut unmatched, mut descents) = (0, 0, 0, 0);
        let mut last: Option<usize> = None;
        for e in self.events() {
            match (e.dir, e.pkt) {
                (Dir::BrokerToClient, Pkt::Publish { pkid, qos, .. }) if qos >= 1 => {
                    positions
                        .entry((e.conn, pkid))
                        .or_default()
                        .push_back(publishes);
                    publishes += 1;
                }
                (Dir::ClientToBroker, Pkt::PubAck(pkid)) => {
                    acks += 1;
                    match positions
                        .get_mut(&(e.conn, pkid))
                        .and_then(|q| q.pop_front())
                    {
                        Some(p) => {
                            if last.is_some_and(|l| p < l) {
                                descents += 1;
                            }
                            last = Some(p);
                        }
                        None => unmatched += 1,
                    }
                }
                _ => {}
            }
        }
        AckOrder {
            publishes,
            acks,
            unmatched,
            descents,
        }
    }

    pub fn publishes_to_client(&self) -> Vec<(u16, bool)> {
        self.events()
            .iter()
            .filter_map(|e| match (&e.dir, &e.pkt) {
                (Dir::BrokerToClient, Pkt::Publish { pkid, dup, qos, .. }) if *qos > 0 => {
                    Some((*pkid, *dup))
                }
                _ => None,
            })
            .collect()
    }

    pub fn uptime(&self) -> Duration {
        self.state.start.elapsed()
    }
}

#[derive(Debug)]
pub struct AckOrder {
    pub publishes: usize,
    pub acks: usize,
    pub unmatched: usize,
    pub descents: usize,
}

async fn pump(
    mut rd: OwnedReadHalf,
    mut wr: OwnedWriteHalf,
    dir: Dir,
    conn: u32,
    st: Arc<State>,
    killed: CancellationToken,
    first_close: Arc<AtomicBool>,
) {
    let mut buf = BytesMut::with_capacity(64 * 1024);
    let mut chunk = vec![0u8; 64 * 1024];
    let close = |by: CloseBy| {
        if !first_close.swap(true, Ordering::AcqRel) {
            push(&st, conn, dir, Pkt::Closed(by));
            if let Some(c) = lock(&st.conns).get_mut(&conn) {
                c.closed = true;
            }
        }
    };
    loop {
        let n = tokio::select! {
            r = rd.read(&mut chunk) => match r {
                Ok(0) | Err(_) => {
                    close(match dir {
                        Dir::ClientToBroker => CloseBy::Client,
                        Dir::BrokerToClient => CloseBy::Broker,
                    });
                    let _ = wr.shutdown().await;
                    return;
                }
                Ok(n) => n,
            },
            () = killed.cancelled() => {
                close(CloseBy::Proxy);
                return;
            }
        };
        buf.extend_from_slice(&chunk[..n]);
        parse(&mut buf, dir, conn, &st);
        if wr.write_all(&chunk[..n]).await.is_err() {
            close(CloseBy::Proxy);
            return;
        }
    }
}

fn push(st: &State, conn: u32, dir: Dir, pkt: Pkt) {
    let now = Instant::now();
    if dir == Dir::ClientToBroker
        && !matches!(pkt, Pkt::Closed(_))
        && let Some(c) = lock(&st.conns).get_mut(&conn)
    {
        if let Some(last) = c.last_c2s {
            c.max_gap = c.max_gap.max(now.duration_since(last));
        }
        c.last_c2s = Some(now);
    }
    lock(&st.events).push(WireEvent {
        t: now.duration_since(st.start),
        conn,
        dir,
        pkt,
    });
}

fn parse(buf: &mut BytesMut, dir: Dir, conn: u32, st: &State) {
    loop {
        if buf.len() < 2 {
            return;
        }
        let b0 = buf[0];
        let (mut mult, mut rl, mut i) = (1usize, 0usize, 1usize);
        loop {
            if i >= buf.len() {
                return;
            }
            let d = buf[i];
            rl += usize::from(d & 0x7f) * mult;
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
        let packet = buf.split_to(i + rl).freeze();
        let body = packet.slice(i..);
        let u16at = |o: usize| {
            body.get(o..o + 2)
                .map_or(0, |b| u16::from_be_bytes([b[0], b[1]]))
        };
        let pkt = match b0 >> 4 {
            1 => {
                let name_len = usize::from(u16at(0));
                let level = body.get(2 + name_len).copied().unwrap_or(4);
                if let Some(c) = lock(&st.conns).get_mut(&conn) {
                    c.v5 = level == 5;
                }
                let flags = body.get(2 + name_len + 1).copied().unwrap_or(0);
                Pkt::Connect {
                    clean: flags & 0x02 != 0,
                    keepalive: u16at(2 + name_len + 2),
                }
            }
            2 => Pkt::ConnAck {
                session_present: body.first().is_some_and(|b| b & 1 != 0),
                rc: body.get(1).copied().unwrap_or(0),
            },
            3 => {
                let qos = (b0 >> 1) & 3;
                let topic_len = usize::from(u16at(0));
                let (pkid, mut header) = if qos > 0 {
                    (u16at(2 + topic_len), 4 + topic_len)
                } else {
                    (0, 2 + topic_len)
                };
                if lock(&st.conns).get(&conn).is_some_and(|c| c.v5) {
                    // MQTT 5 puts a variable-length property block before the payload.
                    let (mut len, mut shift, mut used) = (0usize, 0u32, 0usize);
                    for byte in body.iter().skip(header).take(4) {
                        len |= usize::from(byte & 0x7f) << shift;
                        shift += 7;
                        used += 1;
                        if byte & 0x80 == 0 {
                            break;
                        }
                    }
                    header += used + len;
                }
                Pkt::Publish {
                    pkid,
                    qos,
                    dup: b0 & 0x08 != 0,
                    payload: body.slice(header.min(body.len())..),
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
        push(st, conn, dir, pkt);
    }
}

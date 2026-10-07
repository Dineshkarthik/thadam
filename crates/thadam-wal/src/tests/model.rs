//! Model-based test: random append, read, commit, maintain (delete), crash,
//! power-loss, and reopen sequences checked against an in-memory model.

use std::collections::BTreeMap;

use proptest::prelude::*;
use thadam_core::WalPos;

use super::util::*;
use crate::failpoints::Failpoints;
use crate::{
    ConsumerSpec, CursorState, Fsync, Gap, OnFull, ReadItem, RecoveryReport, StartFrom, Wal,
    WalOptions, WalReader, WalWriter,
};

const CONSUMERS: [&str; 2] = ["req", "opt"];

#[derive(Clone, Debug)]
enum Op {
    Append {
        frames: u8,
        size: u16,
    },
    Read {
        consumer: usize,
        max: u16,
    },
    Commit {
        consumer: usize,
    },
    Maintain,
    /// Process crash: no final flush, seal, or clean marker.
    Crash,
    /// Crash, then drop everything after the active segment's last flush.
    PowerLoss,
    /// Clean close and reopen.
    Reopen,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        6 => (1u8..5, 16u16..1200).prop_map(|(frames, size)| Op::Append { frames, size }),
        4 => (0usize..2, 64u16..6000).prop_map(|(consumer, max)| Op::Read { consumer, max }),
        3 => (0usize..2).prop_map(|consumer| Op::Commit { consumer }),
        2 => Just(Op::Maintain),
        1 => Just(Op::Crash),
        2 => Just(Op::PowerLoss),
        1 => Just(Op::Reopen),
    ]
}

/// `THADAM_WAL_MODEL_CASES` raises the case count for longer runs.
fn cases() -> u32 {
    std::env::var("THADAM_WAL_MODEL_CASES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(48)
}

#[derive(Default)]
struct Model {
    /// Every acknowledged frame not lost to a power loss, by position.
    written: BTreeMap<u64, (u32, u64)>,
    next: u64,
    first: u64,
    committed: [u64; 2],
    read_pos: [u64; 2],
    run: u32,
}

struct Sut<'a> {
    env: &'a Env,
    fsync: Fsync,
    rt: tokio::runtime::Runtime,
    wal: Option<Wal>,
    writer: Option<WalWriter>,
    readers: Vec<WalReader>,
    fp: Failpoints,
}

impl Sut<'_> {
    fn opts(&self) -> WalOptions {
        let mut o = small_opts(OnFull::Block);
        o.fsync = self.fsync;
        o.consumers = vec![
            ConsumerSpec::required(CONSUMERS[0]),
            ConsumerSpec::optional(CONSUMERS[1]),
        ];
        o.failpoints = self.fp.clone();
        o
    }

    fn open(&mut self) -> RecoveryReport {
        self.fp = Failpoints::default();
        let (wal, report) = self.env.open(self.opts());
        self.readers = CONSUMERS
            .iter()
            .map(|c| wal.reader(c, StartFrom::Earliest).unwrap())
            .collect();
        self.writer = None;
        self.wal = Some(wal);
        report
    }

    fn wal(&self) -> &Wal {
        self.wal.as_ref().unwrap()
    }

    fn writer(&mut self) -> WalWriter {
        if self.writer.is_none() {
            self.writer = Some(self.wal().start_writer().unwrap());
        }
        self.writer.clone().unwrap()
    }

    fn shutdown(&mut self, crash: bool) {
        if crash {
            self.fp.crash_on_close();
        } else {
            self.rt.block_on(self.wal().close()).unwrap();
        }
        self.readers.clear();
        self.writer = None;
        self.wal = None;
    }
}

fn after_reopen(sut: &mut Sut<'_>, m: &mut Model, report: &RecoveryReport) {
    m.run += 1;
    m.next = report.next_seq.get();
    m.first = report.first_seq.get();
    for (c, name) in CONSUMERS.iter().enumerate() {
        // Readers reopen at their cursor, or at the oldest retained frame
        // (`StartFrom::Earliest`) if they never committed.
        m.read_pos[c] = if m.committed[c] > 0 {
            m.committed[c] + 1
        } else {
            m.first
        };
        let stored = sut.wal().cursor(name).map_or(0, |s| s.committed.get());
        assert_eq!(stored, m.committed[c]);
    }
    verify_all(sut, m);
}

/// A full read from the first retained frame matches the model exactly; the
/// only gaps are positions that never existed.
fn verify_all(sut: &Sut<'_>, m: &Model) {
    let mut r = sut
        .wal()
        .reader("verify", StartFrom::At(WalPos::new(m.first)))
        .unwrap();
    let mut frames = Vec::new();
    for item in drain(&mut r, 1 << 20) {
        match item {
            Item::Frame(seq, id) => frames.push((seq, id.unwrap())),
            Item::Gap(Gap::Missing { range }) => {
                assert!(
                    m.written
                        .range(range.first().get()..=range.last().get())
                        .next()
                        .is_none()
                );
            }
            Item::Gap(g) => panic!("unexpected gap in a full read: {g:?}"),
        }
    }
    let want: Vec<(u64, (u32, u64))> = m
        .written
        .range(m.first..)
        .map(|(s, id)| (*s, *id))
        .collect();
    assert_eq!(frames, want);
}

fn run(fsync: Fsync, ops: &[Op]) {
    let env = Env::new();
    let mut sut = Sut {
        env: &env,
        fsync,
        rt: rt(),
        wal: None,
        writer: None,
        readers: Vec::new(),
        fp: Failpoints::default(),
    };
    let mut m = Model::default();
    let report = sut.open();
    after_reopen(&mut sut, &mut m, &report);
    for op in ops {
        match *op {
            Op::Append { frames, size } => {
                let w = sut.writer();
                let first = m.next;
                let batches = (0..u64::from(frames))
                    .map(|j| batch(m.run, first + j, 1 + (j % 3) as usize, usize::from(size)));
                let got = sut.rt.block_on(append_all(&w, batches));
                for (j, r) in got.into_iter().enumerate() {
                    let seq = first + j as u64;
                    assert_eq!(r.unwrap(), seq);
                    m.written.insert(seq, (m.run, seq));
                }
                m.next = first + u64::from(frames);
            }
            Op::Read { consumer: c, max } => {
                let item = sut.readers[c].try_next(usize::from(max)).unwrap();
                match item {
                    None => {
                        let last = m.written.keys().next_back().copied().unwrap_or(0);
                        assert!(
                            m.read_pos[c] > last,
                            "reader {c} stopped early at {}",
                            m.read_pos[c]
                        );
                    }
                    Some(ReadItem::Frames(chunk)) => {
                        for f in chunk.frames() {
                            let seq = f.seq().get();
                            assert_eq!(seq, m.read_pos[c]);
                            assert_eq!(identify(f), m.written.get(&seq).copied(), "frame {seq}");
                            m.read_pos[c] += 1;
                        }
                    }
                    Some(ReadItem::Gap(g)) => {
                        let range = g.range();
                        assert_eq!(range.first().get(), m.read_pos[c]);
                        match &g {
                            Gap::Evicted { cursor, .. } => {
                                assert_eq!(cursor, CONSUMERS[c]);
                                assert_eq!(range.last().get() + 1, m.first);
                                if c == 0 {
                                    // Only positions that never held a frame may be
                                    // reported to a required consumer.
                                    let (a, b) = (range.first().get(), range.last().get());
                                    assert!(
                                        m.written.range(a..=b).next().is_none(),
                                        "a required consumer lost unread data: {g:?}"
                                    );
                                }
                            }
                            Gap::Missing { .. } => {
                                let (a, b) = (range.first().get(), range.last().get());
                                assert!(m.written.range(a..=b).next().is_none());
                            }
                            Gap::Corrupt { .. } => panic!("corrupt gap: {g:?}"),
                        }
                        m.read_pos[c] = range.last().get() + 1;
                    }
                }
            }
            Op::Commit { consumer: c } => {
                let pos = m.read_pos[c] - 1;
                if pos > m.committed[c] {
                    sut.wal()
                        .commit_cursor_blocking(CONSUMERS[c], CursorState::at(WalPos::new(pos)))
                        .unwrap();
                    m.committed[c] = pos;
                }
            }
            Op::Maintain => {
                sut.wal().maintain_blocking().unwrap();
                let first = sut.wal().stats().first_seq.get();
                assert!(first >= m.first, "retention went backwards");
                assert!(
                    first == m.first || first <= m.committed[0] + 1,
                    "deleted past the required cursor: first {first}, cursor {}",
                    m.committed[0]
                );
                m.first = first;
            }
            Op::Crash => {
                sut.shutdown(true);
                let report = sut.open();
                assert_eq!(report.next_seq.get(), m.next, "{report:?}");
                assert_eq!(report.first_seq.get(), m.first);
                after_reopen(&mut sut, &mut m, &report);
            }
            Op::Reopen => {
                let had_writer = sut.writer.is_some();
                sut.shutdown(false);
                let report = sut.open();
                assert_eq!(report.clean_shutdown, had_writer);
                assert_eq!(report.next_seq.get(), m.next);
                after_reopen(&mut sut, &mut m, &report);
            }
            Op::PowerLoss => {
                let synced = sut.fp.last_synced();
                sut.shutdown(true);
                if let Some((path, len)) = synced
                    && path.exists()
                {
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .unwrap()
                        .set_len(len)
                        .unwrap();
                }
                let report = sut.open();
                let durable = report.durable_end.map_or(0, WalPos::get);
                if fsync != Fsync::Never {
                    assert_eq!(durable + 1, m.next, "acknowledged frames lost: {report:?}");
                }
                let lost: Vec<u64> = m.written.range(durable + 1..).map(|(s, _)| *s).collect();
                for s in lost {
                    m.written.remove(&s);
                }
                let max_cursor = m.committed.iter().copied().max().unwrap_or(0);
                assert!(
                    report.next_seq.get() > max_cursor,
                    "position reuse below a durable cursor"
                );
                assert!(report.next_seq.get() > durable);
                after_reopen(&mut sut, &mut m, &report);
            }
        }
        assert_eq!(
            env.budget.used(thadam_core::BudgetCategory::Wal),
            env.segment_bytes_on_disk(),
            "disk budget drifted from the bytes on disk after {op:?}"
        );
    }
    sut.shutdown(false);
    let report = sut.open();
    after_reopen(&mut sut, &mut m, &report);
}

proptest! {
    #![proptest_config(ProptestConfig { cases: cases(), failure_persistence: None, ..ProptestConfig::default() })]

    #[test]
    fn wal_matches_model_always(ops in proptest::collection::vec(op(), 1..40)) {
        run(Fsync::Always, &ops);
    }

    #[test]
    fn wal_matches_model_never(ops in proptest::collection::vec(op(), 1..40)) {
        run(Fsync::Never, &ops);
    }
}

//! Fuzzes the stateful Sparkplug decoder. The input is a sequence of
//! messages, each `[topic selector][length][payload]`, fed in order to one
//! fresh decoder so alias tables, `seq` and `bdSeq` state evolve the way a
//! session does. A leading flag byte seeds the decoder from a stored BIRTH
//! first (the restart path), and the topic parser also sees raw text.
#![no_main]

use libfuzzer_sys::fuzz_target;
use thadam_core::{BirthKey, BirthRecord};
use thadam_sparkplug::decode::Decoder;
use thadam_sparkplug::topic;

const TOPICS: &[&str] = &[
    "spBv1.0/g/NBIRTH/e1",
    "spBv1.0/g/NDATA/e1",
    "spBv1.0/g/NDEATH/e1",
    "spBv1.0/g/NCMD/e1",
    "spBv1.0/g/DBIRTH/e1/d1",
    "spBv1.0/g/DDATA/e1/d1",
    "spBv1.0/g/DDEATH/e1/d1",
    "spBv1.0/g/DCMD/e1/d1",
    "spBv1.0/g/STATE/host",
    "spBv1.0/g/DDATA/e1",
    "spBv1.0/g/NDATA/e2/d2",
    "spBv1.0/g2/NBIRTH/e1",
];

fn take<'a>(data: &mut &'a [u8], n: usize) -> &'a [u8] {
    let n = n.min(data.len());
    let (head, tail) = data.split_at(n);
    *data = tail;
    head
}

fuzz_target!(|data: &[u8]| {
    let mut rest = data;
    let flags = take(&mut rest, 1).first().copied().unwrap_or(0);
    let _ = topic::parse(&String::from_utf8_lossy(&rest[..rest.len().min(64)]));

    let mut decoder = if flags & 1 == 1 {
        let len = usize::from(take(&mut rest, 1).first().copied().unwrap_or(0));
        let birth = take(&mut rest, len).to_vec();
        let device = (flags & 2 == 2).then_some("d1");
        let record = BirthRecord {
            birth,
            bd_seq: (flags & 4 == 4).then_some(7),
            seq: None,
            dead: flags & 8 == 8,
        };
        match Decoder::seeded([(BirthKey::new("g", "e1", device), record)]) {
            Ok(d) => d,
            Err(_) => return,
        }
    } else {
        Decoder::new()
    };

    while !rest.is_empty() {
        let selector = take(&mut rest, 1)[0];
        let len = usize::from(take(&mut rest, 1).first().copied().unwrap_or(0));
        let payload = take(&mut rest, len);
        let name = TOPICS[usize::from(selector) % TOPICS.len()];
        if let Some(topic) = topic::parse(name) {
            let _ = decoder.handle(&topic, payload);
        }
    }
});

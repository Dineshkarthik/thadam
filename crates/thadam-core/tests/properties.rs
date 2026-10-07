//! Property tests for the model invariants.
#![cfg(not(loom))]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use proptest::prelude::*;
use thadam_core::{
    AssetPath, AssetPathError, BatchId, Event, EventBuildError, EventId, FieldDef, FieldType,
    Payload, Quality, QualityFlags, Record, Shape, ShapeRef, ShapeRegistry, TimeDomain, Times,
    Timestamp, Utf8Bytes, Value, ValueType, WalEpoch, WalPos, WalRange,
};

const ALL_TYPES: [ValueType; 21] = [
    ValueType::Null,
    ValueType::Bool,
    ValueType::I8,
    ValueType::I16,
    ValueType::I32,
    ValueType::I64,
    ValueType::U8,
    ValueType::U16,
    ValueType::U32,
    ValueType::U64,
    ValueType::F32,
    ValueType::F64,
    ValueType::Str,
    ValueType::Bytes,
    ValueType::Ts,
    ValueType::F32s,
    ValueType::F64s,
    ValueType::I32s,
    ValueType::I64s,
    ValueType::List,
    ValueType::Record,
];

fn hash_of(v: &Value) -> u64 {
    let mut h = DefaultHasher::new();
    v.hash(&mut h);
    h.finish()
}

fn arb_scalar() -> impl Strategy<Value = Value> {
    prop_oneof![
        Just(Value::Null),
        any::<bool>().prop_map(Value::Bool),
        any::<i8>().prop_map(Value::I8),
        any::<i16>().prop_map(Value::I16),
        any::<i32>().prop_map(Value::I32),
        any::<i64>().prop_map(Value::I64),
        any::<u8>().prop_map(Value::U8),
        any::<u16>().prop_map(Value::U16),
        any::<u32>().prop_map(Value::U32),
        any::<u64>().prop_map(Value::U64),
        proptest::num::f32::ANY.prop_map(Value::F32),
        proptest::num::f64::ANY.prop_map(Value::F64),
        ".{0,12}".prop_map(Value::str),
        proptest::collection::vec(any::<u8>(), 0..8).prop_map(|b| Value::bytes(b.into())),
        any::<i64>().prop_map(|n| Value::Ts(Timestamp::from_nanos(n))),
        proptest::collection::vec(proptest::num::f32::ANY, 0..5).prop_map(Value::from),
        proptest::collection::vec(proptest::num::f64::ANY, 0..5).prop_map(Value::from),
        proptest::collection::vec(any::<i32>(), 0..5).prop_map(Value::from),
        proptest::collection::vec(any::<i64>(), 0..5).prop_map(Value::from),
    ]
}

fn arb_value() -> impl Strategy<Value = Value> {
    arb_scalar().prop_recursive(3, 24, 4, |inner| {
        proptest::collection::vec(inner, 0..4).prop_map(Value::List)
    })
}

fn int_of(v: &Value) -> Option<i128> {
    match v {
        Value::I8(x) => Some(i128::from(*x)),
        Value::I16(x) => Some(i128::from(*x)),
        Value::I32(x) => Some(i128::from(*x)),
        Value::I64(x) => Some(i128::from(*x)),
        Value::U8(x) => Some(i128::from(*x)),
        Value::U16(x) => Some(i128::from(*x)),
        Value::U32(x) => Some(i128::from(*x)),
        Value::U64(x) => Some(i128::from(*x)),
        _ => None,
    }
}

#[test]
fn widening_relation_is_a_partial_order_on_types() {
    for a in ALL_TYPES {
        assert!(a.widens_to(a));
        for b in ALL_TYPES {
            if a != b && a != ValueType::Null {
                assert!(!(a.widens_to(b) && b.widens_to(a)), "{a} <-> {b}");
            }
            for c in ALL_TYPES {
                if a.widens_to(b) && b.widens_to(c) {
                    assert!(a.widens_to(c), "{a} -> {b} -> {c}");
                }
            }
        }
    }
}

proptest! {
    #[test]
    fn widening_never_narrows_or_changes_the_value(v in arb_scalar(), ti in 0..ALL_TYPES.len()) {
        let target = ALL_TYPES[ti];
        match v.widen(target) {
            Some(w) => {
                prop_assert!(v.value_type().widens_to(target));
                if v.is_null() {
                    prop_assert!(w.is_null());
                } else {
                    prop_assert_eq!(w.value_type(), target);
                }
                if let (Some(a), Some(b)) = (int_of(&v), int_of(&w)) {
                    prop_assert_eq!(a, b);
                }
                if let (Value::F32(a), Value::F64(b)) = (&v, &w) {
                    prop_assert_eq!(f64::from(*a).to_bits(), b.to_bits());
                }
                if let (Value::F32s(a), Value::F64s(b)) = (&v, &w) {
                    prop_assert!(a.iter().zip(b.iter()).all(|(x, y)| f64::from(*x).to_bits() == y.to_bits()));
                }
                if let (Value::I32s(a), Value::I64s(b)) = (&v, &w) {
                    prop_assert!(a.iter().zip(b.iter()).all(|(x, y)| i64::from(*x) == *y));
                }
            }
            None => prop_assert!(!v.value_type().widens_to(target)),
        }
    }

    #[test]
    fn widening_is_idempotent_for_the_target_type(v in arb_scalar(), ti in 0..ALL_TYPES.len()) {
        let target = ALL_TYPES[ti];
        if let Some(w) = v.widen(target) {
            prop_assert_eq!(w.widen(target), Some(w));
        }
    }

    #[test]
    fn equality_is_reflexive_and_matches_hash(a in arb_value(), b in arb_value()) {
        prop_assert_eq!(&a, &a.clone());
        prop_assert_eq!(hash_of(&a), hash_of(&a.clone()));
        if a == b {
            prop_assert_eq!(hash_of(&a), hash_of(&b));
        }
    }

    #[test]
    fn typed_array_mutation_never_leaks_into_a_clone(
        data in proptest::collection::vec(proptest::num::f64::ANY, 1..16),
        idx in any::<prop::sample::Index>(),
        new in any::<f64>(),
    ) {
        let mut a = Value::from(data.clone());
        let b = a.clone();
        let i = idx.index(data.len());
        a.f64s_mut().unwrap()[i] = new;
        prop_assert_eq!(b, Value::from(data));
        prop_assert_eq!(a.as_f64s().unwrap()[i].to_bits(), new.to_bits());
    }

    #[test]
    fn utf8_validation_agrees_with_std(bytes in proptest::collection::vec(any::<u8>(), 0..32)) {
        let ours = Utf8Bytes::from_utf8_slice(&bytes);
        let std_view = std::str::from_utf8(&bytes);
        prop_assert_eq!(ours.is_ok(), std_view.is_ok());
        if let (Ok(u), Ok(s)) = (ours, std_view) {
            prop_assert_eq!(u.as_str(), s);
            prop_assert_eq!(u.as_bytes(), &bytes[..]);
        }
    }

    #[test]
    fn batch_id_text_form_round_trips(epoch in any::<u64>(), a in any::<u64>(), b in any::<u64>()) {
        let (first, last) = (a.min(b), a.max(b));
        let range = WalRange::new(WalEpoch::new(epoch), WalPos::new(first), WalPos::new(last)).unwrap();
        let id = BatchId::from_range(range);
        prop_assert_eq!(id.to_string().parse::<BatchId>(), Ok(id));
    }

    #[test]
    fn wal_range_extension_covers_both(
        epoch in any::<u64>(),
        a in 0u64..1000, alen in 0u64..50, gap in 1u64..50, blen in 0u64..50,
    ) {
        let e = WalEpoch::new(epoch);
        let r1 = WalRange::new(e, WalPos::new(a), WalPos::new(a + alen)).unwrap();
        let b = a + alen + gap;
        let r2 = WalRange::new(e, WalPos::new(b), WalPos::new(b + blen)).unwrap();
        let merged = r1.extended_by(&r2).unwrap();
        prop_assert_eq!(merged.first(), r1.first());
        prop_assert_eq!(merged.last(), r2.last());
        prop_assert!(r1.extended_by(&r1).is_none());
        prop_assert!(r2.extended_by(&r1).is_none());
        prop_assert!(merged.contains(r1.last()) && merged.contains(r2.first()));
    }

    #[test]
    fn asset_path_validates_names(levels in proptest::collection::vec(("[a-c]{0,2}", "[a-z]{0,4}"), 0..6)) {
        let result = AssetPath::new(levels.clone());
        let has_empty = levels.iter().any(|(k, _)| k.is_empty());
        let has_dup = levels.iter().enumerate().any(|(i, (k, _))| levels[..i].iter().any(|(e, _)| e == k));
        match result {
            Ok(path) => {
                prop_assert!(!has_empty && !has_dup);
                let got: Vec<(String, String)> = path.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
                prop_assert_eq!(got, levels);
            }
            Err(AssetPathError::EmptyLevelName) => prop_assert!(has_empty),
            Err(AssetPathError::DuplicateLevel(_)) => prop_assert!(has_dup),
            Err(other) => prop_assert!(false, "unexpected {other}"),
        }
    }

    #[test]
    fn source_time_is_set_once_and_flags_only_grow(
        ops in proptest::collection::vec((any::<bool>(), any::<i64>(), 0u32..256), 0..20),
    ) {
        let mut times = Times::new(Timestamp::from_nanos(1), TimeDomain::Utc);
        let mut quality = Quality::default();
        let mut first_source: Option<i64> = None;
        let mut expect_flags = QualityFlags::empty();
        for (is_source, ts, bits) in ops {
            let ts = Timestamp::from_nanos(ts);
            if is_source {
                let r = times.set_source(ts);
                prop_assert_eq!(r.is_ok(), first_source.is_none());
                first_source.get_or_insert(ts.as_nanos());
            } else {
                let flag = QualityFlags::from_bits_truncate(bits);
                let r = times.correct(ts, &mut quality, flag);
                prop_assert_eq!(r.is_ok(), !flag.is_empty());
                if r.is_ok() {
                    expect_flags |= flag;
                    prop_assert_eq!(times.corrected(), Some(ts));
                }
            }
            prop_assert_eq!(times.source().map(Timestamp::as_nanos), first_source);
            prop_assert_eq!(times.ingest(), Timestamp::from_nanos(1));
            prop_assert_eq!(quality.flags, expect_flags);
            prop_assert_eq!(times.corrected().is_some(), !expect_flags.is_empty());
        }
    }

    #[test]
    fn event_builder_enforces_invariants(
        with_payload in any::<bool>(),
        correct in any::<bool>(),
        quality_bits in 0u32..256,
        source in proptest::option::of(any::<i64>()),
    ) {
        let mut times = Times::new(Timestamp::from_nanos(10), TimeDomain::Utc);
        if let Some(s) = source {
            times.set_source(Timestamp::from_nanos(s)).unwrap();
        }
        if correct {
            let mut scratch = Quality::default();
            times.correct(Timestamp::from_nanos(3), &mut scratch, QualityFlags::CLOCK_SKEW).unwrap();
        }
        let quality = Quality { flags: QualityFlags::from_bits_truncate(quality_bits), ..Quality::default() };
        let flagged = !quality.flags.is_empty();
        let mut b = Event::builder(EventId::from_u128(1), WalPos::new(1), times).quality(quality);
        if with_payload {
            b = b.payload(Payload::raw("x/y", bytes::Bytes::new()));
        }
        match b.build() {
            Ok(e) => {
                prop_assert!(with_payload && (!correct || flagged));
                prop_assert_eq!(e.times().source().map(Timestamp::as_nanos), source);
                prop_assert_eq!(e.times().ingest(), Timestamp::from_nanos(10));
            }
            Err(EventBuildError::MissingPayload) => prop_assert!(!with_payload),
            Err(EventBuildError::CorrectionWithoutFlag) => prop_assert!(with_payload && correct && !flagged),
            Err(other) => prop_assert!(false, "unexpected {other}"),
        }
    }

    #[test]
    fn shape_registry_never_exceeds_its_cap(cap in 1usize..20, names in proptest::collection::vec("[a-e]{1,3}", 0..60)) {
        let mut reg = ShapeRegistry::new(cap);
        let mut distinct = std::collections::HashSet::new();
        for n in &names {
            let shape = Shape::new(vec![FieldDef::new(n.as_str(), FieldType::F64)]).unwrap();
            let was_known = distinct.contains(n);
            let r = reg.intern(shape);
            prop_assert!(reg.len() <= cap);
            if was_known {
                prop_assert!(r.is_ok());
            } else if r.is_ok() {
                distinct.insert(n.clone());
            }
        }
        prop_assert_eq!(reg.len(), distinct.len());
        prop_assert_eq!(reg.rejected() as usize, names.iter().filter(|n| !distinct.contains(*n)).count());
    }

    #[test]
    fn record_positional_and_named_access_agree(
        fields in proptest::collection::vec(("[a-z]{1,6}", 0u8..4), 1..12),
        seed in any::<u64>(),
    ) {
        let mut seen = std::collections::HashSet::new();
        let fields: Vec<_> = fields.into_iter().filter(|(n, _)| seen.insert(n.clone())).collect();
        let defs: Vec<FieldDef> = fields.iter().map(|(n, t)| FieldDef::new(n.as_str(), match t {
            0 => FieldType::I64,
            1 => FieldType::F32,
            2 => FieldType::Str,
            _ => FieldType::List(Box::new(FieldType::F64)),
        })).collect();
        let shape = ShapeRef::new(Shape::new(defs).unwrap());
        let values: Vec<Value> = fields.iter().enumerate().map(|(i, (_, t))| {
            let n = seed.wrapping_add(i as u64);
            match t {
                0 => Value::I64(n as i64),
                1 => Value::F32(n as f32),
                2 => Value::str(n.to_string()),
                _ => Value::from(vec![n as f64; (n % 4) as usize]),
            }
        }).collect();
        let record = Record::new(shape.clone(), values.clone()).unwrap();
        prop_assert_eq!(record.validate(), Ok(()));
        for (i, (name, _)) in fields.iter().enumerate() {
            let idx = shape.index_of(name).unwrap();
            prop_assert_eq!(idx.index(), i);
            prop_assert_eq!(record.get_at(idx), &values[i]);
            prop_assert_eq!(record.get(name), Some(&values[i]));
        }
        let iterated: Vec<&Value> = record.iter().map(|(_, v)| v).collect();
        prop_assert_eq!(iterated, values.iter().collect::<Vec<_>>());
        prop_assert_eq!(record.clone().into_values(), values);
    }
}

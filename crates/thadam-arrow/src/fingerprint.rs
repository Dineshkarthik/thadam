//! The schema fingerprint: SHA-256 over a canonical, versioned encoding of the
//! output columns, truncated to 128 bits.
//!
//! The encoding is written here rather than taken from Arrow's `Debug` or
//! `Display`, so an Arrow upgrade cannot change a fingerprint that is already
//! in files. Type tags below are fixed once released.

use arrow_schema::{DataType, FieldRef, TimeUnit};
use sha2::{Digest, Sha256};

const DOMAIN: &[u8] = b"thadam-arrow/schema/v1";

struct Encoder(Sha256);

impl Encoder {
    fn byte(&mut self, b: u8) {
        self.0.update([b]);
    }

    fn u32(&mut self, v: u32) {
        self.0.update(v.to_le_bytes());
    }

    fn len(&mut self, n: usize) {
        self.u32(u32::try_from(n).unwrap_or(u32::MAX));
    }

    fn str(&mut self, s: &str) {
        self.len(s.len());
        self.0.update(s.as_bytes());
    }

    fn field(&mut self, f: &FieldRef) {
        self.str(f.name());
        self.byte(u8::from(f.is_nullable()));
        self.data_type(f.data_type());
        let mut meta: Vec<(&String, &String)> = f.metadata().iter().collect();
        meta.sort();
        self.len(meta.len());
        for (k, v) in meta {
            self.str(k);
            self.str(v);
        }
    }

    fn data_type(&mut self, dt: &DataType) {
        match dt {
            DataType::Boolean => self.byte(1),
            DataType::Int8 => self.byte(2),
            DataType::Int16 => self.byte(3),
            DataType::Int32 => self.byte(4),
            DataType::Int64 => self.byte(5),
            DataType::UInt8 => self.byte(6),
            DataType::UInt16 => self.byte(7),
            DataType::UInt32 => self.byte(8),
            DataType::UInt64 => self.byte(9),
            DataType::Float32 => self.byte(10),
            DataType::Float64 => self.byte(11),
            DataType::Utf8 => self.byte(12),
            DataType::Binary => self.byte(13),
            DataType::FixedSizeBinary(n) => {
                self.byte(14);
                self.u32(u32::try_from(*n).unwrap_or(0));
            }
            DataType::Timestamp(unit, tz) => {
                self.byte(15);
                self.byte(match unit {
                    TimeUnit::Second => 0,
                    TimeUnit::Millisecond => 1,
                    TimeUnit::Microsecond => 2,
                    TimeUnit::Nanosecond => 3,
                });
                self.str(tz.as_deref().unwrap_or(""));
            }
            DataType::Dictionary(key, value) => {
                self.byte(16);
                self.data_type(key);
                self.data_type(value);
            }
            DataType::List(item) => {
                self.byte(17);
                self.field(item);
            }
            DataType::Struct(fields) => {
                self.byte(18);
                self.len(fields.len());
                fields.iter().for_each(|f| self.field(f));
            }
            // The schema builder never produces anything else; fingerprint the
            // type's name so an unexpected type still changes the result.
            other => {
                self.byte(0xff);
                self.str(&other.to_string());
            }
        }
    }
}

pub(crate) fn of_fields(fields: &[FieldRef]) -> [u8; 16] {
    let mut enc = Encoder(Sha256::new());
    enc.0.update(DOMAIN);
    enc.len(fields.len());
    fields.iter().for_each(|f| enc.field(f));
    let digest = enc.0.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&digest[..16]);
    out
}

pub(crate) fn to_hex(fp: &[u8; 16]) -> String {
    fp.iter().map(|b| format!("{b:02x}")).collect()
}

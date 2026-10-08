//! Stable error codes.
//!
//! A code is `<domain>.<snake_case_name>`. Codes are never reused for a
//! different meaning; new ones may be added. [`ALL`] lists every code and is
//! snapshot-tested.

use crate::error::ErrorDomain;

macro_rules! codes {
    ($($domain:ident { $($(#[$doc:meta])* $name:ident = $value:literal,)* })*) => {
        $($($(#[$doc])* pub const $name: &str = $value;)*)*

        /// Every code with its domain, grouped by domain in declaration order.
        pub const ALL: &[(ErrorDomain, &str)] = &[
            $($((ErrorDomain::$domain, $name),)*)*
        ];
    };
}

codes! {
    Connector {
        /// Opening the connection failed.
        CONNECTOR_CONNECT_FAILED = "connector.connect_failed",
        /// An established connection dropped.
        CONNECTOR_DISCONNECTED = "connector.disconnected",
        /// The peer violated the protocol.
        CONNECTOR_PROTOCOL = "connector.protocol",
        /// A protocol operation timed out.
        CONNECTOR_TIMEOUT = "connector.timeout",
        /// The broker lost the session, so unacknowledged messages were redelivered.
        CONNECTOR_SESSION_LOST = "connector.session_lost",
        /// An acknowledgement belonged to an older connection epoch and was dropped.
        CONNECTOR_STALE_ACK = "connector.stale_ack",
        /// A message exceeded the configured size limit.
        CONNECTOR_MESSAGE_TOO_LARGE = "connector.message_too_large",
        /// The connector task panicked.
        CONNECTOR_PANIC = "connector.panic",
        /// The operation was cancelled by shutdown.
        CONNECTOR_CANCELLED = "connector.cancelled",
    }
    Decode {
        /// The payload could not be parsed.
        DECODE_INVALID_PAYLOAD = "decode.invalid_payload",
        /// The payload is not valid UTF-8 where text is required.
        DECODE_INVALID_UTF8 = "decode.invalid_utf8",
        /// The payload uses an encoding with no decoder.
        DECODE_UNSUPPORTED_ENCODING = "decode.unsupported_encoding",
        /// The payload exceeds the size limit.
        DECODE_TOO_LARGE = "decode.too_large",
        /// The payload nests deeper than the limit.
        DECODE_DEPTH_EXCEEDED = "decode.depth_exceeded",
        /// A timestamp could not be parsed.
        DECODE_INVALID_TIME = "decode.invalid_time",
    }
    Schema {
        /// A value does not match its declared field type.
        SCHEMA_TYPE_MISMATCH = "schema.type_mismatch",
        /// A required field is missing.
        SCHEMA_MISSING_FIELD = "schema.missing_field",
        /// The shape registry reached `max_shapes`.
        SCHEMA_SHAPE_LIMIT = "schema.shape_limit",
        /// A conversion would narrow a type.
        SCHEMA_NARROWING = "schema.narrowing",
        /// A declared type is not supported.
        SCHEMA_UNSUPPORTED_TYPE = "schema.unsupported_type",
        /// A shape definition is invalid.
        SCHEMA_INVALID_SHAPE = "schema.invalid_shape",
    }
    Transform {
        /// A transform step failed on an event.
        TRANSFORM_FAILED = "transform.failed",
        /// A unit conversion is not defined.
        TRANSFORM_UNKNOWN_UNIT = "transform.unknown_unit",
        /// A filter expression could not be evaluated.
        TRANSFORM_EVAL_FAILED = "transform.eval_failed",
        /// A transform step panicked.
        TRANSFORM_PANIC = "transform.panic",
    }
    Quality {
        /// The event failed a quality rule and was rejected.
        QUALITY_REJECTED = "quality.rejected",
        /// A value is outside its declared range.
        QUALITY_OUT_OF_RANGE = "quality.out_of_range",
    }
    Buffer {
        /// The disk budget is exhausted.
        BUFFER_BUDGET_EXHAUSTED = "buffer.budget_exhausted",
        /// A frame or segment failed its checksum.
        BUFFER_CORRUPT = "buffer.corrupt",
        /// `fsync` failed; the segment can no longer be trusted.
        BUFFER_FSYNC_FAILED = "buffer.fsync_failed",
        /// The on-disk format is newer or otherwise unreadable.
        BUFFER_INCOMPATIBLE = "buffer.incompatible",
        /// A filesystem operation failed.
        BUFFER_IO = "buffer.io",
        /// Data was evicted under `drop_oldest`.
        BUFFER_EVICTED = "buffer.evicted",
    }
    Sink {
        /// Writing a batch failed.
        SINK_WRITE_FAILED = "sink.write_failed",
        /// The sink target did not answer in time.
        SINK_TIMEOUT = "sink.timeout",
        /// The sink target is unavailable.
        SINK_UNAVAILABLE = "sink.unavailable",
        /// The sink rejected an event.
        SINK_REJECTED = "sink.rejected",
        /// Recovery after a crash failed.
        SINK_RECOVER_FAILED = "sink.recover_failed",
        /// A local I/O operation failed.
        SINK_IO = "sink.io",
        /// A segment sink was given a batch not after its previous one.
        SINK_NON_MONOTONIC_APPEND = "sink.non_monotonic_append",
        /// Two different segments mapped to one object key (epoch or node-id
        /// misconfiguration; the shipper refuses to overwrite).
        SINK_OBJECT_KEY_COLLISION = "sink.object_key_collision",
        /// The sink task panicked.
        SINK_PANIC = "sink.panic",
    }
    Auth {
        /// Credentials were rejected.
        AUTH_FAILED = "auth.failed",
        /// Credentials expired.
        AUTH_EXPIRED = "auth.expired",
        /// The credentials lack permission.
        AUTH_FORBIDDEN = "auth.forbidden",
    }
    Config {
        /// A configuration value is invalid.
        CONFIG_INVALID = "config.invalid",
        /// A required configuration value is missing.
        CONFIG_MISSING = "config.missing",
        /// No connector is registered for the configured kind.
        CONFIG_UNKNOWN_KIND = "config.unknown_kind",
        /// A configuration or data-directory version is not supported.
        CONFIG_INCOMPATIBLE = "config.incompatible",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn codes_are_unique_and_prefixed_by_their_domain() {
        let mut seen = HashSet::new();
        for (domain, code) in ALL {
            let (prefix, name) = code.split_once('.').expect("code has a dot");
            assert_eq!(prefix, domain.as_str(), "{code}");
            assert!(
                !name.is_empty()
                    && name
                        .bytes()
                        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
                "{code}"
            );
            assert!(seen.insert(*code), "duplicate {code}");
        }
    }

    #[test]
    fn every_domain_has_codes() {
        for domain in ErrorDomain::ALL {
            assert!(ALL.iter().any(|(d, _)| *d == domain), "{domain}");
        }
    }

    #[test]
    fn code_registry_snapshot() {
        let text: String = ALL
            .iter()
            .map(|(domain, code)| format!("{domain}: {code}\n"))
            .collect();
        insta::assert_snapshot!(text);
    }
}

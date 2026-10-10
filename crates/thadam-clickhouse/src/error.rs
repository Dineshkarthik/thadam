//! Error classification (ADR-0008): a `clickhouse::error::Error` becomes a
//! `ThadamError` with the retry class the batch driver acts on.
//!
//! The client surfaces server-side failures as `Error::BadResponse` carrying
//! the server's text; the exception code is parsed out of it (`Code: N.` in
//! the body or in the `X-ClickHouse-Exception-Code` header the client turns
//! into the same shape, or a bare HTTP status line from a proxy). The code
//! tables below were checked against `src/Common/ErrorCodes.cpp` of ClickHouse
//! `v26.8.22.13-lts`, the server the Docker tests pin. Unknown codes default to
//! Transient: the driver bisects and quarantines after
//! `max_attempts_before_bisect` anyway, so an unrecognized code never retries
//! a poison batch forever, while a wrongly Permanent code would quarantine good
//! data.

use clickhouse::error::Error as ChError;
use thadam_core::{ErrorDomain, Retry, ThadamError, codes};

/// Client or input problems no retry can fix: the batch is poison and the
/// driver bisects it.
///
/// - 6 CANNOT_PARSE_TEXT, 26 CANNOT_PARSE_QUOTED_STRING,
///   27 CANNOT_PARSE_INPUT_ASSERTION_FAILED, 33 CANNOT_READ_ALL_DATA,
///   38 CANNOT_PARSE_DATE, 41 CANNOT_PARSE_DATETIME, 72 CANNOT_PARSE_NUMBER
/// - 53 TYPE_MISMATCH, 70 CANNOT_CONVERT_TYPE, 117 INCORRECT_DATA,
///   131 TOO_LARGE_STRING_SIZE, 321 VALUE_IS_OUT_OF_RANGE_OF_DATA_TYPE,
///   349 CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN
/// - 469 VIOLATED_CONSTRAINT (a table `CHECK` constraint the row breaks)
const PERMANENT_CODES: &[u32] = &[6, 26, 27, 33, 38, 41, 53, 70, 72, 117, 131, 321, 349, 469];

/// Server pressure, availability, or an operator-fixable table state: retry
/// with backoff while the events stay in the WAL.
///
/// - 159 TIMEOUT_EXCEEDED, 173 CANNOT_ALLOCATE_MEMORY,
///   241 MEMORY_LIMIT_EXCEEDED, 252 TOO_MANY_PARTS (long backoff is right),
///   202 TOO_MANY_SIMULTANEOUS_QUERIES, 203 NO_FREE_CONNECTION,
///   574 DISTRIBUTED_TOO_MANY_PENDING_BYTES, 745 SERVER_OVERLOADED
/// - 209 SOCKET_TIMEOUT, 210 NETWORK_ERROR, 279 ALL_CONNECTION_TRIES_FAILED
/// - 225 NO_ZOOKEEPER, 236 ABORTED, 242 TABLE_IS_READ_ONLY,
///   285 TOO_FEW_LIVE_REPLICAS, 369 ALL_REPLICAS_ARE_STALE, 473 DEADLOCK_AVOIDED,
///   999 KEEPER_EXCEPTION
/// - 319 UNKNOWN_STATUS_OF_INSERT (the insert may have landed; with a dedup
///   token the retry is applied once)
/// - 10 NOT_FOUND_COLUMN_IN_BLOCK, 16 NO_SUCH_COLUMN_IN_TABLE,
///   60 UNKNOWN_TABLE, 81 UNKNOWN_DATABASE: the table changed under a running
///   sink. Retrying keeps the data in the WAL until the operator restores the
///   table; quarantining every event would lose the ability to replay them.
const TRANSIENT_CODES: &[u32] = &[
    10, 16, 60, 81, 159, 173, 202, 203, 209, 210, 225, 236, 241, 242, 252, 279, 285, 319, 369, 473,
    574, 745, 999,
];

/// Credentials or permissions: retries cannot help and the operator must act.
///
/// - 192 UNKNOWN_USER, 193 WRONG_PASSWORD, 194 REQUIRED_PASSWORD,
///   516 AUTHENTICATION_FAILED
/// - 164 READONLY, 497 ACCESS_DENIED
const FATAL_CODES: &[u32] = &[164, 192, 193, 194, 497, 516];

/// Extracts `N` from `Code: N.` in a ClickHouse error text.
fn server_code(text: &str) -> Option<u32> {
    let rest = &text[text.find("Code:")? + "Code:".len()..];
    let digits: String = rest
        .trim_start()
        .chars()
        .take_while(char::is_ascii_digit)
        .collect();
    digits.parse().ok()
}

/// The HTTP status a bare status line (`401 Unauthorized`) starts with.
fn status_line(text: &str) -> Option<u16> {
    let token = text.split_whitespace().next()?;
    (token.len() == 3 && token.bytes().all(|b| b.is_ascii_digit()))
        .then(|| token.parse().ok())
        .flatten()
}

/// The retry class and server code for a server error text.
fn classify_server_text(text: &str) -> (Retry, Option<u32>) {
    if let Some(code) = server_code(text) {
        let retry = if PERMANENT_CODES.contains(&code) {
            Retry::Permanent
        } else if FATAL_CODES.contains(&code) {
            Retry::Fatal
        } else {
            // TRANSIENT_CODES and every unknown code.
            Retry::Transient
        };
        return (retry, Some(code));
    }
    match status_line(text) {
        Some(401 | 403) => (Retry::Fatal, None),
        _ => (Retry::Transient, None),
    }
}

fn with_server_text(e: ThadamError, code: Option<u32>, text: &str) -> ThadamError {
    let e = match code {
        Some(code) => e.with_context("clickhouse_code", code.to_string()),
        None => e,
    };
    // The server text is operator-visible and carries no secrets.
    e.with_context("server", text.chars().take(512).collect::<String>())
}

/// Classifies a client error for the write path.
///
/// Timeouts and dropped connections have an *unknown outcome* (the request
/// may have landed) and are Transient: the driver retries with the same
/// `BatchId`, which is the dedup token (ADR-0008).
pub(crate) fn classify(err: &ChError) -> ThadamError {
    let text = err.to_string();
    match err {
        ChError::TimedOut => ThadamError::transient(
            ErrorDomain::Sink,
            codes::SINK_TIMEOUT,
            "the ClickHouse statement timed out; the outcome is unknown, retrying with the \
             same batch id",
        )
        .with_context("server", text),
        ChError::Network(_)
        | ChError::Compression(_)
        | ChError::Decompression(_)
        | ChError::Other(_) => ThadamError::transient(
            ErrorDomain::Sink,
            codes::SINK_UNAVAILABLE,
            "the ClickHouse connection failed; the outcome is unknown, retrying with the \
             same batch id",
        )
        .with_context("server", text),
        ChError::BadResponse(body) => {
            let (retry, code) = classify_server_text(body);
            let e = match retry {
                Retry::Permanent => ThadamError::permanent(
                    ErrorDomain::Sink,
                    codes::SINK_REJECTED,
                    "ClickHouse rejected the batch: a value does not fit the table",
                ),
                Retry::Fatal => ThadamError::fatal(
                    ErrorDomain::Auth,
                    codes::AUTH_FAILED,
                    "ClickHouse refused the credentials or the permissions of the sink user",
                ),
                _ => ThadamError::transient(
                    ErrorDomain::Sink,
                    codes::SINK_UNAVAILABLE,
                    "ClickHouse reported server pressure or an unavailable table",
                ),
            };
            // Flag codes this table has never classified, so an operator can
            // tell a known pressure error from a retry-by-default.
            let e = match code {
                Some(c) if retry == Retry::Transient && !TRANSIENT_CODES.contains(&c) => {
                    e.with_context("unclassified_code", "true")
                }
                _ => e,
            };
            if retry == Retry::Permanent {
                // Data errors quote the offending values ("Column values: ..."),
                // and error context must never carry payloads: keep the code.
                return match code {
                    Some(c) => e.with_context("clickhouse_code", c.to_string()),
                    None => e,
                };
            }
            with_server_text(e, code, body)
        }
        ChError::InvalidParams(_) | ChError::InvalidColumnsHeader(_) | ChError::Unsupported(_) => {
            ThadamError::fatal(
                ErrorDomain::Config,
                codes::CONFIG_INVALID,
                "the ClickHouse client refused the request configuration",
            )
            .with_context("server", text)
        }
        // The client could not (de)serialize what the plan built or what the
        // server answered: a gap between this sink and the table, not a
        // transient condition.
        ChError::DataFormat(_)
        | ChError::SchemaMismatch(_)
        | ChError::NotEnoughData
        | ChError::InvalidUtf8Encoding(_)
        | ChError::Custom(_) => ThadamError::permanent(
            ErrorDomain::Schema,
            codes::SCHEMA_TYPE_MISMATCH,
            "the ClickHouse client could not (de)serialize the batch or the response",
        )
        .with_context("server", text),
        _ => ThadamError::transient(
            ErrorDomain::Sink,
            codes::SINK_WRITE_FAILED,
            "unclassified ClickHouse client error",
        )
        .with_context("server", text),
    }
}

/// Classifies for the probe (`open`) path: everything [`classify`] says,
/// except that a Permanent server answer becomes a Config error (the no-DDL
/// contract: a table or database that does not exist must be fixed by the
/// operator, and the message carries the server's own explanation).
///
/// Fatal stays Fatal. Transient stays Transient: the runtime retries `open`
/// until the server comes back.
pub(crate) fn classify_at_open(err: &ChError) -> ThadamError {
    let e = classify(err);
    if let ChError::BadResponse(body) = err {
        let (_, code) = classify_server_text(body);
        let missing = matches!(code, Some(60 | 81));
        if missing || (e.retry == Retry::Permanent && e.domain == ErrorDomain::Sink) {
            return with_server_text(
                ThadamError::permanent(
                    ErrorDomain::Config,
                    codes::CONFIG_INVALID,
                    format!(
                        "the ClickHouse server refused a startup probe (Thadam never creates \
                         tables): {}",
                        body.chars().take(256).collect::<String>()
                    ),
                ),
                code,
                body,
            );
        }
    }
    e
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn body(code: u32, what: &str) -> String {
        format!(
            "Code: {code}. DB::Exception: {what}. (SOME_NAME) (version 26.8.22.13 (official build))"
        )
    }

    #[test]
    fn parse_type_and_constraint_errors_are_permanent() {
        for (code, msg) in [
            (6, "Cannot parse string 'x' as UInt8"),
            (53, "Type mismatch in IN or VALUES section"),
            (117, "Incorrect data"),
            (131, "Too large string size"),
            (321, "Value 7 is out of range of type Date"),
            (
                349,
                "Cannot insert NULL value into a column of type 'String'",
            ),
            (
                469,
                "Constraint `temp_sane` for table default.imu is violated",
            ),
        ] {
            let (retry, found) = classify_server_text(&body(code, msg));
            assert_eq!(retry, Retry::Permanent, "code {code}");
            assert_eq!(found, Some(code));
        }
    }

    #[test]
    fn memory_parts_and_query_limits_are_transient() {
        for (code, msg) in [
            (241, "Memory limit (for query) exceeded"),
            (252, "Too many parts (300)"),
            (202, "Too many simultaneous queries"),
            (159, "Timeout exceeded"),
            (319, "Unknown status of insert"),
            (60, "Table default.imu does not exist"),
            (16, "No such column in table"),
        ] {
            let (retry, found) = classify_server_text(&body(code, msg));
            assert_eq!(retry, Retry::Transient, "code {code}");
            assert_eq!(found, Some(code));
        }
    }

    #[test]
    fn authentication_and_permission_errors_are_fatal() {
        for (code, msg) in [
            (192, "Unknown user writer"),
            (193, "Wrong password for user writer"),
            (194, "Password required for user writer"),
            (516, "writer: Authentication failed"),
            (497, "Not enough privileges"),
            (164, "Cannot insert in readonly mode"),
        ] {
            let (retry, found) = classify_server_text(&body(code, msg));
            assert_eq!(retry, Retry::Fatal, "code {code}");
            assert_eq!(found, Some(code));
        }
    }

    #[test]
    fn unknown_codes_default_to_transient() {
        let (retry, found) = classify_server_text(&body(4999, "a future error"));
        assert_eq!(retry, Retry::Transient);
        assert_eq!(found, Some(4999));
    }

    #[test]
    fn code_sets_are_disjoint() {
        for code in PERMANENT_CODES {
            assert!(!TRANSIENT_CODES.contains(code), "{code}");
            assert!(!FATAL_CODES.contains(code), "{code}");
        }
        for code in FATAL_CODES {
            assert!(!TRANSIENT_CODES.contains(code), "{code}");
        }
    }

    #[test]
    fn bare_status_lines_from_proxies() {
        assert_eq!(classify_server_text("401 Unauthorized").0, Retry::Fatal);
        assert_eq!(classify_server_text("403 Forbidden").0, Retry::Fatal);
        assert_eq!(classify_server_text("404 Not Found").0, Retry::Transient);
        assert_eq!(
            classify_server_text("503 Service Unavailable").0,
            Retry::Transient
        );
        assert_eq!(
            classify_server_text("something entirely odd").0,
            Retry::Transient
        );
        // A status-looking number elsewhere in the text is not a status line.
        assert_eq!(
            classify_server_text("connection to 10.0.0.401 reset").0,
            Retry::Transient
        );
    }

    #[test]
    fn client_errors_map_to_retry_classes() {
        let timed_out = classify(&ChError::TimedOut);
        assert_eq!(timed_out.retry, Retry::Transient);
        assert_eq!(timed_out.code, codes::SINK_TIMEOUT);

        let net = classify(&ChError::Network("connection refused".into()));
        assert_eq!(net.retry, Retry::Transient);

        let poison = classify(&ChError::BadResponse(body(469, "Constraint violated")));
        assert_eq!(poison.retry, Retry::Permanent);
        assert_eq!(poison.domain, ErrorDomain::Sink);

        let auth = classify(&ChError::BadResponse(body(516, "Authentication failed")));
        assert_eq!(auth.retry, Retry::Fatal);
        assert_eq!(auth.domain, ErrorDomain::Auth);

        let memory = classify(&ChError::BadResponse(body(241, "Memory limit exceeded")));
        assert_eq!(memory.retry, Retry::Transient);

        let format = classify(&ChError::Custom("boom".into()));
        assert_eq!(format.retry, Retry::Permanent);
    }

    #[test]
    fn data_errors_keep_the_code_but_never_the_quoted_row() {
        let text = body(
            469,
            "Constraint `c` is violated at row 4. Column values: temp = -300",
        );
        let e = classify(&ChError::BadResponse(text));
        assert_eq!(e.context.get("clickhouse_code"), Some("469"));
        assert_eq!(
            e.context.get("server"),
            None,
            "payload text must not be kept"
        );
        assert!(!format!("{e:?}").contains("-300"), "{e:?}");

        let memory = classify(&ChError::BadResponse(body(241, "Memory limit exceeded")));
        assert!(
            memory.context.get("server").is_some(),
            "non-data errors keep the text"
        );
    }

    #[test]
    fn open_turns_missing_tables_into_config_errors() {
        let missing = classify_at_open(&ChError::BadResponse(body(
            60,
            "Table default.imu does not exist",
        )));
        assert_eq!(missing.retry, Retry::Permanent);
        assert_eq!(missing.domain, ErrorDomain::Config);
        assert!(missing.message.contains("does not exist"), "{missing}");

        let down = classify_at_open(&ChError::Network("refused".into()));
        assert_eq!(down.retry, Retry::Transient, "an outage at open is retried");

        let auth = classify_at_open(&ChError::BadResponse(body(516, "Authentication failed")));
        assert_eq!(auth.retry, Retry::Fatal);
    }
}

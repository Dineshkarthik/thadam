//! The startup probe and the delivery-class decision (ADR-0008).
//!
//! ClickHouse deduplicates an insert by `insert_deduplication_token` only when
//! the table keeps a deduplication window: `Replicated*MergeTree` tables with
//! `replicated_deduplication_window > 0` (the default is nonzero), or plain
//! `MergeTree` tables with `non_replicated_deduplication_window > 0` (the
//! default is 0, which disables it). `open()` reads the engine, the table's own
//! settings and the server defaults, and reports the class it actually found;
//! Thadam does not assume effectively-once.

use thadam_core::{DeliveryClass, ErrorDomain, ThadamError, codes};

use crate::config::Dedup;

/// The server setting that enables dedup on non-replicated `MergeTree`.
pub(crate) const NON_REPLICATED_WINDOW: &str = "non_replicated_deduplication_window";
/// The server setting that bounds dedup on `Replicated*MergeTree`.
pub(crate) const REPLICATED_WINDOW: &str = "replicated_deduplication_window";

/// What the probe queries found.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct DedupFacts {
    /// The `engine` column of `system.tables` (for example `MergeTree` or
    /// `ReplicatedMergeTree`); `None` when it could not be read.
    pub(crate) engine: Option<String>,
    /// The `engine_full` definition, which carries the table's `SETTINGS`.
    pub(crate) engine_full: String,
    /// The effective server default of [`NON_REPLICATED_WINDOW`] from
    /// `system.merge_tree_settings`; `None` when it could not be read.
    pub(crate) default_non_replicated_window: Option<u64>,
    /// The effective server default of [`REPLICATED_WINDOW`]; `None` when it
    /// could not be read.
    pub(crate) default_replicated_window: Option<u64>,
}

fn is_ident(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

/// The integer a `SETTINGS key = N` clause assigns, matching `key` as a whole
/// identifier (`replicated_deduplication_window` must not match inside
/// `non_replicated_deduplication_window`).
pub(crate) fn setting_from(engine_full: &str, key: &str) -> Option<u64> {
    let mut from = 0;
    while let Some(i) = engine_full[from..].find(key) {
        let at = from + i;
        let end = at + key.len();
        let boundary_before = engine_full[..at]
            .chars()
            .next_back()
            .is_none_or(|c| !is_ident(c));
        let boundary_after = engine_full[end..]
            .chars()
            .next()
            .is_none_or(|c| !is_ident(c));
        if boundary_before && boundary_after {
            let value: String = engine_full[end..]
                .trim_start()
                .strip_prefix('=')?
                .trim_start()
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            return value.parse().ok();
        }
        from = end;
    }
    None
}

/// Whether the facts support insert deduplication: `Some(true)` it does,
/// `Some(false)` it does not, `None` the probe could not tell.
fn supported(facts: &DedupFacts) -> Option<bool> {
    let engine = facts.engine.as_deref()?;
    if engine.starts_with("Replicated") {
        // The window defaults to a nonzero value; only an explicit zero (table
        // or server) turns dedup off.
        let window =
            setting_from(&facts.engine_full, REPLICATED_WINDOW).or(facts.default_replicated_window);
        return Some(window.is_none_or(|w| w > 0));
    }
    if engine.contains("MergeTree") {
        let window = setting_from(&facts.engine_full, NON_REPLICATED_WINDOW)
            .or(facts.default_non_replicated_window)?;
        return Some(window > 0);
    }
    // Memory, Log, Distributed, Null and friends never deduplicate inserts.
    Some(false)
}

/// The outcome of the decision: whether to send the token, and the delivery
/// class to report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Delivery {
    /// Send `insert_deduplication_token` with every insert.
    pub(crate) send_token: bool,
    /// The class `open()` reports.
    pub(crate) class: DeliveryClass,
}

impl Delivery {
    pub(crate) const AT_LEAST_ONCE: Delivery = Delivery {
        send_token: false,
        class: DeliveryClass::AtLeastOnce,
    };
    const IDEMPOTENT: Delivery = Delivery {
        send_token: true,
        class: DeliveryClass::IdempotentBatch,
    };
}

/// Applies the `dedup:` mode to the probe facts.
pub(crate) fn decide(mode: Dedup, facts: &DedupFacts) -> Result<Delivery, ThadamError> {
    let config = |msg: &str| {
        ThadamError::permanent(ErrorDomain::Config, codes::CONFIG_INVALID, msg.to_owned())
            .with_context("path", "dedup")
    };
    match mode {
        Dedup::Auto => Ok(match supported(facts) {
            Some(true) => Delivery::IDEMPOTENT,
            Some(false) | None => Delivery::AT_LEAST_ONCE,
        }),
        Dedup::Token => match supported(facts) {
            Some(true) => Ok(Delivery::IDEMPOTENT),
            Some(false) => Err(config(
                "`dedup: token` was configured, but the target table has no deduplication \
                 window: the engine is not Replicated* and `non_replicated_deduplication_window` \
                 is 0. Set the window on the table, or use `dedup: auto`",
            )),
            None => Err(config(
                "`dedup: token` was configured, but the dedup probe could not read the table \
                 engine or the window settings (permissions on system.tables and \
                 system.merge_tree_settings?); the guarantee cannot be confirmed, so the sink \
                 refuses to start",
            )),
        },
        Dedup::Off => Ok(Delivery::AT_LEAST_ONCE),
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    fn facts(
        engine: Option<&str>,
        engine_full: &str,
        non_replicated: Option<u64>,
        replicated: Option<u64>,
    ) -> DedupFacts {
        DedupFacts {
            engine: engine.map(str::to_owned),
            engine_full: engine_full.to_owned(),
            default_non_replicated_window: non_replicated,
            default_replicated_window: replicated,
        }
    }

    fn auto(f: &DedupFacts) -> Delivery {
        decide(Dedup::Auto, f).expect("auto never fails")
    }

    #[test]
    fn replicated_engines_support_dedup() {
        for e in [
            "ReplicatedMergeTree",
            "ReplicatedReplacingMergeTree",
            "ReplicatedSummingMergeTree",
        ] {
            let d = auto(&facts(Some(e), "", Some(0), Some(1000)));
            assert!(d.send_token, "{e}");
            assert_eq!(d.class, DeliveryClass::IdempotentBatch);
        }
        // Unreadable server defaults: the engine alone decides.
        assert!(auto(&facts(Some("ReplicatedMergeTree"), "", None, None)).send_token);
    }

    #[test]
    fn a_replicated_window_of_zero_turns_dedup_off() {
        let by_table = "ReplicatedMergeTree('/p', 'r') ORDER BY x SETTINGS replicated_deduplication_window = 0";
        assert_eq!(
            auto(&facts(
                Some("ReplicatedMergeTree"),
                by_table,
                None,
                Some(1000)
            ))
            .class,
            DeliveryClass::AtLeastOnce
        );
        assert_eq!(
            auto(&facts(Some("ReplicatedMergeTree"), "", None, Some(0))).class,
            DeliveryClass::AtLeastOnce
        );
    }

    #[test]
    fn the_table_window_enables_dedup_on_plain_merge_tree() {
        let full = "MergeTree PARTITION BY toYYYYMM(_ingest_time) ORDER BY (robot) \
                    SETTINGS non_replicated_deduplication_window = 1000, index_granularity = 8192";
        let d = auto(&facts(Some("MergeTree"), full, Some(0), Some(1000)));
        assert!(d.send_token);
        assert_eq!(d.class, DeliveryClass::IdempotentBatch);
    }

    #[test]
    fn a_server_default_window_applies_to_tables_without_one() {
        let d = auto(&facts(
            Some("MergeTree"),
            "MergeTree ORDER BY x",
            Some(500),
            None,
        ));
        assert!(d.send_token);
        assert_eq!(d.class, DeliveryClass::IdempotentBatch);
    }

    #[test]
    fn no_window_means_at_least_once() {
        let d = auto(&facts(
            Some("MergeTree"),
            "MergeTree ORDER BY x",
            Some(0),
            Some(1000),
        ));
        assert!(!d.send_token);
        assert_eq!(d.class, DeliveryClass::AtLeastOnce);
        // An explicit zero on the table beats a nonzero server default.
        let zero = "MergeTree ORDER BY x SETTINGS non_replicated_deduplication_window = 0";
        assert_eq!(
            auto(&facts(Some("MergeTree"), zero, Some(100), None)).class,
            DeliveryClass::AtLeastOnce
        );
    }

    #[test]
    fn engines_without_dedup_never_claim_it() {
        for e in ["Memory", "Log", "Distributed", "Null"] {
            assert_eq!(
                auto(&facts(Some(e), "", Some(1000), Some(1000))).class,
                DeliveryClass::AtLeastOnce,
                "{e}"
            );
        }
    }

    #[test]
    fn unreadable_probes_degrade_honestly_in_auto() {
        for f in [
            facts(None, "", None, None),
            facts(Some("MergeTree"), "MergeTree ORDER BY x", None, None),
        ] {
            let d = auto(&f);
            assert!(!d.send_token);
            assert_eq!(d.class, DeliveryClass::AtLeastOnce);
        }
    }

    #[test]
    fn token_mode_requires_confirmed_support() {
        let ok = decide(
            Dedup::Token,
            &facts(Some("ReplicatedMergeTree"), "", None, Some(1000)),
        )
        .expect("replicated");
        assert_eq!(ok.class, DeliveryClass::IdempotentBatch);

        let err = decide(Dedup::Token, &facts(Some("MergeTree"), "", Some(0), None))
            .expect_err("no window");
        assert!(err.message.contains("no deduplication window"), "{err}");
        assert_eq!(err.domain, ErrorDomain::Config);

        let err = decide(Dedup::Token, &facts(None, "", None, None)).expect_err("unknown");
        assert!(err.message.contains("cannot be confirmed"), "{err}");
    }

    #[test]
    fn off_never_sends_and_never_claims() {
        let d = decide(
            Dedup::Off,
            &facts(Some("ReplicatedMergeTree"), "", Some(1), Some(1000)),
        )
        .expect("off");
        assert!(!d.send_token);
        assert_eq!(d.class, DeliveryClass::AtLeastOnce);
    }

    #[test]
    fn setting_parsing_matches_whole_identifiers_only() {
        assert_eq!(setting_from("", NON_REPLICATED_WINDOW), None);
        assert_eq!(
            setting_from(
                "SETTINGS non_replicated_deduplication_window=42",
                NON_REPLICATED_WINDOW
            ),
            Some(42)
        );
        assert_eq!(
            setting_from(
                "non_replicated_deduplication_window = 7, x = 1",
                NON_REPLICATED_WINDOW
            ),
            Some(7)
        );
        assert_eq!(
            setting_from(
                "SETTINGS non_replicated_deduplication_window = ",
                NON_REPLICATED_WINDOW
            ),
            None
        );
        // `replicated_...` is a suffix of `non_replicated_...`: must not match.
        assert_eq!(
            setting_from(
                "SETTINGS non_replicated_deduplication_window = 9",
                REPLICATED_WINDOW
            ),
            None
        );
        assert_eq!(
            setting_from(
                "SETTINGS non_replicated_deduplication_window = 9, replicated_deduplication_window = 3",
                REPLICATED_WINDOW
            ),
            Some(3)
        );
    }
}

//! `probe` subcommand: determine, empirically, which query selectors a raw
//! host-side zenoh client can use against rmw_zenoh graph entities. Drives
//! the late-join mapping for transient_local topics.
//!
//! Modes:
//!   default        run the selector matrix, 2.5 s per get
//!   --selftest     declare probe/self, sleep 1 s, ONE default-config get()
//!   --hold-secs N  declare probe/holder and keep it up N s (no gets), for
//!                  two-process routing experiments

use std::time::{Duration, Instant};

use crate::zenoh_impl::{ZenohTransport, describe_sample};

struct Candidate {
    label: &'static str,
    selector: String,
    target: zenoh::query::QueryTarget,
    consolidation: zenoh::query::ConsolidationMode,
}

pub async fn run(
    endpoint: &str,
    mode: &str,
    selftest: bool,
    hold_secs: u64,
) -> Result<String, String> {
    let transport = ZenohTransport::connect(endpoint, 0, 256, mode).await?;
    let session = transport.session();

    // Control: a queryable on this very session. A query to it does not
    // leave the process, so any reply proves the get/reply plumbing. In
    // --hold-secs mode the process declares probe/holder (distinct key, so
    // a peer process can tell remote and local replies apart).
    let control_ke = if hold_secs > 0 {
        "probe/holder"
    } else {
        "probe/self"
    };
    let control = session
        .declare_queryable(control_ke)
        .await
        .map_err(|e| format!("declare self queryable: {e}"))?;
    let reply_payload = control_ke.to_string();
    tokio::spawn(async move {
        while let Ok(query) = control.recv_async().await {
            let _ = query.reply(control_ke, reply_payload.clone()).await;
        }
    });

    if hold_secs > 0 {
        tokio::time::sleep(Duration::from_secs(hold_secs)).await;
        return Ok(String::new());
    }

    if selftest {
        tokio::time::sleep(Duration::from_secs(1)).await;
        let started = Instant::now();
        let mut ok = 0u32;
        let mut err = 0u32;
        let replies = session
            .get("probe/self")
            .await
            .map_err(|e| format!("selftest get: {e}"))?;
        while let Ok(reply) = replies.recv_async().await {
            match reply.into_result() {
                Ok(_) => ok += 1,
                Err(_) => err += 1,
            }
        }
        return serde_json::to_string_pretty(&serde_json::json!({
            "selftest": "default get() on locally declared probe/self after 1 s settle",
            "replies_ok": ok,
            "replies_err": err,
            "elapsed_ms": started.elapsed().as_millis(),
        }))
        .map_err(|e| format!("serialize selftest: {e}"));
    }

    // Let interest declarations settle before querying; a get() fired
    // before propagation finalizes instantly with zero replies.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let all = zenoh::query::QueryTarget::All;
    let candidates = vec![
        Candidate {
            label: "control local queryable probe/self (All, no conso)",
            selector: "probe/self".to_string(),
            target: all,
            consolidation: zenoh::query::ConsolidationMode::None,
        },
        Candidate {
            label: "remote holder queryable probe/holder (All, no conso)",
            selector: "probe/holder".to_string(),
            target: all,
            consolidation: zenoh::query::ConsolidationMode::None,
        },
        Candidate {
            label: "peer service queryable (get_type_description SS)",
            selector: "0/s3_sensor_pub/get_type_description/**".to_string(),
            target: all,
            consolidation: zenoh::query::ConsolidationMode::Auto,
        },
        Candidate {
            label: "tf_static data ke, AllComplete",
            selector: "0/tf_static/**".to_string(),
            target: zenoh::query::QueryTarget::AllComplete,
            consolidation: zenoh::query::ConsolidationMode::Auto,
        },
        Candidate {
            label: "tf_static data ke, All",
            selector: "0/tf_static/**".to_string(),
            target: all,
            consolidation: zenoh::query::ConsolidationMode::Auto,
        },
        Candidate {
            label: "tf_static @adv subtree, All",
            selector: "0/tf_static/**/@adv/**".to_string(),
            target: all,
            consolidation: zenoh::query::ConsolidationMode::Auto,
        },
        Candidate {
            label: "tf_static @adv subtree, All, no consolidation",
            selector: "0/tf_static/**/@adv/**".to_string(),
            target: all,
            consolidation: zenoh::query::ConsolidationMode::None,
        },
        Candidate {
            label: "tf_static exact-ke @adv subtree, All",
            selector: concat!(
                "0/tf_static/tf2_msgs::msg::dds_::TFMessage_/",
                "RIHS01_e369d0f05a23ae52508854b66f6aa0437f3449d652e8cbf22d5abe85d020f087",
                "/@adv/**"
            )
            .to_string(),
            target: all,
            consolidation: zenoh::query::ConsolidationMode::Auto,
        },
        Candidate {
            label: "whole graph @adv subtree, All",
            selector: "**/@adv/**".to_string(),
            target: all,
            consolidation: zenoh::query::ConsolidationMode::Auto,
        },
    ];

    let mut results = Vec::new();
    for cand in &candidates {
        let started = Instant::now();
        let mut ok = 0u32;
        let mut err = 0u32;
        let mut first_ok_ke: Option<String> = None;
        let mut first_err: Option<String> = None;
        let mut first_sample: Option<serde_json::Value> = None;
        let replies = session
            .get(cand.selector.as_str())
            .target(cand.target)
            .consolidation(cand.consolidation)
            .timeout(Duration::from_millis(2500))
            .await;
        let Ok(replies) = replies else {
            results.push(serde_json::json!({
                "label": cand.label,
                "selector": cand.selector,
                "error": "get() failed",
            }));
            continue;
        };
        while let Ok(reply) = replies.recv_async().await {
            match reply.into_result() {
                Ok(sample) => {
                    ok += 1;
                    if first_ok_ke.is_none() {
                        first_ok_ke = Some(sample.key_expr().as_str().to_string());
                        first_sample = Some(describe_sample(&sample));
                    }
                }
                Err(e) => {
                    err += 1;
                    if first_err.is_none() {
                        first_err = Some(format!(
                            "{:?}",
                            e.payload()
                                .try_to_string()
                                .unwrap_or_else(|_| "<binary>".into())
                        ));
                    }
                }
            }
        }
        results.push(serde_json::json!({
            "label": cand.label,
            "selector": cand.selector,
            "target": format!("{:?}", cand.target),
            "consolidation": format!("{:?}", cand.consolidation),
            "elapsed_ms": started.elapsed().as_millis(),
            "replies_ok": ok,
            "replies_err": err,
            "first_ok_ke": first_ok_ke,
            "first_err_payload": first_err,
            "first_sample": first_sample,
        }));
    }
    serde_json::to_string_pretty(&results).map_err(|e| format!("serialize probe: {e}"))
}

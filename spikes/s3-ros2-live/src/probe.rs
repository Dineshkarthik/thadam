//! `probe` subcommand: determine, empirically, which query selectors a raw
//! host-side zenoh client can use against rmw_zenoh graph entities (step 1)
//! or a zenoh-bridge-ros2dds admin space / graph (step 2, `--admin <zid>`).
//!
//! Modes:
//!   default          run the rmw_zenoh selector matrix, 2.5 s per get
//!   --admin <zid>    run the bridge admin/data selector matrix against the
//!                    bridge whose zenoh id is <zid>
//!   --selftest       declare probe/self, sleep 1 s, ONE default-config get()
//!   --hold-secs N    declare probe/holder and keep it up N s (no gets), for
//!                    two-process routing experiments

use std::time::{Duration, Instant};

use crate::zenoh_impl::{ZenohTransport, describe_sample};

struct Candidate {
    label: String,
    selector: String,
    target: zenoh::query::QueryTarget,
    consolidation: zenoh::query::ConsolidationMode,
}

fn cand(
    label: &str,
    selector: String,
    target: zenoh::query::QueryTarget,
    consolidation: zenoh::query::ConsolidationMode,
) -> Candidate {
    Candidate {
        label: label.to_string(),
        selector,
        target,
        consolidation,
    }
}

pub async fn run(
    endpoint: &str,
    mode: &str,
    selftest: bool,
    hold_secs: u64,
    admin: Option<String>,
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
    let none = zenoh::query::ConsolidationMode::None;
    let auto = zenoh::query::ConsolidationMode::Auto;
    let candidates: Vec<Candidate> =
        match &admin {
            Some(zid) => {
                vec![
                    cand(
                        "control local queryable probe/self",
                        "probe/self".into(),
                        all,
                        none,
                    ),
                    cand(
                        "bridge admin space root",
                        format!("@/{zid}/ros2"),
                        all,
                        none,
                    ),
                    cand(
                        "bridge admin space subtree",
                        format!("@/{zid}/ros2/**"),
                        all,
                        none,
                    ),
                    cand(
                        "bridge admin space subtree, auto consolidation",
                        format!("@/{zid}/ros2/**"),
                        all,
                        auto,
                    ),
                    cand(
                        "rmw-style liveliness prefix (control: expect 0)",
                        "@ros2_lv/0/**".into(),
                        all,
                        none,
                    ),
                    cand(
                        "tf_static data ke query (history queryable?)",
                        "tf_static".into(),
                        all,
                        none,
                    ),
                    cand(
                        "tf_static data ke wildcard query",
                        "tf_static/**".into(),
                        all,
                        none,
                    ),
                    cand(
                        "imu data ke query (live topic, expect no queryable)",
                        "imu".into(),
                        all,
                        none,
                    ),
                ]
            }
            None => {
                vec![
                cand("control local queryable probe/self", "probe/self".into(), all, none),
                cand(
                    "remote holder queryable probe/holder",
                    "probe/holder".into(),
                    all,
                    none,
                ),
                cand(
                    "peer service queryable (get_type_description SS)",
                    "0/s3_sensor_pub/get_type_description/**".into(),
                    all,
                    auto,
                ),
                cand(
                    "tf_static data ke, AllComplete",
                    "0/tf_static/**".into(),
                    zenoh::query::QueryTarget::AllComplete,
                    auto,
                ),
                cand("tf_static data ke, All", "0/tf_static/**".into(), all, auto),
                cand("tf_static @adv subtree, All", "0/tf_static/**/@adv/**".into(), all, auto),
                cand(
                    "tf_static @adv subtree, All, no consolidation",
                    "0/tf_static/**/@adv/**".into(),
                    all,
                    none,
                ),
                cand(
                    "tf_static exact-ke @adv subtree, All",
                    concat!(
                        "0/tf_static/tf2_msgs::msg::dds_::TFMessage_/",
                        "RIHS01_e369d0f05a23ae52508854b66f6aa0437f3449d652e8cbf22d5abe85d020f087",
                        "/@adv/**"
                    )
                    .into(),
                    all,
                    auto,
                ),
                cand("whole graph @adv subtree, All", "**/@adv/**".into(), all, auto),
            ]
            }
        };

    let mut results = Vec::new();
    for c in &candidates {
        let started = Instant::now();
        let mut ok = 0u32;
        let mut err = 0u32;
        let mut first_ok_ke: Option<String> = None;
        let mut first_err: Option<String> = None;
        let mut first_sample: Option<serde_json::Value> = None;
        let mut ok_kes: Vec<String> = Vec::new();
        let mut payloads_first3: Vec<serde_json::Value> = Vec::new();
        let replies = session
            .get(c.selector.as_str())
            .target(c.target)
            .consolidation(c.consolidation)
            .timeout(Duration::from_millis(2500))
            .await;
        let Ok(replies) = replies else {
            results.push(serde_json::json!({
                "label": c.label,
                "selector": c.selector,
                "error": "get() failed",
            }));
            continue;
        };
        while let Ok(reply) = replies.recv_async().await {
            match reply.into_result() {
                Ok(sample) => {
                    ok += 1;
                    let ke = sample.key_expr().as_str().to_string();
                    if ok_kes.len() < 40 {
                        ok_kes.push(ke.clone());
                    }
                    if payloads_first3.len() < 3 {
                        payloads_first3.push(serde_json::json!({
                            "ke": ke,
                            "payload_text": sample
                                .payload()
                                .try_to_string()
                                .map(|s| s.into_owned())
                                .ok(),
                        }));
                    }
                    if first_ok_ke.is_none() {
                        first_ok_ke = Some(ke);
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
            "label": c.label,
            "selector": c.selector,
            "target": format!("{:?}", c.target),
            "consolidation": format!("{:?}", c.consolidation),
            "elapsed_ms": started.elapsed().as_millis(),
            "replies_ok": ok,
            "replies_err": err,
            "first_ok_ke": first_ok_ke,
            "first_err_payload": first_err,
            "ok_kes_first40": ok_kes,
            "payloads_first3": payloads_first3,
            "first_sample": first_sample,
        }));
    }
    serde_json::to_string_pretty(&results).map_err(|e| format!("serialize probe: {e}"))
}

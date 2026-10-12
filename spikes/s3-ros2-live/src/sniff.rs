//! `sniff` subcommand: dump raw zenoh samples (key expressions, attachments,
//! payload heads) and rmw_zenoh liveliness tokens, as JSON lines. This is
//! the empirical-discovery tool the spike report's mapping section is built
//! from.

use std::time::Duration;

use crate::zenoh_impl::{ZenohTransport, describe_sample};

pub async fn run(
    endpoint: &str,
    domain: u32,
    duration_secs: u64,
    mode: &str,
) -> Result<(), String> {
    let transport = ZenohTransport::connect(endpoint, domain, 256, mode).await?;
    let session = transport.session();

    let data_sub = session
        .declare_subscriber("**")
        .await
        .map_err(|e| format!("declare data subscriber: {e}"))?;
    let lv_sub = session
        .liveliness()
        .declare_subscriber("@ros2_lv/**")
        .history(true)
        .await
        .map_err(|e| format!("declare liveliness subscriber: {e}"))?;

    let start = tokio::time::Instant::now();
    let deadline = start + Duration::from_secs(duration_secs);
    loop {
        let remaining = deadline
            .checked_duration_since(tokio::time::Instant::now())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            break;
        }
        tokio::select! {
            biased;
            _ = tokio::time::sleep(remaining) => break,
            r = data_sub.recv_async() => {
                if let Ok(sample) = r {
                    let v = describe_sample(&sample);
                    println!(
                        "{}",
                        serde_json::json!({
                            "stream": "data",
                            "t_ms": start.elapsed().as_millis(),
                            "event": v,
                        })
                    );
                }
            }
            r = lv_sub.recv_async() => {
                if let Ok(sample) = r {
                    let ke = sample.key_expr().as_str().to_string();
                    let entity = ZenohTransport::parse_liveliness_token(&ke);
                    println!(
                        "{}",
                        serde_json::json!({
                            "stream": "liveliness",
                            "t_ms": start.elapsed().as_millis(),
                            "kind": format!("{:?}", sample.kind()),
                            "ke": ke,
                            "entity": entity,
                        })
                    );
                }
            }
        }
    }
    Ok(())
}

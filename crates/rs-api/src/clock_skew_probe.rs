//! Per-delivery clock-skew probe: periodically calls VPS /clock and
//! persists RTT-compensated skew samples to clock_skew_samples.

use std::time::Duration;

use sqlx::SqlitePool;

/// Poll interval for clock-skew samples. Longer than the progress poll (10s)
/// because RTT-compensated skew changes slowly; back-to-back probes on a
/// missed tick would exaggerate skew variance without adding signal.
const SKEW_PROBE_INTERVAL_SECS: u64 = 30;

#[derive(Debug, Clone)]
pub struct ClockSkewSample {
    pub measured_at_ms: i64,
    pub local_before_ms: i64,
    pub vps_reported_ms: i64,
    pub local_after_ms: i64,
    pub skew_ms: i64,
    pub rtt_ms: i64,
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

/// Perform a single clock-skew probe against the VPS `/clock` endpoint.
/// Returns RTT-compensated skew: positive means VPS clock is ahead of stream.lan.
pub async fn probe_clock_skew(vps_base_url: &str) -> Result<ClockSkewSample, reqwest::Error> {
    #[derive(serde::Deserialize)]
    struct ClockResp {
        vps_ms: i64,
    }

    let local_before_ms = now_ms();
    let resp: ClockResp = reqwest::Client::new()
        .get(format!("{vps_base_url}/clock"))
        .timeout(Duration::from_secs(5))
        .send()
        .await?
        .json()
        .await?;
    let local_after_ms = now_ms();
    let vps_reported_ms = resp.vps_ms;
    let midpoint = (local_before_ms + local_after_ms) / 2;
    Ok(ClockSkewSample {
        measured_at_ms: local_after_ms,
        local_before_ms,
        vps_reported_ms,
        local_after_ms,
        skew_ms: vps_reported_ms - midpoint,
        rtt_ms: local_after_ms - local_before_ms,
    })
}

/// Background task: probe every 30 s, persist results.
/// Exits when `delivering_activated` is false/gone in the DB (mirrors the
/// pattern used by `monitor_delivery_health`).
pub fn spawn_skew_probe(pool: SqlitePool, event_id: i64, vps_base_url: String) {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(SKEW_PROBE_INTERVAL_SECS));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        tick.tick().await; // skip immediate first tick
        loop {
            tick.tick().await;

            // Stop if the event is no longer delivering.
            match rs_core::db::get_streaming_event_by_id(&pool, event_id).await {
                Ok(Some(evt)) if !evt.delivering_activated => {
                    tracing::info!(
                        event_id,
                        "Clock-skew probe stopping: event no longer delivering"
                    );
                    return;
                }
                Ok(None) => {
                    tracing::info!(event_id, "Clock-skew probe stopping: event deleted");
                    return;
                }
                Err(e) => {
                    tracing::warn!(event_id, "Clock-skew probe DB error: {e}");
                }
                _ => {}
            }

            match probe_clock_skew(&vps_base_url).await {
                Ok(s) => {
                    if let Err(e) = rs_core::db::drift::insert_clock_skew_sample(
                        &pool,
                        event_id,
                        s.measured_at_ms,
                        s.local_before_ms,
                        s.vps_reported_ms,
                        s.local_after_ms,
                        s.skew_ms,
                        s.rtt_ms,
                    )
                    .await
                    {
                        tracing::warn!(event_id, "Failed to persist clock skew sample: {e}");
                    } else {
                        tracing::debug!(
                            event_id,
                            skew_ms = s.skew_ms,
                            rtt_ms = s.rtt_ms,
                            "Clock skew sample stored"
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!(event_id, "Clock skew probe failed: {e}");
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn skew_probe_computes_rtt_compensated_skew() {
        let vps_reported_ms: i64 = 1_700_000_500_000;
        let app = axum::Router::new().route(
            "/clock",
            axum::routing::get(move || async move {
                axum::Json(serde_json::json!({ "vps_ms": vps_reported_ms }))
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let sample = probe_clock_skew(&format!("http://{addr}")).await.unwrap();
        let midpoint = (sample.local_before_ms + sample.local_after_ms) / 2;
        assert_eq!(sample.skew_ms, vps_reported_ms - midpoint);
        assert!(sample.rtt_ms >= 0);
    }
}

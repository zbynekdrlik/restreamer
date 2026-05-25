//! Coarse endpoint + delivery summary for `/api/v1/status` (#228).
//!
//! Extracted from `handlers.rs` to keep that file under the 1000-line CI cap.

use rs_core::models::{ComponentStatus, EndpointLifecycle};

use crate::state::CachedDeliveryStatus;

/// Derive the coarse `endpoint` + `delivery` summary for `/api/v1/status` from
/// the cached delivery status the broadcast loop maintains. Returns empty
/// states when idle (broadcast loop caches `status="none"` when not
/// delivering). When delivering, `delivery.state` mirrors the cached status and
/// `endpoint.state` is a health rollup of the per-endpoint lifecycles:
/// `attention` (red) > `recovering` (blue) > `degraded` (some not alive) >
/// `live` (all alive) > `starting` (no metrics yet). #228 fix: this replaces
/// the previous `Default` (empty) summary that painted a false RED after a
/// mid-event restart.
///
/// A `Pending` endpoint (configured placeholder, `alive=false`, not in the
/// recovering set) counts as not-alive, so a delivering event with any
/// not-yet-live endpoint rolls up to `degraded` — intentional: it is a calm
/// (non-red) "not fully up yet" signal, distinct from `attention`.
pub fn summarize_delivery(cached: &CachedDeliveryStatus) -> (ComponentStatus, ComponentStatus) {
    if cached.status.is_empty() || cached.status == "none" {
        return (ComponentStatus::default(), ComponentStatus::default());
    }

    let total = cached.endpoints.len();
    let alive = cached.endpoints.iter().filter(|e| e.alive).count();
    let any_attention = cached
        .endpoints
        .iter()
        .any(|e| matches!(e.lifecycle, EndpointLifecycle::Attention));
    let any_recovering = cached.endpoints.iter().any(|e| {
        matches!(
            e.lifecycle,
            EndpointLifecycle::Buffering
                | EndpointLifecycle::Rescue
                | EndpointLifecycle::Recovering
        )
    });

    let endpoint_state = if total == 0 {
        "starting"
    } else if any_attention {
        "attention"
    } else if any_recovering {
        "recovering"
    } else if alive == total {
        "live"
    } else {
        "degraded"
    };

    let endpoint = ComponentStatus {
        state: endpoint_state.to_string(),
        details: serde_json::json!({ "alive": alive, "total": total }),
    };
    let delivery = ComponentStatus {
        state: cached.status.clone(),
        details: serde_json::json!({
            "instance_name": cached.instance_name,
            "server_ip": cached.server_ip,
            "endpoint_count": cached.endpoint_count,
        }),
    };
    (endpoint, delivery)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rs_core::models::DeliveryEndpointMetrics;

    fn ep(alias: &str, alive: bool, lifecycle: EndpointLifecycle) -> DeliveryEndpointMetrics {
        DeliveryEndpointMetrics {
            alias: alias.to_string(),
            alive,
            current_chunk_id: 0,
            bytes_processed_total: 0,
            chunks_processed: 0,
            chunk_delay_secs: 0.0,
            stall_reason: None,
            ffmpeg_restart_count: 0,
            reconnect_count: 0,
            last_error: None,
            is_fast: false,
            delivery_mode: None,
            rescue_eta_secs: None,
            youtube_health: None,
            lifecycle,
        }
    }

    fn cached(status: &str, endpoints: Vec<DeliveryEndpointMetrics>) -> CachedDeliveryStatus {
        CachedDeliveryStatus {
            instance_name: "rs-delivery-test".into(),
            status: status.into(),
            server_ip: Some("10.0.0.5".into()),
            endpoint_count: endpoints.len() as u32,
            endpoints,
        }
    }

    #[test]
    fn idle_status_yields_empty_summary() {
        for s in ["", "none"] {
            let (e, d) = summarize_delivery(&cached(s, vec![]));
            assert_eq!(e.state, "", "idle endpoint.state must be empty for status={s:?}");
            assert_eq!(d.state, "", "idle delivery.state must be empty for status={s:?}");
        }
    }

    #[test]
    fn all_live_rolls_up_to_live() {
        let (e, d) = summarize_delivery(&cached(
            "delivering",
            vec![
                ep("YT", true, EndpointLifecycle::Live),
                ep("FB", true, EndpointLifecycle::Live),
            ],
        ));
        assert_eq!(e.state, "live");
        assert_eq!(d.state, "delivering");
    }

    #[test]
    fn delivering_with_no_endpoints_is_starting() {
        let (e, d) = summarize_delivery(&cached("delivering", vec![]));
        assert_eq!(e.state, "starting", "total==0 while delivering => starting");
        assert_eq!(d.state, "delivering");
    }

    #[test]
    fn any_recovering_rolls_up_to_recovering() {
        for lc in [
            EndpointLifecycle::Buffering,
            EndpointLifecycle::Rescue,
            EndpointLifecycle::Recovering,
        ] {
            let (e, _) = summarize_delivery(&cached(
                "delivering",
                vec![ep("YT", true, EndpointLifecycle::Live), ep("FB", true, lc)],
            ));
            assert_eq!(e.state, "recovering", "lifecycle {lc:?} must roll up to recovering");
        }
    }

    #[test]
    fn mixed_not_alive_without_attention_is_degraded() {
        // One Pending (not alive, not recovering) + one Live, no attention.
        let (e, _) = summarize_delivery(&cached(
            "delivering",
            vec![
                ep("YT", true, EndpointLifecycle::Live),
                ep("FB", false, EndpointLifecycle::Pending),
            ],
        ));
        assert_eq!(e.state, "degraded");
    }

    #[test]
    fn any_attention_rolls_up_to_attention() {
        let (e, _) = summarize_delivery(&cached(
            "delivering",
            vec![
                ep("YT", true, EndpointLifecycle::Live),
                ep("FB", false, EndpointLifecycle::Attention),
            ],
        ));
        assert_eq!(e.state, "attention");
    }

    #[test]
    fn attention_takes_precedence_over_recovering() {
        // Both an Attention and a Recovering endpoint present: red wins so the
        // operator's eye lands on the endpoint that needs action.
        let (e, _) = summarize_delivery(&cached(
            "delivering",
            vec![
                ep("YT", false, EndpointLifecycle::Recovering),
                ep("FB", false, EndpointLifecycle::Attention),
            ],
        ));
        assert_eq!(e.state, "attention");
    }
}

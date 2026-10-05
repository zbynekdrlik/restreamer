//! Endpoint start-up checks that must pass BEFORE anything is fetched or
//! pushed (#192).
//!
//! The service type is parsed ONCE here and the parsed [`ServiceType`] is
//! handed to the warmup loop and the consumer. An unknown type never falls
//! back to `ServiceType::TestFile`: since #192 that is a real loopback
//! accept-and-discard sink, so a fallback would "deliver" into a black hole
//! while the endpoint looks alive. Instead the endpoint refuses to start,
//! and says so where the operator looks: an error log line, `last_error` +
//! `stall_reason` in the VPS status, and the start-failure audit row the
//! host mirrors into `audit_log`.

use std::sync::Arc;

use rs_ffmpeg::ServiceType;

use crate::api::EndpointConfig;
use crate::audit_ring::AuditRing;
use crate::endpoint_audit;
use crate::endpoint_stats::Stats;

/// `stall_reason` of an endpoint that refused to start because its service
/// type is unknown.
pub(crate) const UNKNOWN_SERVICE_TYPE_STALL: &str = "unknown_service_type";

/// `delivery_mode` of a refused endpoint. It replaces the seeded "warmup"
/// so the dashboard never shows a WARMUP badge next to `alive: false`; the
/// UI renders no badge for it and shows the `stall_reason` instead.
pub(crate) const REFUSED_DELIVERY_MODE: &str = "refused";

/// Parse the endpoint's service type, or refuse the start loudly and return
/// `None` (the caller returns without fetching or pushing anything).
pub(crate) async fn service_type_or_refuse(
    ep_cfg: &EndpointConfig,
    stats: &Stats,
    audit_ring: &Option<Arc<AuditRing>>,
) -> Option<ServiceType> {
    let err = match ep_cfg.service_type.parse::<ServiceType>() {
        Ok(service_type) => return Some(service_type),
        Err(err) => err,
    };
    tracing::error!(
        alias = %ep_cfg.alias,
        service_type = %ep_cfg.service_type,
        "Endpoint NOT started: {err} -- refusing to deliver it anywhere (#192)"
    );
    {
        let mut s = stats.lock().await;
        s.last_error = Some(format!("endpoint not started: {err}"));
        s.stall_reason = Some(UNKNOWN_SERVICE_TYPE_STALL.to_string());
        s.delivery_mode = REFUSED_DELIVERY_MODE.to_string();
    }
    endpoint_audit::emit_unknown_service_type(
        audit_ring,
        &ep_cfg.alias,
        &ep_cfg.service_type,
        &err,
    );
    None
}

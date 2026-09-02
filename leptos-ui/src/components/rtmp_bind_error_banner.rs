//! Dedicated banner for an RTMP listener port-bind failure (#106).
//!
//! When another process holds the RTMP port (e.g. a legacy `inpoint_service`
//! on 1234), the RTMP server cannot bind and OBS/vMix cannot publish — but the
//! dashboard used to show "everything fine" because the bind error was
//! swallowed. This banner makes the failure loud and actionable: it names the
//! port and, when the OS conflict was identified, the holding process so the
//! operator knows exactly what to kill.
//!
//! Driven by `store.rtmp_bind_error` (`Some(msg)` while the port is held),
//! refreshed every 2s from the `/api/v1/status` poll and set instantly by the
//! `RtmpBindFailed` WebSocket event. Clears automatically once the port frees.

use crate::store::DashboardStore;
use leptos::prelude::*;

#[component]
pub fn RtmpBindErrorBanner() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let bind_error = store.rtmp_bind_error;

    view! {
        <Show when=move || bind_error.get().is_some()>
            <div class="banner banner--critical" role="alert" data-testid="rtmp-bind-error-banner">
                {move || {
                    // The recorded message already names the port + holder and
                    // ends with "RTMP streaming will not work...".
                    format!(
                        "\u{26A0}\u{FE0F} RTMP server failed to start: {}",
                        bind_error.get().unwrap_or_default()
                    )
                }}
            </div>
        </Show>
    }
}

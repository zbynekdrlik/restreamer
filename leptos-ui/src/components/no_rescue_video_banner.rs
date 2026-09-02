//! Dedicated banner warning that the CURRENT live / going-live event has no
//! custom rescue video configured (#260).
//!
//! When an event's `rescue_video_url` is NULL/empty, a delivery outage falls
//! back to the embedded generic default clip
//! (`rs_delivery::rescue::resolve_rescue_source` → `Countdown`) instead of a
//! branded Slovak clip — and until #260 nothing told the operator. That is
//! exactly what happened on 2026-06-19 (event 9316): it went live with an empty
//! rescue video and nobody knew what viewers would see during the 4G outage.
//!
//! Driven ENTIRELY off `store.streaming_event` (the `ZeroEndpointBanner`
//! precedent) — `rescue_video_url` already rides on the event that the
//! `/api/v1/status` poll delivers, so this banner needs no new status field.
//! The durable counterpart is the `Action::NoRescueVideoConfigured` audit row
//! emitted at delivery start (`rs-api`).

use crate::api::StreamingEvent;
use crate::store::DashboardStore;
use leptos::prelude::*;

impl StreamingEvent {
    /// True when the event is receiving or delivering — i.e. live or going
    /// live, the moment a missing rescue video actually matters.
    pub fn is_live(&self) -> bool {
        self.receiving_activated || self.delivering_activated
    }

    /// True when no usable custom rescue video is configured
    /// (`rescue_video_url` is absent, empty, or whitespace-only). Mirrors
    /// `rs_core::models::StreamingEvent::rescue_video_missing` — the wasm UI
    /// crate cannot depend on `rs-core`, so the predicate is duplicated and
    /// both sides are unit-tested to keep them in lockstep.
    pub fn rescue_video_missing(&self) -> bool {
        match self.rescue_video_url.as_deref() {
            Some(url) => url.trim().is_empty(),
            None => true,
        }
    }
}

/// Amber warning shown when the current event is live / going live but has no
/// custom rescue video. Slovak, operator-facing.
#[component]
pub fn NoRescueVideoBanner() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let event = store.streaming_event;

    let show = Memo::new(move |_| {
        event
            .get()
            .map(|e| e.is_live() && e.rescue_video_missing())
            .unwrap_or(false)
    });

    view! {
        <Show when=move || show.get()>
            <div class="banner banner--warn" role="alert" data-testid="no-rescue-video-banner">
                {"\u{26A0}\u{FE0F} Táto udalosť nemá nastavené záložné video — pri výpadku sa pustí generické núdzové video, nie vaše. Nahraj záložné video v nastaveniach udalosti."}
            </div>
        </Show>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(receiving: bool, delivering: bool, url: Option<&str>) -> StreamingEvent {
        StreamingEvent {
            id: 1,
            name: "9316".to_string(),
            received_bytes: 0,
            receiving_activated: receiving,
            delivering_activated: delivering,
            cache_delay_secs: None,
            created_from: None,
            rescue_video_url: url.map(str::to_string),
        }
    }

    #[test]
    fn rescue_video_missing_when_none_empty_or_whitespace() {
        assert!(ev(true, false, None).rescue_video_missing());
        assert!(ev(true, false, Some("")).rescue_video_missing());
        assert!(ev(true, false, Some("   \t ")).rescue_video_missing());
    }

    #[test]
    fn rescue_video_present_when_url_set() {
        assert!(!ev(true, false, Some("https://s3.example/rescue.flv")).rescue_video_missing());
        assert!(
            !ev(true, false, Some("  https://s3.example/rescue.flv  ")).rescue_video_missing(),
            "surrounding whitespace must not falsely flag a configured URL as missing"
        );
    }

    #[test]
    fn is_live_true_when_receiving_or_delivering() {
        assert!(ev(true, false, None).is_live());
        assert!(ev(false, true, None).is_live());
        assert!(
            !ev(false, false, None).is_live(),
            "an idle event is not live — banner must not fire on the idle dashboard"
        );
    }
}

//! Operator-facing single-page dashboard — vertical pipeline flow with endpoint tree.

use gloo_timers::callback::Interval;
use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

use super::add_endpoint_modal::AddEndpointModal;
use super::audit_panel::AuditPanel;
use super::confirm_modal::ConfirmModal;
use super::disk_pressure_banner::DiskPressureBanner;
use super::endpoint_tree::EndpointTree;
use super::ingest_skew_banner::IngestSkewBanner;
use super::mbps_graph::MbpsGraph;
use super::oauth_authorize::OAuthAuthorize;
use super::outage_banner::OutageBanner;
use super::pacing_panel::PacingPanel;
use super::s3_region_banner::S3RegionBanner;
use super::upload_strip::UploadStrip;
use super::zero_endpoint_banner::ZeroEndpointBanner;
use crate::api;
use crate::store::DashboardStore;
use crate::utils::{vps_destroy_dot_class, vps_destroy_label};

/// Minimum seconds the RTMP publisher must be connected before the
/// operator can start delivery. Mirrors
/// `rs_api::delivery_handlers::RTMP_STABLE_REQUIRED_SECS` but kept as a
/// client-side constant because the WASM target cannot depend on
/// `rs-api`.
const RTMP_STABLE_REQUIRED_SECS: u64 = 15;

/// Main operator dashboard view.
#[component]
pub fn OperatorDashboard() -> impl IntoView {
    let show_add_modal = RwSignal::new(false);
    provide_context(show_add_modal);

    view! {
        <div class="operator-dashboard">
            <IngestSkewBanner />
            <DiskPressureBanner />
            <S3RegionBanner />
            <ZeroEndpointBanner />
            <OutageBanner />
            <div class="operator-dashboard__layout">
                <div class="operator-dashboard__main">
                    <ControlBar />
                    <Pipeline />
                </div>
                <aside class="operator-dashboard__sidebar">
                    <AuditPanel />
                    <PacingPanel />
                    <OAuthAuthorize />
                </aside>
            </div>
            <AddEndpointModal show=show_add_modal />
        </div>
    }
}

// ---------------------------------------------------------------------------
// ControlBar
// ---------------------------------------------------------------------------

/// Control bar with event selector, start/stop buttons, state badge, timer, cache.
#[component]
fn ControlBar() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let loading = RwSignal::new(false);
    let show_stop_confirm = RwSignal::new(false);
    // #354: the emergency-override confirmation for starting delivery while
    // the ingest A/V-skew banner is latched. Separate from `show_stop_confirm`
    // so the two flows don't fight over one signal.
    let show_skew_override_confirm = RwSignal::new(false);

    // Poll /status every 2s so rtmp_stable_secs updates even when the
    // WebSocket only emits InpointStatus on byte-count ticks.
    //
    // Only `rtmp_stable_secs` is pulled from the poll. `inpoint_connected`
    // stays WebSocket-authoritative — the InpointStatus event on the WS is
    // the single source of truth for the RTMP connection indicator, so
    // overwriting it here would cause the pipeline display to flip back to
    // "connected" within a poll cycle after a disconnect event.
    let _status_poll = Interval::new(2_000, move || {
        spawn_local(async move {
            if let Ok(s) = api::get_status().await {
                store.rtmp_stable_secs.set(s.rtmp_stable_secs);
                store.disk_pressure.set(s.disk_pressure);
                store.s3_region_standard.set(s.s3_region_standard);
                store.ingest_skew_ms.set(s.ingest_skew_ms);
                store.ingest_skew_active.set(s.ingest_skew_active);
            }
        });
    });
    std::mem::forget(_status_poll);

    let pipeline_state = move || store.pipeline_state.get().state.clone();
    let is_active = move || {
        let s = pipeline_state();
        s == "streaming" || s == "buffering" || s == "buffer_exhausted"
    };

    // Lock event selector when pipeline is active
    let is_delivering_active = move || is_active();

    // Auto-select the active event on mount. Keyed off `streaming_event`
    // (WS-synced, backend-canonical receiving-preferred current event —
    // see rs-core::db::get_streaming_event) rather than re-scanning
    // events_list for `delivering_activated` only: an event that is
    // activated (receiving) but not yet delivering was never matched by
    // the old condition, leaving the pacing panel stuck on "No event
    // selected" even though the backend correctly reports an active
    // event (#151).
    Effect::new(move |_| {
        if store.selected_event_id.get_untracked().is_none() {
            if let Some(active) = store.streaming_event.get() {
                if active.receiving_activated || active.delivering_activated {
                    store.selected_event_id.set(Some(active.id));
                }
            }
        }
    });

    // Shared by the normal Start click AND the skew-override confirm below --
    // `force` is `false` for the former, `true` for the latter (#354).
    let start_delivery = move |force: bool| {
        let selected = store.selected_event_id.get();
        if let Some(event_id) = selected {
            loading.set(true);
            spawn_local(async move {
                if let Err(e) = api::start_stream(event_id, force).await {
                    store.push_error("dashboard".to_string(), format!("Start failed: {e}"));
                }
                loading.set(false);
                if let Ok(events) = api::list_events().await {
                    store.events_list.set(events);
                }
            });
        }
    };

    let on_start = move |_| start_delivery(false);

    // #354: the ONLY thing blocking Start is the ingest-skew latch -- offer
    // the deliberate emergency override instead of a hard dead-end. Gated on
    // the SAME conditions as the Start button's own `disabled` (below), minus
    // the skew check itself, so this never appears while some OTHER gate
    // (no event selected, already active, RTMP not stable) is also failing.
    let skew_override_available = move || {
        store.ingest_skew_active.get()
            && store.selected_event_id.get().is_some()
            && !is_active()
            && store.rtmp_stable_secs.get() >= RTMP_STABLE_REQUIRED_SECS
    };
    let on_skew_override_click = move |_| {
        show_skew_override_confirm.set(true);
    };
    let on_skew_override_confirmed = Callback::new(move |()| {
        start_delivery(true);
    });
    let skew_override_confirm_message = Signal::derive(move || {
        let secs = (store.ingest_skew_ms.get().abs() as f64 / 1000.0).round();
        format!(
            "Zvuk a obraz z OBS sú rozídené o ~{secs} s. Každý cieľ (YouTube, Facebook...) sa \
             pravdepodobne bude opakovane odpájať, kým to platí. Naozaj chceš spustiť delivery \
             napriek tomu?"
        )
    });

    let on_stop_click = move |_| {
        show_stop_confirm.set(true);
    };

    let on_stop_confirmed = Callback::new(move |()| {
        let selected = store.selected_event_id.get();
        if let Some(event_id) = selected {
            loading.set(true);
            spawn_local(async move {
                if let Err(e) = api::stop_stream(event_id).await {
                    store.push_error("dashboard".to_string(), format!("Stop failed: {e}"));
                }
                loading.set(false);
                if let Ok(events) = api::list_events().await {
                    store.events_list.set(events);
                }
            });
        }
    });

    let stop_confirm_message = Signal::derive(move || {
        let ep_count = store.delivery.get().endpoints.len();
        let event_name = store
            .pipeline_state
            .get()
            .event_name
            .unwrap_or_else(|| "this event".to_string());
        format!(
            "This will stop all delivery for \"{}\" and tear down the VPS. \
             {} endpoint(s) will go offline immediately.",
            event_name, ep_count
        )
    });

    // 1-second tick for session timer
    let tick = RwSignal::new(0u32);
    let _interval = Interval::new(1_000, move || {
        tick.update(|t| *t = t.wrapping_add(1));
    });
    std::mem::forget(_interval);

    let session_duration = move || {
        let _ = tick.get();
        let ps = store.pipeline_state.get();
        if let Some(ref start) = ps.session_start {
            let start_ms = js_sys::Date::parse(start);
            if start_ms.is_nan() {
                return "--:--:--".to_string();
            }
            let now_ms = js_sys::Date::now();
            let elapsed_secs = ((now_ms - start_ms) / 1000.0).max(0.0) as u64;
            let h = elapsed_secs / 3600;
            let m = (elapsed_secs % 3600) / 60;
            let s = elapsed_secs % 60;
            format!("{h:02}:{m:02}:{s:02}")
        } else {
            "--:--:--".to_string()
        }
    };

    let state_class = move || format!("state-badge {}", pipeline_state());

    let state_label = move || {
        match pipeline_state().as_str() {
            "idle" => "Idle",
            "buffering" => "Buffering",
            "streaming" => "Streaming",
            "stopping" => "Stopping",
            "buffer_exhausted" => "Exhausted",
            _ => "Idle",
        }
        .to_string()
    };

    view! {
        <div class="control-bar">
            <div class="control-bar-left">
                <label class="event-selector-label">"Event:"</label>
                <select
                    class="event-selector"
                    disabled=move || is_delivering_active()
                    on:change=move |ev| {
                        let val = event_target_value(&ev);
                        let id: Option<i64> = val.parse().ok();
                        store.selected_event_id.set(id);
                    }
                >
                    <option value="">"-- Select Event --"</option>
                    {move || {
                        store.events_list.get().iter().map(|e| {
                            let id_str = e.id.to_string();
                            let name = e.name.clone();
                            let selected = store.selected_event_id.get() == Some(e.id);
                            view! {
                                <option value={id_str} selected=selected>{name}</option>
                            }
                        }).collect::<Vec<_>>()
                    }}
                </select>
                <button
                    class="start-btn"
                    on:click=on_start
                    disabled=move || {
                        loading.get()
                            || store.selected_event_id.get().is_none()
                            || is_active()
                            || store.rtmp_stable_secs.get() < RTMP_STABLE_REQUIRED_SECS
                            || store.ingest_skew_active.get()
                    }
                    title=move || {
                        let stable = store.rtmp_stable_secs.get();
                        if store.ingest_skew_active.get() {
                            // #354: name the SOURCE fault + the remedy so the
                            // operator knows WHY Start is blocked.
                            let secs = (store.ingest_skew_ms.get().abs() as f64 / 1000.0).round();
                            format!(
                                "Zvuk a obraz z OBS sú rozídené o ~{secs} s — reštartuj stream v OBS"
                            )
                        } else if stable < RTMP_STABLE_REQUIRED_SECS {
                            format!(
                                "Waiting for OBS stream to stabilize ({stable}/{RTMP_STABLE_REQUIRED_SECS}s)"
                            )
                        } else {
                            "Start delivering".to_string()
                        }
                    }
                >
                    "Start Delivering"
                </button>
                <Show when=skew_override_available>
                    <button
                        class="skew-override-btn"
                        data-testid="skew-override-btn"
                        on:click=on_skew_override_click
                        title="Núdzové spustenie napriek rozídenému zvuku a obrazu z OBS"
                    >
                        "Spustiť napriek rozídeniu"
                    </button>
                </Show>
                <button
                    class="stop-btn"
                    on:click=on_stop_click
                    disabled=move || loading.get() || !(is_active() || is_delivering_active())
                >
                    "Stop Delivering"
                </button>
            </div>
            <div class="control-bar-right">
                <span class={state_class}>{state_label}</span>
                <span class="session-timer">{session_duration}</span>
            </div>
            <ConfirmModal
                show=show_stop_confirm
                title="Stop Delivering?"
                message=stop_confirm_message
                confirm_label="Stop Delivering"
                on_confirm=on_stop_confirmed
            />
            <ConfirmModal
                show=show_skew_override_confirm
                title="Spustiť napriek rozídeniu OBS?"
                message=skew_override_confirm_message
                confirm_label="Spustiť napriek tomu"
                on_confirm=on_skew_override_confirmed
            />
        </div>
    }
}

// ---------------------------------------------------------------------------
// Pipeline — vertical flow with 4 nodes + endpoint tree
// ---------------------------------------------------------------------------

/// Vertical pipeline flow: OBS -> RTMP -> BUFFER -> S3/VPS -> EndpointTree.
#[component]
fn Pipeline() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");

    let obs = move || store.obs_status.get();
    let rtmp_connected = move || store.inpoint_connected.get();
    let ps = move || store.pipeline_state.get();
    let is_delivering = move || {
        let s = ps().state;
        s == "buffering" || s == "streaming" || s == "buffer_exhausted"
    };
    let local_chunks = move || {
        if is_delivering() {
            ps().local_buffer_chunks
        } else {
            store.chunk_stats.get().pending_chunks
        }
    };
    let s3_chunks = move || {
        if is_delivering() {
            ps().s3_queue_chunks
        } else {
            store.chunk_stats.get().sent_chunks
        }
    };
    let delivery_status = move || store.delivery.get().status.clone();

    let obs_toggle_loading = RwSignal::new(false);
    let on_obs_toggle = move |_| {
        let currently_streaming = obs().streaming;
        obs_toggle_loading.set(true);
        spawn_local(async move {
            let result = if currently_streaming {
                api::obs_stop_stream().await
            } else {
                api::obs_start_stream().await
            };
            if let Err(e) = result {
                let store = use_context::<DashboardStore>().expect("DashboardStore");
                store.push_error("obs".to_string(), format!("OBS control failed: {e}"));
            }
            obs_toggle_loading.set(false);
        });
    };

    // OBS node status
    let obs_dot_class = move || {
        let o = obs();
        if o.streaming {
            "status-dot active"
        } else if o.connected || rtmp_connected() {
            "status-dot warning"
        } else {
            "status-dot"
        }
    };
    let obs_metric = move || {
        let o = obs();
        if o.streaming {
            "Streaming".to_string()
        } else if o.connected {
            "Connected".to_string()
        } else if rtmp_connected() {
            "RTMP Only".to_string()
        } else {
            "Disconnected".to_string()
        }
    };

    // RTMP node — bitrate from delta + ABSOLUTE received_bytes from inpoint.
    // Previously this computed a "session bytes = current - session_start"
    // delta where session_start was reset on every page load and every
    // disconnect. After 10 hours of streaming the dashboard would show ~3 MB
    // because the session-start kept getting reset. The absolute
    // received_bytes from the InpointStatus WS event is the right number —
    // it persists across page reloads in the streaming_event DB row.
    let rtmp_dot = move || {
        if rtmp_connected() {
            "status-dot active"
        } else {
            "status-dot"
        }
    };
    let prev_bytes = RwSignal::new(0i64);
    let bitrate_mbps = RwSignal::new(0.0f64);
    let _bitrate_interval = Interval::new(2_000, move || {
        let current = store.chunk_stats.get().total_bytes;
        let prev = prev_bytes.get_untracked();
        if prev > 0 && current > prev {
            let delta_bytes = (current - prev) as f64;
            let mbps = (delta_bytes * 8.0) / (2.0 * 1_000_000.0); // bits/sec -> Mbps
            bitrate_mbps.set(mbps);
        }
        prev_bytes.set(current);
    });
    std::mem::forget(_bitrate_interval);
    let rtmp_metric = move || {
        if rtmp_connected() {
            let mbps = bitrate_mbps.get();
            let current = store.chunk_stats.get().total_bytes;
            let bytes_str = api::format_bytes(current);
            if mbps > 0.1 {
                format!("{:.1} Mbps | {bytes_str}", mbps)
            } else {
                format!("Receiving | {bytes_str}")
            }
        } else {
            "Idle".to_string()
        }
    };

    // Local Buffer node — chunks waiting to be uploaded to S3
    let local_buffer_dot = move || {
        let chunks = local_chunks();
        if !rtmp_connected() {
            "status-dot"
        } else if chunks <= 1 {
            "status-dot active"
        } else if chunks <= 5 {
            "status-dot warning"
        } else {
            "status-dot error"
        }
    };
    let local_buffer_metric = move || {
        let chunks = local_chunks();
        if chunks > 0 {
            format!("{} chunks", chunks)
        } else {
            "0 chunks".to_string()
        }
    };

    // Persistent "VPS destroyed" confirmation (#75). Once delivery goes
    // idle for the selected event, fetch the most recent vps_deleted audit
    // record so the operator gets confirmation the VPS is actually gone —
    // and a warning if the Hetzner delete call itself failed — instead of
    // the node silently reverting to a blank "0 on S3" with no signal
    // either way. Scoped to the selected event so it never shows stale
    // teardown info for an event the operator has since switched away from.
    let last_destroy: RwSignal<Option<api::LastVpsDestroy>> = RwSignal::new(None);
    Effect::new(move |_| {
        let status = delivery_status();
        let event_id = store.selected_event_id.get();
        match (status.is_empty() || status == "none", event_id) {
            (true, Some(id)) => {
                spawn_local(async move {
                    last_destroy.set(api::get_last_vps_destroy(id).await.ok().flatten());
                });
            }
            _ => last_destroy.set(None),
        }
    });

    // S3 → Delivery node — chunks on S3 + delivered by VPS
    let delivered_chunks = move || {
        store
            .delivery
            .get()
            .endpoints
            .iter()
            .map(|ep| ep.chunks_processed)
            .max()
            .unwrap_or(0)
    };
    let s3_dot = move || {
        let p = ps();
        let s = delivery_status();
        match s.as_str() {
            "running" | "delivering" => {
                if p.state == "buffer_exhausted" {
                    "status-dot error"
                } else {
                    "status-dot active"
                }
            }
            // VPS provisioning phases — show as "warning" (yellow) so the
            // operator can distinguish them from idle (gray) and from
            // delivering (green). Each phase is normal but takes time.
            "creating" | "booting" | "initializing" => "status-dot warning",
            "stopping" => "status-dot warning",
            _ => {
                if is_delivering() {
                    "status-dot warning"
                } else if let Some(d) = last_destroy.get() {
                    vps_destroy_dot_class(&d.reason)
                } else {
                    "status-dot"
                }
            }
        }
    };
    let s3_metric = move || {
        let s = delivery_status();
        match s.as_str() {
            "running" | "delivering" => format!(
                "{} queued \u{2192} {} delivered",
                s3_chunks(),
                delivered_chunks()
            ),
            "" | "none" => match last_destroy.get() {
                // #75: confirm the teardown instead of a blank "0 on S3" —
                // and distinguish a clean destroy from one where Hetzner's
                // delete_server() call itself failed (still-billing risk).
                Some(d) => vps_destroy_label(&d.reason).to_string(),
                None => format!("{} on S3", s3_chunks()),
            },
            // Map orchestrator phases to operator-friendly text. Without
            // this, the dashboard would show the raw enum value (e.g.
            // "booting") which doesn't tell the user what's happening.
            "creating" => "Creating VPS \u{2026}".to_string(),
            "booting" => "VPS booting \u{2026}".to_string(),
            "initializing" => "Starting endpoints \u{2026}".to_string(),
            "stopping" => "Stopping \u{2026}".to_string(),
            other => other.to_string(),
        }
    };

    view! {
        <div class="pipeline">
            // --- OBS node ---
            <div class="pipeline-node" class:active=move || obs().streaming>
                <div class="pipeline-node-left">
                    <div class={obs_dot_class}></div>
                    <span class="pipeline-node-label">"OBS"</span>
                </div>
                <span class="pipeline-node-metric">{obs_metric}</span>
                {move || {
                    let o = obs();
                    if o.connected {
                        Some(view! {
                            <button
                                class="obs-toggle-btn"
                                on:click=on_obs_toggle
                                disabled=move || obs_toggle_loading.get()
                            >
                                {move || if obs().streaming { "Stop" } else { "Start" }}
                            </button>
                        })
                    } else {
                        None
                    }
                }}
            </div>
            <div class="pipeline-connector">{"\u{2502}"}</div>

            // --- RTMP node ---
            <div class="pipeline-node" class:active=move || rtmp_connected()>
                <div class="pipeline-node-left">
                    <div class={rtmp_dot}></div>
                    <span class="pipeline-node-label">"RTMP"</span>
                </div>
                <span class="pipeline-node-metric">{rtmp_metric}</span>
            </div>
            <div class="pipeline-connector">{"\u{2502}"}</div>

            // --- Local Buffer node ---
            <div class="pipeline-node" class:active=move || rtmp_connected()>
                <div class="pipeline-node-left">
                    <div class={local_buffer_dot}></div>
                    <span class="pipeline-node-label">"Local Buffer"</span>
                </div>
                <span class="pipeline-node-metric">{local_buffer_metric}</span>
            </div>
            <div class="pipeline-connector">{"\u{2502}"}</div>

            // --- S3 / Delivery node ---
            <div class="pipeline-node" class:active=move || {
                let s = delivery_status();
                s == "running" || s == "delivering"
            }>
                <div class="pipeline-node-left">
                    <div class={s3_dot}></div>
                    <span class="pipeline-node-label">"S3 \u{2192} VPS"</span>
                </div>
                <span class="pipeline-node-metric">{s3_metric}</span>
            </div>

            <UploadStrip />

            // --- Outgoing-Mbps history graph (#77) ---
            <MbpsGraph />

            // --- Endpoint tree (branching from VPS) ---
            <EndpointTree />
        </div>
    }
}

// EndpointTree lives in `endpoint_tree.rs` (split out to keep this file
// under the 1000-line-per-file cap, #75).

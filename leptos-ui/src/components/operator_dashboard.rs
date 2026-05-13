//! Operator-facing single-page dashboard — vertical pipeline flow with endpoint tree.

use gloo_timers::callback::Interval;
use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

use super::add_endpoint_modal::AddEndpointModal;
use super::audit_panel::AuditPanel;
use super::confirm_modal::ConfirmModal;
use super::endpoint_history::EndpointHistory;
use super::endpoint_remove_confirm_modal::EndpointRemoveConfirmModal;
use super::pacing_panel::PacingPanel;
use super::upload_strip::UploadStrip;
use super::zero_endpoint_banner::ZeroEndpointBanner;
use crate::api;
use crate::store::DashboardStore;
use crate::utils::cache_threshold_for_service;

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
            <ZeroEndpointBanner />
            <div class="operator-dashboard__layout">
                <div class="operator-dashboard__main">
                    <ControlBar />
                    <Pipeline />
                </div>
                <aside class="operator-dashboard__sidebar">
                    <AuditPanel />
                    <PacingPanel />
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

    // Auto-select the active event on mount
    Effect::new(move |_| {
        let events = store.events_list.get();
        if store.selected_event_id.get_untracked().is_none() {
            if let Some(active) = events.iter().find(|e| e.delivering_activated) {
                store.selected_event_id.set(Some(active.id));
            }
        }
    });

    let on_start = move |_| {
        let selected = store.selected_event_id.get();
        if let Some(event_id) = selected {
            loading.set(true);
            spawn_local(async move {
                if let Err(e) = api::start_stream(event_id).await {
                    store.push_error("dashboard".to_string(), format!("Start failed: {e}"));
                }
                loading.set(false);
                if let Ok(events) = api::list_events().await {
                    store.events_list.set(events);
                }
            });
        }
    };

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
                    }
                    title=move || {
                        let stable = store.rtmp_stable_secs.get();
                        if stable < RTMP_STABLE_REQUIRED_SECS {
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
            _ => {
                if is_delivering() {
                    "status-dot warning"
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
            "" | "none" => format!("{} on S3", s3_chunks()),
            // Map orchestrator phases to operator-friendly text. Without
            // this, the dashboard would show the raw enum value (e.g.
            // "booting") which doesn't tell the user what's happening.
            "creating" => "Creating VPS \u{2026}".to_string(),
            "booting" => "VPS booting \u{2026}".to_string(),
            "initializing" => "Starting endpoints \u{2026}".to_string(),
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

            // --- Endpoint tree (branching from VPS) ---
            <EndpointTree />
        </div>
    }
}

// ---------------------------------------------------------------------------
// EndpointTree — branching endpoints from the VPS node
// ---------------------------------------------------------------------------

#[component]
fn EndpointTree() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let show_add_modal = use_context::<RwSignal<bool>>().expect("show_add_modal");

    // Confirm modal state for endpoint removal
    let confirm_remove_alias: RwSignal<Option<String>> = RwSignal::new(None);
    let show_remove_confirm = RwSignal::new(false);

    // Last-endpoint confirm modal (type-to-confirm). Separate from the
    // generic confirm modal because it requires the operator to type the
    // event name to prevent accidental audience-offline clicks.
    let last_remove_alias: RwSignal<Option<String>> = RwSignal::new(None);
    let show_last_remove_modal = RwSignal::new(false);

    // When modal is dismissed, clear the alias
    Effect::new(move |_| {
        if !show_remove_confirm.get() {
            confirm_remove_alias.set(None);
        }
    });
    Effect::new(move |_| {
        if !show_last_remove_modal.get() {
            last_remove_alias.set(None);
        }
    });

    let remove_confirm_message = Signal::derive(move || match confirm_remove_alias.get() {
        Some(ref alias) => format!("Remove endpoint \"{}\" from active delivery?", alias),
        None => String::new(),
    });

    let on_remove_confirmed = Callback::new(move |()| {
        if let Some(alias) = confirm_remove_alias.get_untracked() {
            let event_id = store.pipeline_state.get().event_id.unwrap_or(0);
            spawn_local(async move {
                let _ = api::delivery_remove_endpoint(event_id, &alias).await;
            });
        }
    });

    // Props for the last-endpoint modal. Signals are derived from the
    // `last_remove_alias` and pipeline_state so the modal body updates
    // reactively while it's mounted.
    let last_modal_alias: Signal<String> =
        Signal::derive(move || last_remove_alias.get().unwrap_or_default());
    let last_modal_event_name: Signal<String> =
        Signal::derive(move || store.pipeline_state.get().event_name.unwrap_or_default());
    let last_modal_visible: Signal<bool> = Signal::derive(move || show_last_remove_modal.get());

    let on_last_cancel = move || {
        show_last_remove_modal.set(false);
    };
    let on_last_confirm = move || {
        if let Some(alias) = last_remove_alias.get_untracked() {
            let event_id = store.pipeline_state.get().event_id.unwrap_or(0);
            spawn_local(async move {
                let _ = api::delivery_remove_endpoint(event_id, &alias).await;
            });
        }
        show_last_remove_modal.set(false);
    };

    // YouTube health polling: fast initial poll, then every 30s
    let yt_has_polled = RwSignal::new(false);
    let _yt_poll = Interval::new(5_000, move || {
        let delivery_active = !store.delivery.get().endpoints.is_empty();
        if delivery_active && !yt_has_polled.get_untracked() {
            yt_has_polled.set(true);
            spawn_local(async move {
                let health = api::get_youtube_health().await;
                store.youtube_health.set(health);
            });
        }
    });
    std::mem::forget(_yt_poll);
    let _yt_refresh = Interval::new(30_000, move || {
        let delivery_active = !store.delivery.get().endpoints.is_empty();
        if delivery_active {
            spawn_local(async move {
                let health = api::get_youtube_health().await;
                store.youtube_health.set(health);
            });
        }
    });
    std::mem::forget(_yt_refresh);

    let has_endpoints = Memo::new(move |_| !store.delivery.get().endpoints.is_empty());
    let is_running = Memo::new(move |_| {
        let s = store.delivery.get().status.clone();
        s == "running" || s == "delivering"
    });

    view! {
        <div class="endpoint-tree" style:display=move || if has_endpoints.get() || is_running.get() || store.pipeline_state.get().state == "buffering" { "block" } else { "none" }>
            // Buffering indicator only when no endpoints exist at all
            <Show when=move || {
                let ps = store.pipeline_state.get();
                ps.state == "buffering"
                    && store.delivery.get().endpoints.is_empty()
            } fallback=|| ()>
                <div class="buffering-indicator">
                    {move || {
                        let ps = store.pipeline_state.get();
                        format!("Buffering: {} chunks on S3 (~{}s)", ps.s3_queue_chunks, ps.cache_duration_secs as u64)
                    }}
                </div>
            </Show>
            <For
                each=move || store.delivery.get().endpoints.clone()
                key=|ep| ep.alias.clone()
                children=move |ep| {
                    let store = use_context::<DashboardStore>().expect("DashboardStore");
                    let alias = ep.alias.clone();
                    let remove_alias = alias.clone();
                    let ep_alias_key = alias.clone();
                    // Per-card toggle for the EndpointHistory sparkline.
                    let show_history = RwSignal::new(false);
                    let history_alias_signal: Signal<String> = Signal::derive({
                        let ep_alias_key = ep_alias_key.clone();
                        move || ep_alias_key.clone()
                    });

                    // Derive per-endpoint reactive data from the delivery signal
                    let ep_data = Memo::new(move |_| {
                        store.delivery.get().endpoints.iter()
                            .find(|e| e.alias == ep_alias_key)
                            .cloned()
                            .unwrap_or_default()
                    });

                    let connector = {
                        let alias = alias.clone();
                        move || {
                            let delivery = store.delivery.get();
                            let is_running = delivery.status == "running" || delivery.status == "delivering";
                            let is_last = delivery.endpoints.last().map_or(false, |last| last.alias == alias) && !is_running;
                            if is_last { "\u{2514}\u{2500}\u{2500}" } else { "\u{251C}\u{2500}\u{2500}" }
                        }
                    };

                    let status_class = move || {
                        let ep = ep_data.get();
                        if !ep.alive && ep.chunks_processed == 0 {
                            "endpoint-node pending"
                        } else if !ep.alive {
                            "endpoint-node dead"
                        } else if ep.stall_reason.is_some() || ep.ffmpeg_restart_count >= 10 {
                            "endpoint-node stalled"
                        } else {
                            "endpoint-node healthy"
                        }
                    };

                    let dot_class = move || {
                        let ep = ep_data.get();
                        let is_pending = !ep.alive && ep.chunks_processed == 0 && ep.chunk_delay_secs == 0.0;
                        let has_critical_error = ep.ffmpeg_restart_count >= 10
                            || ep.stall_reason.is_some();
                        if is_pending {
                            "status-dot"
                        } else if !ep.alive || has_critical_error {
                            "status-dot error"
                        } else {
                            "status-dot active"
                        }
                    };

                    let is_running_memo = Memo::new(move |_| {
                        let s = store.delivery.get().status.clone();
                        s == "running" || s == "delivering"
                    });

                    view! {
                        <div class="endpoint-branch">
                            <span class="branch-connector">{connector}</span>
                            <div
                                class=status_class
                                data-testid="endpoint-card"
                                data-is-fast=if ep.is_fast { "true" } else { "false" }
                            >
                                <div class=dot_class></div>
                                <span class="endpoint-alias">{ep.alias.clone()}</span>
                                {move || {
                                    ep_data.get().youtube_health.map(|h| {
                                        let data_health = h.health_status.clone();
                                        let tooltip = format!(
                                            "Status: {} / {}\nIssue: {}\n{}{}{}",
                                            h.stream_status,
                                            h.health_status,
                                            h.top_issue.clone().unwrap_or_else(|| "(none)".into()),
                                            h.resolution.clone().unwrap_or_default(),
                                            if h.resolution.is_some() && h.frame_rate.is_some() { " @ " } else { "" },
                                            h.frame_rate.clone().map(|f| format!("{f}fps")).unwrap_or_default(),
                                        );
                                        view! {
                                            <div
                                                class="yt-health-badge"
                                                data-testid="yt-health-badge"
                                                data-health=data_health
                                            >
                                                <span class="yt-health-dot"></span>
                                                <span class="yt-health-text">{h.health_status.clone()}</span>
                                                <div class="yt-health-tooltip" data-testid="yt-health-tooltip">
                                                    {tooltip}
                                                </div>
                                            </div>
                                        }
                                    })
                                }}
                                <span class="endpoint-metrics">
                                    {move || {
                                        let ep = ep_data.get();
                                        let is_pending = !ep.alive && ep.chunks_processed == 0 && ep.chunk_delay_secs == 0.0;
                                        if is_pending {
                                            String::new()
                                        } else {
                                            format!("{} chunks", ep.chunks_processed)
                                        }
                                    }}
                                </span>
                                {move || {
                                    ep_data.get().stall_reason.clone().map(|r| view! {
                                        <span class="endpoint-anomaly">{format!("stall: {r}")}</span>
                                    })
                                }}
                                {move || {
                                    let ep = ep_data.get();
                                    ep.delivery_mode.clone().and_then(|mode| {
                                        let (badge_class, label) = match mode.as_str() {
                                            "warmup" => ("endpoint-mode-warmup", "WARMUP"),
                                            "rescue" => ("endpoint-mode-rescue", "RESCUE"),
                                            "recovering" => {
                                                ("endpoint-mode-recovering", "RECOVERING")
                                            }
                                            _ => return None,
                                        };
                                        let eta = ep
                                            .rescue_eta_secs
                                            .map(|s| {
                                                if s >= 60 {
                                                    format!(" ~{}m {}s", s / 60, s % 60)
                                                } else {
                                                    format!(" ~{s}s")
                                                }
                                            })
                                            .unwrap_or_default();
                                        Some(view! {
                                            <span class=badge_class>
                                                {format!("{label}{eta}")}
                                            </span>
                                        })
                                    })
                                }}
                                {move || {
                                    ep_data.get().last_error.clone().map(|e| view! {
                                        <span class="endpoint-anomaly">{e}</span>
                                    })
                                }}
                                {move || {
                                    let count = ep_data.get().ffmpeg_restart_count;
                                    if count > 0 {
                                        Some(view! {
                                            <span class="endpoint-anomaly">{format!("ffmpeg x{count}")}</span>
                                        })
                                    } else {
                                        None
                                    }
                                }}
                                {move || {
                                    // Issue #172: rust-pusher reconnect counter.
                                    // Surfaces YT/FB upstream-rotation events the
                                    // operator otherwise had to dig out of the
                                    // audit log (every endpoint_rtmp_push_died
                                    // bumps this).
                                    let count = ep_data.get().reconnect_count;
                                    if count > 0 {
                                        Some(view! {
                                            <span class="endpoint-anomaly">{format!("reconn x{count}")}</span>
                                        })
                                    } else {
                                        None
                                    }
                                }}
                                {move || {
                                    let remove_alias = remove_alias.clone();
                                    is_running_memo.get().then(move || {
                                        let remove_alias = remove_alias.clone();
                                        view! {
                                            <button
                                                class="btn-remove-endpoint"
                                                title="Remove endpoint"
                                                on:click=move |_| {
                                                    let alias = remove_alias.clone();
                                                    // If this is the last endpoint on an
                                                    // active delivery, show the
                                                    // type-to-confirm last-endpoint modal
                                                    // instead of the generic one.
                                                    let d = store.delivery.get();
                                                    let is_last = d.endpoints.len() <= 1;
                                                    let ps_state =
                                                        store.pipeline_state.get().state.clone();
                                                    let pipeline_active = ps_state != "idle"
                                                        && ps_state != "stopping";
                                                    if is_last && pipeline_active {
                                                        last_remove_alias.set(Some(alias));
                                                        show_last_remove_modal.set(true);
                                                    } else {
                                                        confirm_remove_alias.set(Some(alias));
                                                        show_remove_confirm.set(true);
                                                    }
                                                }
                                            >
                                                {"\u{00D7}"}
                                            </button>
                                        }
                                    })
                                }}
                                {move || {
                                    let ep = ep_data.get();
                                    let ps = store.pipeline_state.get();
                                    let target = ps.target_delay_secs;
                                    if target == 0 {
                                        return None;
                                    }
                                    // Use per-endpoint delivery delay so each
                                    // endpoint's cache bar reflects its own
                                    // state. During the initial buffer-fill
                                    // phase each endpoint reports
                                    // chunk_delay_secs = 0, so we fall back to
                                    // the global cache_duration_secs until
                                    // delivery has started.
                                    //
                                    // The backend caps cache_duration_secs at
                                    // ~1.5x target (#187) so a Stop+Start
                                    // cycle no longer surfaces stale
                                    // accumulated values like 1726s.
                                    // Branch on is_fast: fast endpoints measure lag-from-live-edge
                                    // and want a low number (<=5s = green, >8s = critical). Non-fast
                                    // endpoints want the bar to fill to the target buffer (~120s).
                                    // See spec docs/superpowers/specs/2026-05-11-cache-metric-and-start-reset-design.md.
                                    let (cache_secs, target_label, progress, bar_class) = if ep.is_fast {
                                        // Fast endpoint UX: "Xs / live cache". Bar fill proportional
                                        // to lag / 8s ceiling (so a healthy 2s reads as a thin green
                                        // sliver, not an empty bar). Threshold: healthy <=5s, critical
                                        // >8s, warning otherwise.
                                        let secs = ep.chunk_delay_secs;
                                        let prog = (secs / 8.0).clamp(0.0, 1.0);
                                        let class_ = if secs > 8.0 {
                                            "buffer-bar-fill critical"
                                        } else if secs <= 5.0 {
                                            "buffer-bar-fill healthy"
                                        } else {
                                            "buffer-bar-fill warning"
                                        };
                                        (secs, "live".to_string(), prog, class_)
                                    } else {
                                        // Non-fast: prefer per-endpoint chunk_delay_secs so each
                                        // endpoint's bar shows ITS own buffer depth (regression
                                        // test at e2e/frontend.spec.ts:994). During prefill
                                        // (chunks_processed=0) fall back to ps.cache_duration_secs
                                        // which the backend caps at 1.5x target (#187).
                                        // Per-service threshold multiplier from utils.rs.
                                        let secs = if ep.chunks_processed > 0 {
                                            ep.chunk_delay_secs
                                        } else {
                                            ps.cache_duration_secs
                                        };
                                        let alias_lookup = ep.alias.clone();
                                        let service_type = store.endpoints_list.get()
                                            .iter()
                                            .find(|e| e.alias == alias_lookup)
                                            .map(|e| e.service_type.clone())
                                            .unwrap_or_default();
                                        let threshold_mult = cache_threshold_for_service(&service_type);
                                        let prog = (secs / target as f64).min(1.0);
                                        let class_ = if secs > target as f64 * threshold_mult {
                                            "buffer-bar-fill critical"
                                        } else if prog >= 0.75 {
                                            "buffer-bar-fill healthy"
                                        } else if prog >= 0.40 {
                                            "buffer-bar-fill warning"
                                        } else {
                                            "buffer-bar-fill critical"
                                        };
                                        (secs, format!("{}s", target), prog, class_)
                                    };
                                    let label = format!("{}s / {} cache", cache_secs as u64, target_label);
                                    Some(view! {
                                        <div class="endpoint-cache">
                                            <div class="buffer-bar">
                                                <div class=bar_class style:width=format!("{}%", (progress * 100.0).min(100.0))></div>
                                            </div>
                                            <span class="endpoint-cache-label">{label}</span>
                                        </div>
                                    })
                                }}
                                <button
                                    class="btn-endpoint-history"
                                    title="Toggle chunk_delay history"
                                    on:click=move |_| show_history.update(|v| *v = !*v)
                                >
                                    "History"
                                </button>
                                <Show when=move || show_history.get()>
                                    <EndpointHistory alias=history_alias_signal />
                                </Show>
                            </div>
                        </div>
                    }
                }
            />
            <Show when=move || is_running.get() fallback=|| ()>
                <div class="endpoint-branch">
                    <span class="branch-connector">{"\u{2514}\u{2500}\u{2500}"}</span>
                    <button
                        class="btn-add-endpoint"
                        on:click=move |_| show_add_modal.set(true)
                    >
                        "+ Add"
                    </button>
                </div>
            </Show>
            <ConfirmModal
                show=show_remove_confirm
                title="Remove Endpoint?"
                message=remove_confirm_message
                confirm_label="Remove"
                on_confirm=on_remove_confirmed
            />
            <EndpointRemoveConfirmModal
                alias=last_modal_alias
                event_name=last_modal_event_name
                visible=last_modal_visible
                on_cancel=on_last_cancel
                on_confirm=on_last_confirm
            />
        </div>
    }
}

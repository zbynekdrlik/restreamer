//! Operator-facing single-page dashboard — vertical pipeline flow with endpoint tree.

use gloo_timers::callback::Interval;
use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::api;
use crate::store::DashboardStore;

/// Main operator dashboard view.
#[component]
pub fn OperatorDashboard() -> impl IntoView {
    let _store = use_context::<DashboardStore>().expect("DashboardStore");

    view! {
        <div class="operator-dashboard">
            <ControlBar />
            <Pipeline />
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

    let on_stop = move |_| {
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
    };

    let pipeline_state = move || store.pipeline_state.get().state.clone();
    let is_active = move || {
        let s = pipeline_state();
        s == "streaming" || s == "buffering" || s == "buffer_exhausted"
    };

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
                    disabled=move || loading.get() || store.selected_event_id.get().is_none() || is_active()
                >
                    "Start Delivering"
                </button>
                <button
                    class="stop-btn"
                    on:click=on_stop
                    disabled=move || loading.get() || !is_active()
                >
                    "Stop Delivering"
                </button>
            </div>
            <div class="control-bar-right">
                <span class={state_class}>{state_label}</span>
                <span class="session-timer">{session_duration}</span>
                <span class="cache-display">
                    "Cache: "
                    {move || {
                        let ps = store.pipeline_state.get();
                        let prefix = if ps.predicted { "~" } else { "" };
                        format!("{prefix}{}s / {}s", ps.current_delay_secs as u64, ps.target_delay_secs)
                    }}
                </span>
            </div>
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

    // RTMP node
    let rtmp_dot = move || {
        if rtmp_connected() { "status-dot active" } else { "status-dot" }
    };
    let rtmp_metric = move || {
        if rtmp_connected() {
            format!("{} chunks", store.chunk_stats.get().total_chunks)
        } else {
            "Idle".to_string()
        }
    };

    // Buffer node
    let buffer_dot = move || {
        let p = ps();
        if !is_delivering() {
            "status-dot"
        } else if p.state == "buffer_exhausted" {
            "status-dot error"
        } else if p.predicted {
            "status-dot warning"
        } else if p.buffer_progress >= 0.75 {
            "status-dot active"
        } else if p.buffer_progress >= 0.40 {
            "status-dot warning"
        } else {
            "status-dot error"
        }
    };
    let buffer_metric = move || {
        let p = ps();
        if is_delivering() {
            format!("{}s / {}s", p.current_delay_secs as u64, p.target_delay_secs)
        } else {
            format!("{} pending", local_chunks())
        }
    };
    let buffer_progress = move || store.pipeline_state.get().buffer_progress;
    let buffer_bar_class = move || {
        let p = ps();
        if p.state == "buffer_exhausted" {
            "cache-bar-fill exhausted"
        } else if p.predicted {
            "cache-bar-fill predicted"
        } else if buffer_progress() >= 0.75 {
            "cache-bar-fill healthy"
        } else if buffer_progress() >= 0.40 {
            "cache-bar-fill warning"
        } else {
            "cache-bar-fill critical"
        }
    };

    // S3/VPS node
    let vps_dot = move || {
        let s = delivery_status();
        if s == "running" || s == "delivering" {
            "status-dot active"
        } else {
            "status-dot"
        }
    };
    let vps_metric = move || {
        let s = delivery_status();
        let ep_count = store.delivery.get().endpoints.len();
        if s == "running" || s == "delivering" {
            format!("{} queued \u{2192} {} endpoints", s3_chunks(), ep_count)
        } else if s.is_empty() || s == "none" {
            "Idle".to_string()
        } else {
            s
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

            // --- BUFFER node ---
            <div class="pipeline-node" class:active=move || is_delivering()>
                <div class="pipeline-node-left">
                    <div class={buffer_dot}></div>
                    <span class="pipeline-node-label">"BUFFER"</span>
                </div>
                <span class="pipeline-node-metric">{buffer_metric}</span>
                <div class="pipeline-buffer-bar"
                    style:display=move || if is_delivering() { "block" } else { "none" }
                >
                    <div class={buffer_bar_class}
                        style:width=move || format!("{}%", (buffer_progress() * 100.0).min(100.0))
                    ></div>
                </div>
            </div>
            <div class="pipeline-connector">{"\u{2502}"}</div>

            // --- S3 -> VPS node ---
            <div class="pipeline-node" class:active=move || {
                let s = delivery_status();
                s == "running" || s == "delivering"
            }>
                <div class="pipeline-node-left">
                    <div class={vps_dot}></div>
                    <span class="pipeline-node-label">"S3 \u{2192} VPS"</span>
                </div>
                <span class="pipeline-node-metric">{vps_metric}</span>
            </div>

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

    let has_endpoints = move || !store.delivery.get().endpoints.is_empty();
    let is_running = move || {
        let s = store.delivery.get().status.clone();
        s == "running" || s == "delivering"
    };

    view! {
        <div class="endpoint-tree" style:display=move || if has_endpoints() || is_running() { "block" } else { "none" }>
            {move || {
                let delivery = store.delivery.get();
                let eps = delivery.endpoints.clone();
                let len = eps.len();
                eps.into_iter().enumerate().map(|(i, ep)| {
                    let is_last = i == len - 1 && !is_running();
                    let connector = if is_last {
                        "\u{2514}\u{2500}\u{2500}"
                    } else {
                        "\u{251C}\u{2500}\u{2500}"
                    };
                    let has_anomaly = ep.chunk_delay_secs > 30.0
                        || ep.stall_reason.is_some()
                        || ep.ffmpeg_restart_count > 0
                        || !ep.alive;
                    let status_class = if !ep.alive && ep.chunks_processed == 0 {
                        "endpoint-node pending"
                    } else if !ep.alive {
                        "endpoint-node dead"
                    } else if ep.stall_count >= 3 || ep.stall_reason.is_some() {
                        "endpoint-node stalled"
                    } else {
                        "endpoint-node healthy"
                    };
                    let alias = ep.alias.clone();
                    let is_youtube = {
                        let a = alias.to_lowercase();
                        a.contains("youtube") || a.contains("yt")
                    };
                    let remove_alias = alias.clone();

                    view! {
                        <div class="endpoint-branch">
                            <span class="branch-connector">{connector}</span>
                            <div class={status_class}>
                                <span class="endpoint-alias">{alias}</span>
                                {if is_youtube {
                                    Some(view! {
                                        <span class=move || {
                                            let health = store.youtube_health.get()
                                                .and_then(|r| r.streams.first()
                                                    .and_then(|s| s.health_status.clone()));
                                            match health.as_deref() {
                                                Some("good") => "yt-health-badge good",
                                                Some("ok") => "yt-health-badge ok",
                                                Some("bad") => "yt-health-badge bad",
                                                _ => "yt-health-badge unknown",
                                            }
                                        }>
                                            {move || {
                                                store.youtube_health.get()
                                                    .and_then(|r| r.streams.first()
                                                        .and_then(|s| s.health_status.clone()))
                                                    .unwrap_or_else(|| "\u{2014}".to_string())
                                            }}
                                        </span>
                                    })
                                } else {
                                    None
                                }}
                                {if has_anomaly {
                                    let delay_text = format!("{:.0}s delay", ep.chunk_delay_secs);
                                    let stall_text = ep.stall_reason.clone();
                                    let error_text = ep.last_error.clone();
                                    let restarts = ep.ffmpeg_restart_count;
                                    Some(view! {
                                        <span class="endpoint-anomaly">
                                            {if ep.chunk_delay_secs > 30.0 {
                                                Some(view! { <span class="anomaly-delay">{delay_text}</span> })
                                            } else {
                                                None
                                            }}
                                            {stall_text.map(|r| view! {
                                                <span class="anomaly-stall">{format!("stall: {r}")}</span>
                                            })}
                                            {error_text.map(|e| view! {
                                                <span class="anomaly-error">{e}</span>
                                            })}
                                            {if restarts > 0 {
                                                Some(view! {
                                                    <span class="anomaly-restart">
                                                        {format!("ffmpeg restarts: {restarts}")}
                                                    </span>
                                                })
                                            } else {
                                                None
                                            }}
                                        </span>
                                    })
                                } else {
                                    None
                                }}
                                {move || {
                                    let remove_alias = remove_alias.clone();
                                    is_running().then(move || {
                                        let remove_alias = remove_alias.clone();
                                        view! {
                                            <button
                                                class="btn-remove-endpoint"
                                                title="Remove endpoint"
                                                on:click=move |_| {
                                                    let alias = remove_alias.clone();
                                                    let event_id = store.pipeline_state.get()
                                                        .event_id.unwrap_or(0);
                                                    spawn_local(async move {
                                                        let _ = api::delivery_remove_endpoint(
                                                            event_id, &alias,
                                                        ).await;
                                                    });
                                                }
                                            >
                                                {"\u{00D7}"}
                                            </button>
                                        }
                                    })
                                }}
                            </div>
                        </div>
                    }
                }).collect::<Vec<_>>()
            }}
            {move || is_running().then(|| view! {
                <div class="endpoint-branch">
                    <span class="branch-connector">{"\u{2514}\u{2500}\u{2500}"}</span>
                    <AddEndpointControl />
                </div>
            })}
        </div>
    }
}

// ---------------------------------------------------------------------------
// AddEndpointControl
// ---------------------------------------------------------------------------

/// Dropdown to add an unattached endpoint to the running delivery.
#[component]
fn AddEndpointControl() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let start_position = RwSignal::new("Live".to_string());

    view! {
        <div class="add-endpoint-control">
            <select
                class="add-endpoint-select"
                on:change=move |ev| {
                    let val = event_target_value(&ev);
                    if let Ok(ep_id) = val.parse::<i64>() {
                        let pos = start_position.get();
                        let event_id = store.pipeline_state.get().event_id.unwrap_or(0);
                        spawn_local(async move {
                            let _ = api::delivery_add_endpoint(event_id, ep_id, &pos).await;
                        });
                    }
                }
            >
                <option value="">"+ Add endpoint"</option>
                {move || {
                    let all = store.endpoints_list.get();
                    let active_aliases: Vec<String> = store.delivery.get()
                        .endpoints.iter().map(|e| e.alias.clone()).collect();
                    all.iter()
                        .filter(|ep| !active_aliases.contains(&ep.alias))
                        .map(|ep| {
                            let id_str = ep.id.to_string();
                            let alias = ep.alias.clone();
                            view! { <option value={id_str}>{alias}</option> }
                        })
                        .collect::<Vec<_>>()
                }}
            </select>
            <select
                class="start-position-select"
                prop:value=move || start_position.get()
                on:change=move |ev| start_position.set(event_target_value(&ev))
            >
                <option value="Live">"Live"</option>
                <option value="Beginning">"From Beginning"</option>
            </select>
        </div>
    }
}

//! Operator-facing single-page dashboard.

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
            <PipelineFlow />
            <CacheBar />
            <EndpointGroups />
            <ActivityFeed />
        </div>
    }
}

/// Control bar with event selector and start/stop buttons.
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
                    store.push_error(
                        "dashboard".to_string(),
                        format!("Start failed: {e}"),
                    );
                }
                loading.set(false);
                // Refresh events list
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
                    store.push_error(
                        "dashboard".to_string(),
                        format!("Stop failed: {e}"),
                    );
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

    let session_duration = move || {
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

    let state_class = move || {
        let s = pipeline_state();
        format!("state-badge {s}")
    };

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

/// Horizontal pipeline flow visualization.
#[component]
fn PipelineFlow() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");

    let rtmp_connected = move || store.inpoint_connected.get();
    let chunk_stats = move || store.chunk_stats.get();
    let delivery_status = move || store.delivery.get().status.clone();

    view! {
        <div class="pipeline-flow">
            <div class="pipeline-node">
                <span class={move || if rtmp_connected() { "status-dot active" } else { "status-dot" }}></span>
                <span class="pipeline-label">"OBS"</span>
                <span class="pipeline-metric">{move || if rtmp_connected() { "Connected" } else { "Disconnected" }}</span>
            </div>
            <span class="pipeline-arrow">{"\u{2192}"}</span>
            <div class="pipeline-node">
                <span class={move || if rtmp_connected() { "status-dot active" } else { "status-dot" }}></span>
                <span class="pipeline-label">"RTMP"</span>
                <span class="pipeline-metric">{move || if rtmp_connected() { "Receiving" } else { "Idle" }}</span>
            </div>
            <span class="pipeline-arrow">{"\u{2192}"}</span>
            <div class="pipeline-node">
                <span class={move || if chunk_stats().total_chunks > 0 { "status-dot active" } else { "status-dot" }}></span>
                <span class="pipeline-label">"Chunker"</span>
                <span class="pipeline-metric">{move || format!("{} chunks", chunk_stats().total_chunks)}</span>
            </div>
            <span class="pipeline-arrow">{"\u{2192}"}</span>
            <div class="pipeline-node">
                <span class={move || if chunk_stats().sent_chunks > 0 { "status-dot active" } else { "status-dot" }}></span>
                <span class="pipeline-label">"S3 Upload"</span>
                <span class="pipeline-metric">{move || format!("{} pending", chunk_stats().pending_chunks)}</span>
            </div>
            <span class="pipeline-arrow">{"\u{2192}"}</span>
            <div class="pipeline-node">
                <span class={move || {
                    let s = delivery_status();
                    if s == "running" { "status-dot active" } else { "status-dot" }
                }}></span>
                <span class="pipeline-label">"VPS"</span>
                <span class="pipeline-metric">{move || {
                    let s = delivery_status();
                    if s.is_empty() || s == "none" { "Idle".to_string() } else { s }
                }}</span>
            </div>
        </div>
    }
}

/// Cache buffer progress bar.
#[component]
fn CacheBar() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");

    let progress = move || store.pipeline_state.get().buffer_progress;
    let target = move || store.pipeline_state.get().target_delay_secs;
    let current = move || store.pipeline_state.get().current_delay_secs;
    let is_predicted = move || store.pipeline_state.get().predicted;
    let is_exhausted = move || store.pipeline_state.get().state == "buffer_exhausted";
    let is_visible = move || {
        let ps = store.pipeline_state.get();
        ps.state == "buffering" || ps.state == "streaming" || ps.state == "buffer_exhausted"
    };
    let bar_class = move || {
        if is_exhausted() {
            "cache-bar-fill exhausted"
        } else if is_predicted() {
            "cache-bar-fill predicted"
        } else {
            "cache-bar-fill"
        }
    };

    view! {
        <div class="cache-bar-container" style:display=move || if is_visible() { "block" } else { "none" }>
            <div class="cache-bar">
                <div class={bar_class} style:width=move || format!("{}%", (progress() * 100.0).min(100.0))></div>
            </div>
            <span class="cache-bar-label">
                {move || {
                    if is_exhausted() {
                        "Buffer Exhausted — delivery VPS has no remaining cache".to_string()
                    } else if is_predicted() {
                        format!("~{}s / {}s target (predicted — VPS unreachable)", current() as u64, target())
                    } else {
                        format!("Cache: {}s / {}s target", current() as u64, target())
                    }
                }}
            </span>
        </div>
    }
}

/// Endpoint cards grouped by fast (monitor) vs cached (delivery).
#[component]
fn EndpointGroups() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");

    let has_endpoints = move || !store.delivery.get().endpoints.is_empty();
    let has_monitor_eps = move || {
        store.delivery.get().endpoints.iter()
            .any(|ep| ep.chunk_delay_secs < 10.0 && ep.alive)
    };

    view! {
        <div class="endpoint-groups" style:display=move || if has_endpoints() { "grid" } else { "none" }
             style:grid-template-columns=move || if has_monitor_eps() { "1fr 1fr" } else { "1fr" }>
            {move || {
                if has_monitor_eps() {
                    Some(view! {
                        <div class="endpoint-group">
                            <h3 class="group-title">"Monitor Endpoints"</h3>
                            {move || {
                                let delivery = store.delivery.get();
                                let monitor_eps: Vec<_> = delivery.endpoints.iter()
                                    .filter(|ep| ep.chunk_delay_secs < 10.0 && ep.alive)
                                    .cloned()
                                    .collect();
                                monitor_eps.into_iter().map(|ep| {
                                    let alias = ep.alias.clone();
                                    let alive = ep.alive;
                                    let delay = ep.chunk_delay_secs;
                                    let chunks = ep.chunks_processed;
                                    view! {
                                        <div class="endpoint-card monitor">
                                            <div class="endpoint-header">
                                                <span class="monitor-badge">{"\u{26A1}"}</span>
                                                <span class="endpoint-alias">{alias}</span>
                                            </div>
                                            <div class="endpoint-metrics">
                                                <span class={if alive { "status-indicator alive" } else { "status-indicator dead" }}>
                                                    {if alive { "Alive" } else { "Dead" }}
                                                </span>
                                                <span class="delay-metric">{format!("{delay:.1}s delay")}</span>
                                                <span class="chunks-metric">{format!("{chunks} chunks")}</span>
                                            </div>
                                        </div>
                                    }
                                }).collect::<Vec<_>>()
                            }}
                        </div>
                    })
                } else {
                    None
                }
            }}
            <div class="endpoint-group">
                <h3 class="group-title">"Delivery Endpoints"</h3>
                {move || {
                    let delivery = store.delivery.get();
                    let delivery_eps: Vec<_> = delivery.endpoints.iter()
                        .filter(|ep| ep.chunk_delay_secs >= 10.0 || !ep.alive)
                        .cloned()
                        .collect();
                    if delivery_eps.is_empty() {
                        view! { <div class="empty-state">"No delivery endpoints"</div> }.into_any()
                    } else {
                        delivery_eps.into_iter().map(|ep| {
                            let is_pending = !ep.alive && ep.chunks_processed == 0 && ep.chunk_delay_secs == 0.0;
                            let card_class = if is_pending {
                                "endpoint-card delivery pending"
                            } else {
                                "endpoint-card delivery"
                            };
                            let delay_class = if is_pending {
                                "delay-metric"
                            } else if ep.chunk_delay_secs < 30.0 {
                                "delay-metric green"
                            } else if ep.chunk_delay_secs < 120.0 {
                                "delay-metric yellow"
                            } else {
                                "delay-metric red"
                            };
                            let status_class = if is_pending {
                                "status-indicator pending"
                            } else if ep.alive {
                                "status-indicator alive"
                            } else if ep.stall_count >= 3 {
                                "status-indicator stalled"
                            } else {
                                "status-indicator dead"
                            };
                            let status_text = if is_pending {
                                "Initializing..."
                            } else if ep.alive {
                                "Alive"
                            } else if ep.stall_count >= 3 {
                                "Stalled"
                            } else {
                                "Dead"
                            };
                            let alias = ep.alias.clone();
                            let delay = ep.chunk_delay_secs;
                            let chunks = ep.chunks_processed;
                            let bytes = ep.bytes_processed_total;
                            let stall_reason = ep.stall_reason.clone();
                            let last_error = ep.last_error.clone();
                            let ffmpeg_restart_count = ep.ffmpeg_restart_count;
                            view! {
                                <div class={card_class}>
                                    <div class="endpoint-header">
                                        <span class="endpoint-alias">{alias}</span>
                                    </div>
                                    <div class="endpoint-metrics">
                                        <span class={status_class}>{status_text}</span>
                                        {if is_pending {
                                            view! {
                                                <span class={delay_class}>{"— delay"}</span>
                                                <span class="chunks-metric">{"—"}</span>
                                                <span class="bytes-metric">{"—"}</span>
                                            }.into_any()
                                        } else {
                                            view! {
                                                <span class={delay_class}>{format!("{delay:.0}s delay")}</span>
                                                <span class="chunks-metric">{format!("{chunks} chunks")}</span>
                                                <span class="bytes-metric">{api::format_bytes(bytes)}</span>
                                            }.into_any()
                                        }}
                                    </div>
                                    {if !is_pending {
                                        stall_reason.map(|reason| {
                                            view! { <div class="stall-info">{format!("Stall: {reason}")}</div> }
                                        })
                                    } else {
                                        None
                                    }}
                                    {if !is_pending {
                                        last_error.map(|err| {
                                            view! { <div class="error-info">{err}</div> }
                                        })
                                    } else {
                                        None
                                    }}
                                    {if !is_pending && ffmpeg_restart_count > 0 {
                                        Some(view! { <div class="restart-info">{format!("ffmpeg restarts: {ffmpeg_restart_count}")}</div> })
                                    } else {
                                        None
                                    }}
                                </div>
                            }
                        }).collect::<Vec<_>>().into_any()
                    }
                }}
            </div>
        </div>
    }
}

/// Real-time activity feed.
#[component]
fn ActivityFeed() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");

    view! {
        <div class="activity-feed">
            <h3 class="section-title">"Activity Feed"</h3>
            <div class="feed-container">
                {move || {
                    let feed = store.activity_feed.get();
                    if feed.is_empty() {
                        view! { <div class="empty-state">"No activity yet"</div> }.into_any()
                    } else {
                        feed.iter().rev().take(50).map(|entry| {
                            let severity_class = format!("activity-entry {}", entry.severity);
                            let ts: String = entry.timestamp.chars().skip(11).take(8).collect();
                            let source = entry.source.clone();
                            let message = entry.message.clone();
                            view! {
                                <div class={severity_class}>
                                    <span class="activity-time">{ts}</span>
                                    <span class="activity-source">{format!("[{source}]")}</span>
                                    <span class="activity-message">{message}</span>
                                </div>
                            }
                        }).collect::<Vec<_>>().into_any()
                    }
                }}
            </div>
        </div>
    }
}

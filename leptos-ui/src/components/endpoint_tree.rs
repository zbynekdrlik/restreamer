//! EndpointTree — branching endpoints from the VPS node.
//!
//! Split out of `operator_dashboard.rs` (#75) to keep that file under the
//! project's 1000-line-per-file cap.

use gloo_timers::callback::Interval;
use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

use super::confirm_modal::ConfirmModal;
use super::endpoint_history::EndpointHistory;
use super::endpoint_remove_confirm_modal::EndpointRemoveConfirmModal;
use crate::api;
use crate::store::{DashboardStore, EndpointLifecycle};
use crate::utils::{cache_threshold_for_service, fast_buffer_class};

#[component]
pub fn EndpointTree() -> impl IntoView {
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
                        match ep_data.get().lifecycle {
                            EndpointLifecycle::Live => "endpoint-node live",
                            EndpointLifecycle::Pending => "endpoint-node pending",
                            EndpointLifecycle::Buffering
                            | EndpointLifecycle::Rescue
                            | EndpointLifecycle::Recovering => "endpoint-node recovering",
                            EndpointLifecycle::Attention => "endpoint-node attention",
                        }
                    };

                    let dot_class = move || {
                        match ep_data.get().lifecycle {
                            EndpointLifecycle::Live => "status-dot active",
                            EndpointLifecycle::Pending => "status-dot",
                            EndpointLifecycle::Buffering
                            | EndpointLifecycle::Rescue
                            | EndpointLifecycle::Recovering => "status-dot recovering",
                            EndpointLifecycle::Attention => "status-dot error",
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
                                            // #296: buffered endpoint rebuilding a
                                            // below-target cushion — protection is
                                            // degraded but actively recovering.
                                            "refilling" => {
                                                ("endpoint-mode-refilling", "REFILLING")
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
                                    let ep = ep_data.get();
                                    if ep.lifecycle == EndpointLifecycle::Attention {
                                        ep.last_error.clone().map(|e| {
                                            let short: String = e.chars().take(60).collect();
                                            let short = if e.chars().count() > 60 {
                                                format!("{short}\u{2026}")
                                            } else {
                                                short
                                            };
                                            view! {
                                                <span class="endpoint-anomaly" title=e>{short}</span>
                                            }
                                        })
                                    } else {
                                        None // survivable states: no scary raw error
                                    }
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
                                        // Fast endpoint UX (#295): the #294 controller RATCHETS the
                                        // read-delay up on a real drain and then HOLDS it for the
                                        // session, anywhere in 5..=120s. So the bar is measured
                                        // against the endpoint's OWN ratcheted target, not the stale
                                        // absolute 8s ceiling that assumed a 2-5s near-live buffer
                                        // and painted a correctly-held 30s buffer permanently red.
                                        // Full green bar = tracking its target = working as designed.
                                        // Falls back to the old absolute bands when the VPS binary
                                        // is too old to report a target.
                                        let secs = ep.chunk_delay_secs;
                                        let target = ep.fast_delay_target_secs
                                            .filter(|t| *t > 0)
                                            .map(|t| t as f64);
                                        let prog = match target {
                                            Some(t) => (secs / t).clamp(0.0, 1.0),
                                            None => (secs / 8.0).clamp(0.0, 1.0),
                                        };
                                        let class_ = match fast_buffer_class(secs, target) {
                                            "critical" => "buffer-bar-fill critical",
                                            "warning" => "buffer-bar-fill warning",
                                            _ => "buffer-bar-fill healthy",
                                        };
                                        // Surface the target so a held buffer reads as
                                        // healthy-by-design rather than an unexplained number.
                                        let label = match target {
                                            Some(t) => format!("{}s target", t as u64),
                                            None => "live".to_string(),
                                        };
                                        (secs, label, prog, class_)
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

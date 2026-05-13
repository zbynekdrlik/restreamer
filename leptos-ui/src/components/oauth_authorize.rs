//! OAuth Device Code Flow channel-authorization UI.
//! - Channels panel: lists `GET /api/v1/youtube/oauths`
//! - Authorize button: opens modal, calls `device-start`, polls `device-status`

use gloo_net::http::Request;
use gloo_timers::future::TimeoutFuture;
use leptos::prelude::*;
use serde::{Deserialize, Serialize};
use wasm_bindgen_futures::spawn_local;

#[derive(Debug, Clone, Deserialize, PartialEq)]
pub struct OAuthRow {
    pub id: i64,
    pub label: String,
    pub channel_id: Option<String>,
    pub connected_at: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DeviceStartBody {
    label: String,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct DeviceStartResp {
    user_code: String,
    verification_url: String,
    #[allow(dead_code)]
    expires_in: i64,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct DeviceStatusResp {
    status: String,
    #[allow(dead_code)]
    user_code: Option<String>,
    #[allow(dead_code)]
    verification_url: Option<String>,
    #[allow(dead_code)]
    channel_id: Option<String>,
    #[allow(dead_code)]
    error: Option<String>,
}

#[component]
pub fn OAuthAuthorize() -> impl IntoView {
    let oauths = RwSignal::new(Vec::<OAuthRow>::new());
    let modal_open = RwSignal::new(false);
    let label_input = RwSignal::new(String::new());
    let pending = RwSignal::new(Option::<DeviceStartResp>::None);
    let status = RwSignal::new(Option::<DeviceStatusResp>::None);
    let error = RwSignal::new(Option::<String>::None);

    // Fetch the current list of authorized channels.
    let refresh = move || {
        spawn_local(async move {
            if let Ok(resp) = Request::get("/api/v1/youtube/oauths").send().await {
                if let Ok(list) = resp.json::<Vec<OAuthRow>>().await {
                    oauths.set(list);
                }
            }
        });
    };
    refresh();

    let start_authorize = move |_| {
        error.set(None);
        let label = label_input.get().trim().to_string();
        let valid = !label.is_empty()
            && label.len() <= 32
            && label
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_');
        if !valid {
            error.set(Some("Label must match [a-z0-9_]{1,32}".into()));
            return;
        }
        spawn_local(async move {
            let body = match serde_json::to_string(&DeviceStartBody {
                label: label.clone(),
            }) {
                Ok(b) => b,
                Err(e) => {
                    error.set(Some(format!("Serialize error: {e}")));
                    return;
                }
            };
            let r = Request::post("/api/v1/youtube/oauth/device-start")
                .header("Content-Type", "application/json")
                .body(body);
            let r = match r {
                Ok(req) => req.send().await,
                Err(e) => {
                    error.set(Some(format!("Request build error: {e}")));
                    return;
                }
            };
            match r {
                Ok(resp) if resp.status() == 200 => {
                    if let Ok(body) = resp.json::<DeviceStartResp>().await {
                        pending.set(Some(body));
                        let l = label.clone();
                        spawn_local(async move {
                            loop {
                                TimeoutFuture::new(3_000).await;
                                let url = format!(
                                    "/api/v1/youtube/oauth/device-status?label={l}"
                                );
                                if let Ok(s) = Request::get(&url).send().await {
                                    if let Ok(b) = s.json::<DeviceStatusResp>().await {
                                        let term = matches!(
                                            b.status.as_str(),
                                            "granted" | "denied" | "expired" | "error"
                                        );
                                        status.set(Some(b));
                                        if term {
                                            break;
                                        }
                                    }
                                }
                            }
                        });
                    }
                }
                Ok(resp) if resp.status() == 409 => {
                    error.set(Some(format!("Label '{label}' is already authorized")));
                }
                Ok(resp) => {
                    error.set(Some(format!("device-start failed: HTTP {}", resp.status())));
                }
                Err(e) => {
                    error.set(Some(format!("device-start failed: {e}")));
                }
            }
        });
    };

    // When status transitions to "granted", close the modal and refresh.
    Effect::new(move |_| {
        if let Some(s) = status.get() {
            if s.status == "granted" {
                modal_open.set(false);
                pending.set(None);
                status.set(None);
                refresh();
            }
        }
    });

    view! {
        <section class="oauth-section">
            <h3>"YouTube channels"</h3>
            <table class="oauth-channels-table">
                <thead>
                    <tr>
                        <th>"Label"</th>
                        <th>"Channel"</th>
                        <th>"Connected"</th>
                    </tr>
                </thead>
                <tbody>
                    {move || {
                        oauths
                            .get()
                            .into_iter()
                            .map(|o| {
                                let label = o.label.clone();
                                view! {
                                    <tr data-testid=format!("oauth-channel-row-{label}")>
                                        <td>{o.label.clone()}</td>
                                        <td>
                                            {o.channel_id
                                                .clone()
                                                .unwrap_or_else(|| "(none)".into())}
                                        </td>
                                        <td>{o.connected_at.clone().unwrap_or_default()}</td>
                                    </tr>
                                }
                            })
                            .collect_view()
                    }}
                </tbody>
            </table>
            <button on:click=move |_| {
                modal_open.set(true);
            }>"Authorize new channel"</button>
            {move || {
                if modal_open.get() {
                    Some(
                        view! {
                            <div class="oauth-modal" data-testid="oauth-modal">
                                {move || {
                                    error
                                        .get()
                                        .map(|e| {
                                            view! { <p class="oauth-error">{e}</p> }
                                        })
                                }}
                                {move || {
                                    match pending.get() {
                                        None => {
                                            view! {
                                                <div>
                                                    <label for="oauth-label-input">
                                                        "Channel label"
                                                    </label>
                                                    <input
                                                        id="oauth-label-input"
                                                        type="text"
                                                        on:input=move |ev| {
                                                            label_input.set(event_target_value(&ev));
                                                        }
                                                    />
                                                    <button on:click=start_authorize>
                                                        "Start authorization"
                                                    </button>
                                                    <button on:click=move |_| {
                                                        modal_open.set(false);
                                                    }>"Cancel"</button>
                                                </div>
                                            }
                                                .into_any()
                                        }
                                        Some(p) => {
                                            view! {
                                                <div>
                                                    <p>"Open this URL on any device:"</p>
                                                    <a
                                                        data-testid="oauth-verification-url"
                                                        href=p.verification_url.clone()
                                                        target="_blank"
                                                    >
                                                        {p.verification_url.clone()}
                                                    </a>
                                                    <p>"Enter this code:"</p>
                                                    <code
                                                        data-testid="oauth-user-code"
                                                        class="oauth-user-code"
                                                    >
                                                        {p.user_code.clone()}
                                                    </code>
                                                    <p class="oauth-status">
                                                        {move || {
                                                            status
                                                                .get()
                                                                .map(|s| s.status)
                                                                .unwrap_or_else(|| "pending".into())
                                                        }}
                                                    </p>
                                                </div>
                                            }
                                                .into_any()
                                        }
                                    }
                                }}
                            </div>
                        },
                    )
                } else {
                    None
                }
            }}
        </section>
    }
}

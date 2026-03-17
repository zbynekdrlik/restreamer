//! Header component with version and WebSocket connection status.

use leptos::prelude::*;

use crate::store::DashboardStore;

/// Header showing app name, version, and WebSocket connection status.
#[component]
pub fn Header() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore not provided");

    view! {
        <header>
            <h1>"Restreamer"</h1>
            <div class="header-right">
                <span class="ws-status">
                    <span class=move || {
                        if store.ws_connected.get() {
                            "status-indicator active"
                        } else {
                            "status-indicator error"
                        }
                    }></span>
                    {move || if store.ws_connected.get() { "Connected" } else { "Reconnecting..." }}
                </span>
                <span class="version">
                    "v" {env!("BUILD_VERSION")} " · Built " {env!("BUILD_TIMESTAMP")}
                </span>
            </div>
        </header>
    }
}

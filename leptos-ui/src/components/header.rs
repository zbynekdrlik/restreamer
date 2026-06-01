//! App header with navigation and connection status.

use leptos::prelude::*;
use leptos_router::components::A;
use leptos_router::hooks::use_location;

use crate::store::DashboardStore;

/// Header showing app name, navigation, and connection status.
#[component]
pub fn Header() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");

    let ws_class = move || {
        if store.ws_connected.get() {
            "ws-indicator active"
        } else {
            "ws-indicator reconnecting"
        }
    };

    let ws_text = move || {
        if store.ws_connected.get() {
            "Connected"
        } else {
            "Reconnecting..."
        }
    };

    let location = use_location();
    let is_settings = move || location.pathname.get().contains("/settings");

    view! {
        <header class="app-header">
            {move || {
                if is_settings() {
                    view! { <A href="/" attr:class="header-nav-btn">{"\u{2190} Dashboard"}</A> }.into_any()
                } else {
                    view! { <A href="/settings" attr:class="header-nav-btn">{"\u{2699} Settings"}</A> }.into_any()
                }
            }}
            <h1 class="app-title">"Restreamer"</h1>
            <span class={ws_class}>{ws_text}</span>
            <span class="version-info" data-testid="version">
                {option_env!("BUILD_VERSION").unwrap_or("dev")}
            </span>
        </header>
    }
}

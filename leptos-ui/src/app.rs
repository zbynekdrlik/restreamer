//! Main application component with router and WebSocket initialization.

use leptos::prelude::*;
use leptos_router::components::{Route, Router, Routes};
use leptos_router::path;

use crate::components::{Header, OperatorDashboard, SettingsView, UploadsView};
use crate::store::DashboardStore;
use crate::ws;

/// Main application component.
#[component]
pub fn App() -> impl IntoView {
    let store = DashboardStore::new();
    provide_context(store);

    // Connect WebSocket (runs once on mount)
    ws::connect_websocket(store);

    view! {
        <Router>
            <div class="app">
                <Header />
                <main class="content">
                    <Routes fallback=|| view! { <div class="empty">"Page not found"</div> }>
                        <Route path=path!("/") view=OperatorDashboard />
                        <Route path=path!("/settings") view=SettingsView />
                        <Route path=path!("/uploads") view=UploadsView />
                    </Routes>
                </main>
            </div>
        </Router>
    }
}

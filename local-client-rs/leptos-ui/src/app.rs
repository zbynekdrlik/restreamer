//! Main application component with router and WebSocket initialization.

use leptos::prelude::*;
use leptos_router::components::{Route, Router, Routes, A};
use leptos_router::path;

use crate::components::{DashboardView, EndpointsView, EventsView, Header, LogsView};
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
                <nav class="nav-bar">
                    <A href="/" class="nav-link">"Dashboard"</A>
                    <A href="/events" class="nav-link">"Events"</A>
                    <A href="/endpoints" class="nav-link">"Endpoints"</A>
                    <A href="/logs" class="nav-link">"Logs"</A>
                </nav>
                <main class="content">
                    <Routes fallback=|| view! { <div class="empty">"Page not found"</div> }>
                        <Route path=path!("/") view=DashboardView />
                        <Route path=path!("/events") view=EventsView />
                        <Route path=path!("/endpoints") view=EndpointsView />
                        <Route path=path!("/logs") view=LogsView />
                    </Routes>
                </main>
            </div>
        </Router>
    }
}

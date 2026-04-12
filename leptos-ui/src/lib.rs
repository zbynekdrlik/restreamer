//! Restreamer Dashboard - Leptos WASM Frontend
//!
//! WebSocket-first reactive dashboard with fine-grained signals.

mod api;
mod app;
mod components;
mod store;
mod utils;
mod ws;

use wasm_bindgen::prelude::*;

/// WASM entry point - mounts the Leptos app to the DOM.
#[wasm_bindgen(start)]
pub fn main() {
    console_error_panic_hook::set_once();
    leptos::mount::mount_to_body(app::App);
}

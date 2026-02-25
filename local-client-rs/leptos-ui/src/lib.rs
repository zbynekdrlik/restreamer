//! Restreamer Dashboard - Leptos WASM Frontend
//!
//! This crate provides the web UI for the Restreamer application,
//! communicating with the Tauri backend via invoke commands.

mod api;
mod app;
mod components;

use wasm_bindgen::prelude::*;

/// WASM entry point - mounts the Leptos app to the DOM.
#[wasm_bindgen(start)]
pub fn main() {
    // Set up panic hook for better error messages in console
    console_error_panic_hook::set_once();

    // Mount the app
    leptos::mount::mount_to_body(app::App);
}

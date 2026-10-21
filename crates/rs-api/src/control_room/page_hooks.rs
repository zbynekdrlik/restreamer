//! Scripts the relay installs in every room page, for later documents
//! (`Page.addScriptToEvaluateOnNewDocument`) and the one already open
//! (`Runtime.evaluate`).

/// Makes YouTube Studio's live preview load its HLS stream in CORS mode
/// (#415). The script is a file of its own, so the frontend E2E runs the
/// very same source in a real browser (`e2e/hls-cors-hook.unit.spec.ts`).
pub fn hls_cors_hook_source() -> &'static str {
    include_str!("hls_cors_hook.js")
}

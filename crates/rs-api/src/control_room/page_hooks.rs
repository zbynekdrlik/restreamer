//! Scripts the relay installs in every room page, for later documents
//! (`Page.addScriptToEvaluateOnNewDocument`) and the one already open
//! (`Runtime.evaluate`).

use super::relay::CLIP_BINDING;

/// The script that reports the page's copies through [`CLIP_BINDING`]: a
/// `copy` event (Ctrl+C, a page's `execCommand('copy')`) and
/// `navigator.clipboard.writeText` (YT Studio's "Copy" buttons). The copy
/// lands in the box Chrome's own clipboard, which the operator never sees.
pub fn copy_hook_source() -> String {
    const HOOK: &str = r#"(() => {
  if (window.__restreamerCopyHooked) return;
  window.__restreamerCopyHooked = true;
  const report = (t) => { try { if (typeof t === 'string' && t) BINDING(t); } catch (_) {} };
  const selected = () => {
    const a = document.activeElement;
    if (a && (a.tagName === 'INPUT' || a.tagName === 'TEXTAREA') && typeof a.selectionStart === 'number') {
      return a.value.slice(a.selectionStart, a.selectionEnd);
    }
    return String(window.getSelection() || '');
  };
  // A page handler may set its own data during the event: read it after the
  // event reached the window; a handler that stops it gets the selection.
  let pending = null;
  document.addEventListener('copy', () => {
    const sel = selected();
    pending = sel;
    setTimeout(() => { if (pending !== null) { pending = null; report(sel); } }, 0);
  }, true);
  window.addEventListener('copy', (e) => {
    const data = e.clipboardData && e.clipboardData.getData('text/plain');
    const sel = pending;
    pending = null;
    report(data || sel || selected());
  });
  const c = navigator.clipboard;
  if (c && c.writeText) {
    const write = c.writeText.bind(c);
    c.writeText = (t) => { report(String(t)); return write(t); };
  }
})()"#;
    HOOK.replace("BINDING", CLIP_BINDING)
}

/// Makes YouTube Studio's live preview load its HLS stream in CORS mode
/// (#415). The script is a file of its own, so the frontend E2E runs the
/// very same source in a real browser (`e2e/hls-cors-hook.unit.spec.ts`).
pub fn hls_cors_hook_source() -> &'static str {
    include_str!("hls_cors_hook.js")
}

//! #437: exact stream states on an endpoint node, plus the operator's live
//! control of a fast endpoint's read-delay target.
//!
//! PP live 25.11.2026: a fast YouTube endpoint froze every ~6 chunks (its
//! backup mirror stayed behind) and later every ~62 s (live-edge skips), and
//! nothing on the dashboard showed it. Now every endpoint node shows:
//! - `preskočené: N` — live-edge skips ahead of the VPS producer (always on a
//!   fast endpoint, so 0 confirms a clean stream; on others only when > 0);
//! - `záloha: OK / zaostáva` — the YouTube backup ingest (when there is one);
//! - on a fast endpoint: the read-delay target, the lag behind live, and a
//!   number input + "Nastaviť" that sets the target live (owner decision
//!   25.11.2026), so an intervention can be brought back to a known state.

use leptos::prelude::*;
use wasm_bindgen_futures::spawn_local;

use crate::api;
use crate::store::{BackupIngest, DashboardStore, DeliveryEndpointState};

/// Show the skips label: always for a fast endpoint, otherwise only when it
/// skipped at least once.
pub fn shows_skips(is_fast: bool, skips: u64) -> bool {
    is_fast || skips > 0
}

/// Text + CSS class of the backup ingest label.
pub fn backup_label(backup: &BackupIngest, current_chunk_id: i64) -> (String, &'static str) {
    if backup.is_behind(current_chunk_id) {
        let dropped = match backup.dropped_chunks {
            0 => String::new(),
            n => format!(" (vypadlo {n})"),
        };
        (format!("záloha: zaostáva{dropped}"), "endpoint-anomaly")
    } else {
        ("záloha: OK".to_string(), "endpoint-state-ok")
    }
}

/// The operator's input as a target, `None` when it is not a whole number of
/// seconds in 5..=120 (the VPS clamps too; this keeps a typo from moving it).
pub fn parse_target(input: &str) -> Option<u64> {
    input
        .trim()
        .parse::<u64>()
        .ok()
        .filter(|s| (5..=120).contains(s))
}

/// #437: how far (seconds) the lag must exceed the target before the VPS
/// producer skips ahead: `producer_lag::FAST_JUMP_MIN_CHUNKS` (3) chunks of
/// the chunker's 2 s (`is_real_lag`). Less than that is jitter, never skipped.
pub const FAST_JUMP_MIN_SECS: f64 = 3.0 * 2.0;

/// The operator's confirmation. A target above the current lag never adds
/// buffer by itself (the push is always real time, the producer never waits):
/// it stops the skips ahead, and the lag grows to it only through an outage.
/// A target at least [`FAST_JUMP_MIN_SECS`] below the lag skips ahead to it
/// on the next probe (~1 min); closer than that nothing is skipped.
pub fn set_result_text(from_secs: u64, to_secs: u64, lag_secs: f64) -> String {
    let to = to_secs as f64;
    let what = if to > lag_secs {
        "nad oneskorením: nič sa nepreskočí, oneskorenie narastie až pri výpadku"
    } else if lag_secs - to >= FAST_JUMP_MIN_SECS {
        "pod oneskorením: do minúty preskočí bližšie k živému"
    } else {
        "blízko oneskorenia: nič sa nepreskočí, oneskorenie ostane"
    };
    format!("nastavené: {from_secs} s → {to_secs} s ({what})")
}

#[component]
pub fn EndpointStreamState(ep_data: Memo<DeliveryEndpointState>) -> impl IntoView {
    view! {
        {move || {
            let ep = ep_data.get();
            shows_skips(ep.is_fast, ep.skips_ahead).then(|| {
                let class = if ep.skips_ahead > 0 { "endpoint-anomaly" } else { "endpoint-state-ok" };
                view! {
                    <span class=class data-testid="endpoint-skips">
                        {format!("preskočené: {}", ep.skips_ahead)}
                    </span>
                }
            })
        }}
        {move || {
            let ep = ep_data.get();
            ep.backup_ingest.as_ref().map(|b| {
                let (text, class) = backup_label(b, ep.current_chunk_id);
                view! { <span class=class data-testid="endpoint-backup">{text}</span> }
            })
        }}
    }
}

#[component]
pub fn FastDelayControl(alias: String, ep_data: Memo<DeliveryEndpointState>) -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore");
    let input = RwSignal::new(String::new());
    let result: RwSignal<Option<(bool, String)>> = RwSignal::new(None);
    let busy = RwSignal::new(false);
    // The live delivery's event_id arrives over the WebSocket, which can land
    // after the endpoint nodes (HTTP) are drawn; fall back to the selected
    // event, the same source the change-key action uses. "Nastaviť" stays
    // disabled until one is known, so a click never races the WS state.
    let event_id = Memo::new(move |_| {
        store
            .pipeline_state
            .get()
            .event_id
            .or_else(|| store.selected_event_id.get())
    });
    let alias = StoredValue::new(alias);

    let submit = move |_| {
        let Some(target) = parse_target(&input.get_untracked()) else {
            result.set(Some((false, "zadaj 5 až 120 s".to_string())));
            return;
        };
        let Some(event_id) = event_id.get_untracked() else {
            result.set(Some((false, "žiadna aktívna udalosť".to_string())));
            return;
        };
        let lag_secs = ep_data.get_untracked().chunk_delay_secs;
        let alias = alias.get_value();
        busy.set(true);
        spawn_local(async move {
            let outcome = api::delivery_set_fast_delay(event_id, &alias, target).await;
            let _ = busy.try_set(false);
            let _ = result.try_set(Some(match outcome {
                Ok(set) => (true, set_result_text(set.from_secs, set.to_secs, lag_secs)),
                Err(e) => (false, format!("nenastavené: {e}")),
            }));
        });
    };

    view! {
        <div class="fast-delay-control" data-testid="fast-delay-control">
            <span class="endpoint-state-ok" data-testid="fast-delay-state">
                {move || {
                    let ep = ep_data.get();
                    let target = ep
                        .fast_delay_target_secs
                        .map_or("-".to_string(), |t| format!("{t} s"));
                    format!("cieľ: {target} · za živým: {} s", ep.chunk_delay_secs as u64)
                }}
            </span>
            <input
                type="number"
                min="5"
                max="120"
                class="fast-delay-input"
                data-testid="fast-delay-input"
                placeholder="s"
                prop:value=move || input.get()
                on:input=move |ev| input.set(event_target_value(&ev))
            />
            <button
                class="btn-change-key"
                data-testid="fast-delay-set"
                prop:disabled=move || busy.get() || event_id.get().is_none()
                on:click=submit
            >
                "Nastaviť"
            </button>
            {move || {
                result.get().map(|(ok, text)| {
                    let class = if ok { "endpoint-state-ok" } else { "endpoint-anomaly" };
                    view! { <span class=class data-testid="fast-delay-result">{text}</span> }
                })
            }}
        </div>
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn backup(connected: bool, last: Option<i64>, dropped: u64) -> BackupIngest {
        BackupIngest {
            connected,
            chunks_pushed: 10,
            dropped_chunks: dropped,
            last_chunk_id: last,
        }
    }

    #[test]
    fn the_skips_label_shows_on_fast_endpoints_and_on_any_skip() {
        assert!(shows_skips(true, 0));
        assert!(shows_skips(false, 1));
        assert!(!shows_skips(false, 0));
    }

    #[test]
    fn the_backup_label_reads_ok_or_behind_with_its_drops() {
        assert_eq!(
            backup_label(&backup(true, Some(101), 0), 100),
            ("záloha: OK".to_string(), "endpoint-state-ok")
        );
        assert_eq!(
            backup_label(&backup(true, Some(90), 75), 100),
            (
                "záloha: zaostáva (vypadlo 75)".to_string(),
                "endpoint-anomaly"
            )
        );
        assert_eq!(
            backup_label(&backup(false, Some(100), 0), 100).0,
            "záloha: zaostáva"
        );
    }

    #[test]
    fn the_confirmation_says_what_the_new_target_does() {
        assert_eq!(
            set_result_text(9, 30, 28.0),
            "nastavené: 9 s → 30 s (nad oneskorením: nič sa nepreskočí, \
             oneskorenie narastie až pri výpadku)"
        );
        assert_eq!(
            set_result_text(30, 9, 28.0),
            "nastavené: 30 s → 9 s (pod oneskorením: do minúty preskočí bližšie k živému)"
        );
        // 25 s against a 28 s lag: 3 s below, less than the 3-chunk skip
        // threshold, so the producer never skips (review of #437).
        assert_eq!(
            set_result_text(9, 25, 28.0),
            "nastavené: 9 s → 25 s (blízko oneskorenia: nič sa nepreskočí, oneskorenie ostane)"
        );
        assert!(
            set_result_text(9, 28, 28.0).contains("blízko oneskorenia"),
            "equal is not above, and no skip"
        );
        assert!(
            set_result_text(9, 22, 28.0).contains("pod oneskorením"),
            "exactly the threshold below skips"
        );
        assert!(set_result_text(9, 23, 28.0).contains("blízko oneskorenia"));
        assert_eq!(FAST_JUMP_MIN_SECS, 6.0);
    }

    #[test]
    fn the_operator_target_must_be_5_to_120_whole_seconds() {
        assert_eq!(parse_target(" 30 "), Some(30));
        assert_eq!(parse_target("5"), Some(5));
        assert_eq!(parse_target("120"), Some(120));
        assert_eq!(parse_target("4"), None);
        assert_eq!(parse_target("121"), None);
        assert_eq!(parse_target("abc"), None);
        assert_eq!(parse_target(""), None);
    }

    #[test]
    fn the_store_backup_rule_matches_the_hosts() {
        assert!(!backup(true, Some(98), 0).is_behind(100));
        assert!(backup(true, Some(97), 0).is_behind(100));
        assert!(backup(true, None, 0).is_behind(100));
    }
}

//! Dedicated banner for orphaned delivery VPS still billing (#352).
//!
//! Every teardown path is keyed on a `delivery_instances` DB row; when that row
//! is lost (DB reset/reinstall, a crash in the create window, a forced kill
//! mid-stop) the Hetzner VPS bills forever, invisible to the app. The runtime
//! orphan reaper (`reconcile_orphan_vps`) finds these by listing Hetzner and
//! reconciling against the DB, and publishes the still-billing count onto
//! `/api/v1/status`. This banner is the loud dashboard-level signal so an
//! operator sees a money leak while it lasts (and knows to check the audit feed
//! for the `vps_orphan_detected` rows, which carry the ids).
//!
//! Driven by `store.vps_orphan_count`, refreshed every 2s from the
//! `/api/v1/status` poll.

use crate::store::DashboardStore;
use leptos::prelude::*;

#[component]
pub fn VpsOrphanBanner() -> impl IntoView {
    let store = use_context::<DashboardStore>().expect("DashboardStore in context");
    let count = store.vps_orphan_count;

    let show = Memo::new(move |_| count.get() > 0);

    view! {
        <Show when=move || show.get()>
            <div class="banner banner--warn" role="alert" data-testid="vps-orphan-banner">
                {move || format!(
                    "\u{1F4B8} {} orphaned delivery VPS still billing on Hetzner with no active \
                     delivery. The app is reconciling and will auto-delete them after the grace \
                     period; check the activity feed (vps_orphan_detected) for the server ids.",
                    count.get()
                )}
            </div>
        </Show>
    }
}

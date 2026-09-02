//! Deny-by-default credential redaction for the config JSON the API hands out
//! (#336).
//!
//! History: the config-GET handler used to mask a hardcoded LIST of fields.
//! The list failed twice — `youtube.device_flow.client_secret` and
//! `notifications.discord_bot_token` were added to [`crate::config`] long after
//! the list was written and nobody extended it, so both were served in
//! plaintext on an internet-reachable endpoint.
//!
//! So the rule is inverted here: a field is a credential **unless proven
//! otherwise**. Anything whose name carries a [`SECRET_MARKERS`] substring is
//! masked; only the explicitly listed [`READABLE_PATHS`] (a key *name*, TLS
//! file *paths*) stay readable. A credential field added to `Config` tomorrow
//! is masked with no edit here.
//!
//! [`restore_redacted`] is the symmetric half: a client that reads the masked
//! config and PATCHes it back verbatim sends [`REDACTED`] for those fields, and
//! that must restore the stored credential instead of overwriting it with the
//! mask.

use serde_json::Value;

/// The mask the dashboard already renders for s3/hetzner/obs credentials.
/// Kept byte-identical — the UI and the PATCH round-trip both key on it.
pub const REDACTED: &str = "***";

/// Case-insensitive substrings that mark a config field name as a credential.
///
/// Deliberately wider than the obvious four words:
/// - `webhook` — a Discord webhook URL is a bearer credential in its own right
///   (anyone holding it can post as the integration), though its name carries
///   no "secret"/"token" word.
/// - `auth` / `credential` / `bearer` / `signature` / `salt` / `private` /
///   `cookie` / `session` / `pwd` — the plausible names of the NEXT credential
///   added here. A false positive is cheap (add the path to
///   [`READABLE_PATHS`]); a false negative is the bug this module exists for.
///
/// The marker list is a heuristic, not the guarantee. The guarantee is the
/// `config_inventory_is_fully_classified` test below: any new `Config` field,
/// credential-named or not, fails CI until someone classifies it.
const SECRET_MARKERS: &[&str] = &[
    "secret",
    "token",
    "key",
    "password",
    "passphrase",
    "webhook",
    "auth",
    "credential",
    "bearer",
    "signature",
    "salt",
    "private",
    "cookie",
    "session",
    "pwd",
];

/// Dotted paths that MATCH a marker but are not credentials, so they stay
/// readable in the API response. Keep this list as short as the UI/CI allow.
///
/// - `hetzner.ssh_key_name` / `hetzner.extra_ssh_key_names` — *names* of keys
///   registered in Hetzner Cloud, not key material.
/// - `api.tls_key` — a file *path* (`key.pem`), resolved against the config
///   directory. If it is ever changed to hold inline PEM material, DELETE it
///   from this list — the material would then be a credential.
/// - `api.tls_cert` — also a path (`cert.pem`). It matches no marker today, so
///   the exemption is inert; it is listed so the cert/key pair stays together
///   and a future `cert`/`pem` marker cannot silently mask a path.
const READABLE_PATHS: &[&str] = &[
    "hetzner.ssh_key_name",
    "hetzner.extra_ssh_key_names",
    "api.tls_cert",
    "api.tls_key",
];

/// True when the field at `path` (whose leaf name is `key`) holds a credential.
fn is_secret_field(path: &str, key: &str) -> bool {
    if READABLE_PATHS.contains(&path) {
        return false;
    }
    let lower = key.to_ascii_lowercase();
    SECRET_MARKERS.iter().any(|marker| lower.contains(marker))
}

/// Mask every credential in a serialized `Config` in place.
///
/// Only NON-EMPTY string leaves are replaced:
/// - numbers, bools and `null` carry no credential material, and masking them
///   would break the typed round-trip (callers deserialize this response back
///   into `Config`); a `null` staying `null` keeps "unset" honest in the UI;
/// - an EMPTY string is left empty for the same reason — a zero-length value
///   discloses nothing, and masking it to `***` would make an unconfigured
///   mechanism (no Discord token, no OBS password) look configured on the
///   settings screen, which is exactly the question an operator opens it to
///   answer.
pub fn redact_secrets(value: &mut Value) {
    redact_at("", value);
}

fn redact_at(path: &str, value: &mut Value) {
    match value {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                let child_path = join_path(path, key);
                if is_secret_field(&child_path, key) {
                    mask_in_place(child);
                } else {
                    redact_at(&child_path, child);
                }
            }
        }
        Value::Array(items) => {
            // Array elements share their container's path.
            for item in items.iter_mut() {
                redact_at(path, item);
            }
        }
        _ => {}
    }
}

/// Mask every string leaf beneath a field already known to be a credential —
/// including inside a nested object or array, so a future `secrets: { .. }`
/// sub-struct cannot slip through.
fn mask_in_place(value: &mut Value) {
    match value {
        Value::String(s) if !s.is_empty() => *s = REDACTED.to_string(),
        Value::Array(items) => items.iter_mut().for_each(mask_in_place),
        Value::Object(map) => map.values_mut().for_each(mask_in_place),
        _ => {}
    }
}

/// Restore credentials a client echoed back as [`REDACTED`].
///
/// `patched` is the incoming config merged over the stored one; `current` is
/// the stored config as JSON. Wherever a credential field in `patched` carries
/// the mask, the stored value is put back. A field carrying a genuinely new
/// value is left alone, so rotating a credential through the API still works.
///
/// RECURSION BOUND: `patched` is attacker-influenced (`merge_json` embeds the
/// request subtree verbatim once it hits its own depth limit), but the walk
/// descends ONLY where `current.get(..)` yields a value — and `current` must
/// always be a serialized `Config`, which is shallow. Depth is therefore bound
/// by the STORED config's shape, not by the request. Do not add an
/// else-recurse-anyway branch, and never call this with an attacker-controlled
/// `current`, or that bound is gone.
pub fn restore_redacted(patched: &mut Value, current: &Value) {
    restore_at("", patched, current);
}

fn restore_at(path: &str, patched: &mut Value, current: &Value) {
    match patched {
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                let child_path = join_path(path, key);
                let Some(current_child) = current.get(key.as_str()) else {
                    continue;
                };
                if is_secret_field(&child_path, key) {
                    restore_masked_leaves(child, current_child);
                } else {
                    restore_at(&child_path, child, current_child);
                }
            }
        }
        // Mirrors `redact_at`'s array arm. Without it, a credential nested in
        // an array of objects would be masked on the way out and then written
        // back as `***` on a verbatim round-trip — silent credential
        // destruction. Elements are paired positionally with the stored array.
        Value::Array(items) => {
            for (index, item) in items.iter_mut().enumerate() {
                if let Some(current_item) = current.get(index) {
                    restore_at(path, item, current_item);
                }
            }
        }
        _ => {}
    }
}

/// Replace masked leaves under a credential field with the stored value.
fn restore_masked_leaves(patched: &mut Value, current: &Value) {
    if patched.as_str() == Some(REDACTED) {
        *patched = current.clone();
        return;
    }
    match patched {
        // A fully-masked array carries no information — restore it whole.
        Value::Array(items) => {
            if !items.is_empty() && items.iter().all(|i| i.as_str() == Some(REDACTED)) {
                *patched = current.clone();
            }
        }
        Value::Object(map) => {
            for (key, child) in map.iter_mut() {
                if let Some(current_child) = current.get(key.as_str()) {
                    restore_masked_leaves(child, current_child);
                }
            }
        }
        _ => {}
    }
}

fn join_path(parent: &str, key: &str) -> String {
    if parent.is_empty() {
        key.to_string()
    } else {
        format!("{parent}.{key}")
    }
}

/// Config subtrees the API may never modify, whatever a client PATCHes.
///
/// `api.access` governs who is allowed to reach the API at all (#273). Leaving
/// it writable through `PATCH /api/v1/config` would mean the door can be
/// unlocked through the door it guards: one request could persist
/// `mode = "log_only"`, or — far worse — repoint `team_domain` at an
/// attacker-controlled Zero Trust org, after which the app would fetch THEIR
/// signing keys and accept THEIR tokens from the internet on the next restart.
/// That would turn momentary LAN presence into permanent remote access.
///
/// Changing these values is a deliberate act on the box: edit
/// `C:\ProgramData\Restreamer\config.json` and restart. That is exactly the
/// property the design depends on, so it is enforced here rather than
/// described in a comment.
const IMMUTABLE_PATHS: &[&[&str]] = &[&["api", "access"]];

/// Everything `PATCH /api/v1/config` must do to an incoming merged config
/// before it is deserialized and saved.
///
/// 1. [`restore_redacted`] — a client that read the masked config and sent it
///    back must not overwrite real credentials with `***` (#336).
/// 2. [`preserve_immutable`] — the access-control settings are put back
///    verbatim from the stored config (#273).
///
/// Call THIS from the handler, not the two halves separately, so a future
/// immutable path cannot be forgotten at one call site.
pub fn sanitize_patch(patched: &mut Value, current: &Value) {
    restore_redacted(patched, current);
    preserve_immutable(patched, current);
}

/// Overwrite every [`IMMUTABLE_PATHS`] subtree in `patched` with the stored
/// value (or delete it when the stored config has none).
fn preserve_immutable(patched: &mut Value, current: &Value) {
    for path in IMMUTABLE_PATHS {
        let stored = lookup(current, path).cloned();
        let changed = lookup(patched, path) != stored.as_ref();
        if changed {
            // Deliberately loud: an attempt to rewrite the access gate through
            // the gated API is exactly what a reader of these logs is looking
            // for. The value is not a credential, so logging it is safe.
            tracing::warn!(
                "config PATCH tried to change the immutable {} subtree — ignored (#273)",
                path.join(".")
            );
        }
        apply(patched, path, stored);
    }
}

fn lookup<'v>(value: &'v Value, path: &[&str]) -> Option<&'v Value> {
    let mut node = value;
    for segment in path {
        node = node.get(*segment)?;
    }
    Some(node)
}

/// Set (or, with `None`, remove) `path` in `target`, creating intermediate
/// objects as needed. A non-object on the way is replaced — the caller is
/// restoring a known-good subtree, so a client that sent `"api": 5` must not
/// be able to keep the gate's settings out of the result.
fn apply(target: &mut Value, path: &[&str], value: Option<Value>) {
    let Some((leaf, parents)) = path.split_last() else {
        return;
    };
    let mut node = target;
    for segment in parents {
        if !node.is_object() {
            *node = Value::Object(serde_json::Map::new());
        }
        node = node
            .as_object_mut()
            .expect("just ensured object")
            .entry((*segment).to_string())
            .or_insert(Value::Object(serde_json::Map::new()));
    }
    if !node.is_object() {
        *node = Value::Object(serde_json::Map::new());
    }
    let map = node.as_object_mut().expect("just ensured object");
    match value {
        Some(v) => {
            map.insert((*leaf).to_string(), v);
        }
        None => {
            map.remove(*leaf);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn sample() -> Value {
        json!({
            "client_uuid": "uuid-1",
            "s3": {
                "bucket": "b",
                "access_key_id": "fake-access",
                "secret_access_key": "fake-secret"
            },
            "hetzner": {
                "api_token": "fake-hetzner",
                "location": "fsn1",
                "ssh_key_name": "restreamer",
                "extra_ssh_key_names": ["debug"]
            },
            "youtube": {
                "client_id": "id",
                "client_secret": "fake-yt",
                "device_flow": { "client_secret": "fake-device", "daily_quota": 10000 }
            },
            "notifications": {
                "discord_bot_token": "fake-bot",
                "discord_webhook_url": "https://discord.test/hook",
                "discord_channel_id": "123"
            },
            "api": {
                "tls_cert": "cert.pem",
                "tls_key": "key.pem",
                // NOT a `Config` field any more (`api.diag_token` was deleted
                // with #273 — the Access design stores no secret on the box).
                // Kept in this fixture on purpose: it proves the walker masks a
                // credential-named field it has never heard of, which is the
                // deny-by-default promise this module exists for.
                "diag_token": "fake-diag",
                "port": 8910
            },
            "obs": { "ws_url": "ws://x", "ws_password": "fake-obs" },
            // Not a real Config shape — guards the array arms of both walkers
            // against a future `Vec<SomeStruct>` config field.
            "vps_pool": [{ "api_token": "fake-vps-a" }, { "api_token": "fake-vps-b" }]
        })
    }

    #[test]
    fn masks_every_marker_matching_field() {
        let mut v = sample();
        redact_secrets(&mut v);
        assert_eq!(v["s3"]["access_key_id"], REDACTED);
        assert_eq!(v["s3"]["secret_access_key"], REDACTED);
        assert_eq!(v["hetzner"]["api_token"], REDACTED);
        assert_eq!(v["youtube"]["client_secret"], REDACTED);
        assert_eq!(v["youtube"]["device_flow"]["client_secret"], REDACTED);
        assert_eq!(v["notifications"]["discord_bot_token"], REDACTED);
        assert_eq!(v["notifications"]["discord_webhook_url"], REDACTED);
        assert_eq!(v["api"]["diag_token"], REDACTED);
        assert_eq!(v["obs"]["ws_password"], REDACTED);
        // Inside an array of objects too.
        assert_eq!(v["vps_pool"][0]["api_token"], REDACTED);
        assert_eq!(v["vps_pool"][1]["api_token"], REDACTED);
    }

    #[test]
    fn leaves_an_empty_credential_empty_so_unset_stays_visible() {
        let mut v = json!({ "notifications": { "discord_bot_token": "" } });
        redact_secrets(&mut v);
        assert_eq!(v["notifications"]["discord_bot_token"], "");
    }

    #[test]
    fn keeps_readable_paths_and_plain_fields_intact() {
        let mut v = sample();
        redact_secrets(&mut v);
        assert_eq!(v["hetzner"]["ssh_key_name"], "restreamer");
        assert_eq!(v["hetzner"]["extra_ssh_key_names"][0], "debug");
        assert_eq!(v["api"]["tls_cert"], "cert.pem");
        assert_eq!(v["api"]["tls_key"], "key.pem");
        assert_eq!(v["client_uuid"], "uuid-1");
        assert_eq!(v["s3"]["bucket"], "b");
        assert_eq!(v["notifications"]["discord_channel_id"], "123");
        assert_eq!(v["obs"]["ws_url"], "ws://x");
    }

    #[test]
    fn masks_a_credential_field_added_without_touching_this_module() {
        // The whole point of deny-by-default: a field nobody listed anywhere.
        let mut v = json!({ "future": { "brand_new_api_key": "fake-new" } });
        redact_secrets(&mut v);
        assert_eq!(v["future"]["brand_new_api_key"], REDACTED);
    }

    #[test]
    fn masks_strings_nested_under_a_credential_key() {
        let mut v = json!({ "secrets": { "a": "fake-a", "b": ["fake-b"], "n": 5 } });
        redact_secrets(&mut v);
        assert_eq!(v["secrets"]["a"], REDACTED);
        assert_eq!(v["secrets"]["b"][0], REDACTED);
        // Numbers are left alone so the typed round-trip still works.
        assert_eq!(v["secrets"]["n"], 5);
    }

    #[test]
    fn leaves_null_and_non_string_scalars_alone() {
        let mut v = json!({ "api": { "diag_token": null, "port": 8910, "tls": false } });
        redact_secrets(&mut v);
        assert!(v["api"]["diag_token"].is_null());
        assert_eq!(v["api"]["port"], 8910);
        assert_eq!(v["api"]["tls"], false);
    }

    #[test]
    fn matches_marker_case_insensitively() {
        let mut v = json!({ "svc": { "API_TOKEN": "fake", "Client_Secret": "fake" } });
        redact_secrets(&mut v);
        assert_eq!(v["svc"]["API_TOKEN"], REDACTED);
        assert_eq!(v["svc"]["Client_Secret"], REDACTED);
    }

    #[test]
    fn restores_echoed_masks_from_the_stored_config() {
        let current = sample();
        let mut patched = current.clone();
        patched["youtube"]["device_flow"]["client_secret"] = json!(REDACTED);
        patched["notifications"]["discord_bot_token"] = json!(REDACTED);
        patched["api"]["diag_token"] = json!(REDACTED);
        patched["s3"]["access_key_id"] = json!(REDACTED);

        restore_redacted(&mut patched, &current);

        assert_eq!(
            patched["youtube"]["device_flow"]["client_secret"],
            "fake-device"
        );
        assert_eq!(patched["notifications"]["discord_bot_token"], "fake-bot");
        assert_eq!(patched["api"]["diag_token"], "fake-diag");
        assert_eq!(patched["s3"]["access_key_id"], "fake-access");
    }

    #[test]
    fn restore_leaves_a_genuinely_new_credential_in_place() {
        let current = sample();
        let mut patched = current.clone();
        patched["notifications"]["discord_bot_token"] = json!("fake-rotated");
        restore_redacted(&mut patched, &current);
        assert_eq!(
            patched["notifications"]["discord_bot_token"],
            "fake-rotated"
        );
    }

    #[test]
    fn restore_ignores_a_mask_typed_into_a_non_credential_field() {
        let current = sample();
        let mut patched = current.clone();
        patched["obs"]["ws_url"] = json!(REDACTED);
        restore_redacted(&mut patched, &current);
        assert_eq!(patched["obs"]["ws_url"], REDACTED);
    }

    #[test]
    fn restore_tolerates_a_field_absent_from_the_stored_config() {
        let current = json!({ "api": { "port": 8910 } });
        let mut patched = json!({ "api": { "port": 8910, "diag_token": REDACTED } });
        restore_redacted(&mut patched, &current);
        // Nothing stored to restore — the mask stays, and nothing panics.
        assert_eq!(patched["api"]["diag_token"], REDACTED);
    }

    #[test]
    fn restores_masks_echoed_from_inside_an_array_of_objects() {
        let current = sample();
        let mut patched = current.clone();
        patched["vps_pool"][0]["api_token"] = json!(REDACTED);
        restore_redacted(&mut patched, &current);
        assert_eq!(patched["vps_pool"][0]["api_token"], "fake-vps-a");
    }

    #[test]
    fn redact_then_restore_round_trips_to_the_original() {
        let current = sample();
        let mut public = current.clone();
        redact_secrets(&mut public);
        // The client sends the masked config straight back.
        let mut patched = public;
        restore_redacted(&mut patched, &current);
        assert_eq!(patched, current);
    }

    // ---------------------------------------------------------------------
    // The actual guarantee behind "the next credential cannot leak".
    //
    // The marker list above is a heuristic and will miss a name nobody
    // predicted (`ingest_url` carrying a stream key, `svc_creds`, …). This
    // inventory pins EVERY leaf of the real `Config` to an explicit
    // classification, so adding ANY field — credential-named or not — fails
    // this test until a human classifies it. That is what makes the module's
    // promise real rather than aspirational.
    // ---------------------------------------------------------------------

    /// Every serialized leaf path of `Config`, with `true` = must be masked in
    /// the public response. When this test fails because you added a config
    /// field: classify it here. Mask it unless you can argue it is NOT a
    /// credential (a name, an id, a path, a port, a flag).
    const CONFIG_INVENTORY: &[(&str, bool)] = &[
        ("api.access.aud", false),
        ("api.access.mode", false),
        ("api.access.team_domain", false),
        ("api.bind", false),
        ("api.https_domain", false),
        ("api.https_port", false),
        ("api.port", false),
        ("api.tls", false),
        ("api.tls_cert", false),
        ("api.tls_key", false),
        ("client_uuid", false),
        ("delivery.delivery_delay_secs", false),
        ("hetzner.api_token", true),
        ("hetzner.default_server_type", false),
        ("hetzner.extra_ssh_key_names", false),
        ("hetzner.location", false),
        ("hetzner.snapshot_label", false),
        ("hetzner.ssh_key_name", false),
        ("inpoint.chunk_duration_ms", false),
        ("inpoint.chunk_format", false),
        ("inpoint.read_buffer_bytes", false),
        ("inpoint.skew_threshold_ms", false),
        ("inpoint.rtmp_bind", false),
        ("inpoint.rtmp_port", false),
        ("notifications.discord_bot_token", true),
        ("notifications.discord_channel_id", false),
        ("notifications.discord_webhook_url", true),
        ("obs.enabled", false),
        ("obs.ws_password", true),
        ("obs.ws_url", false),
        ("s3.access_key_id", true),
        ("s3.bucket", false),
        ("s3.endpoint", false),
        ("s3.region", false),
        ("s3.secret_access_key", true),
        ("youtube.client_id", false),
        ("youtube.client_secret", true),
        ("youtube.device_flow.client_id", false),
        ("youtube.device_flow.client_secret", true),
        ("youtube.device_flow.daily_quota", false),
    ];

    /// Collect every leaf path of a serialized value, in sorted order.
    fn leaf_paths(value: &Value) -> Vec<String> {
        let mut out = Vec::new();
        collect_leaf_paths("", value, &mut out);
        out.sort();
        out
    }

    fn collect_leaf_paths(path: &str, value: &Value, out: &mut Vec<String>) {
        match value {
            Value::Object(map) if !map.is_empty() => {
                for (key, child) in map {
                    collect_leaf_paths(&join_path(path, key), child, out);
                }
            }
            // An empty object, an array (of any length) and every scalar are
            // leaves for classification purposes.
            _ => out.push(path.to_string()),
        }
    }

    #[test]
    fn config_inventory_is_fully_classified() {
        let actual = leaf_paths(&serde_json::to_value(crate::config::Config::default()).unwrap());
        let expected: Vec<String> = {
            let mut v: Vec<String> = CONFIG_INVENTORY
                .iter()
                .map(|(p, _)| p.to_string())
                .collect();
            v.sort();
            v
        };
        let unclassified: Vec<&String> = actual.iter().filter(|p| !expected.contains(p)).collect();
        let stale: Vec<&String> = expected.iter().filter(|p| !actual.contains(p)).collect();
        assert!(
            unclassified.is_empty(),
            "new/renamed config field(s) not classified in CONFIG_INVENTORY: {unclassified:?} — \
             add them (mask unless provably not a credential), see #336"
        );
        assert!(
            stale.is_empty(),
            "CONFIG_INVENTORY lists field(s) that no longer exist: {stale:?}"
        );
    }

    #[test]
    fn redaction_matches_the_classification_field_for_field() {
        // A config with every string field populated, so a "must be masked"
        // field has something to mask. Based on `for_testing()` rather than
        // `default()` so clippy's `field_reassign_with_default` stays quiet.
        let mut config = crate::config::Config::for_testing();
        config.client_uuid = "uuid".into();
        config.s3.access_key_id = "fake-a".into();
        config.s3.secret_access_key = "fake-b".into();
        config.hetzner.api_token = "fake-c".into();
        config.hetzner.extra_ssh_key_names = vec!["extra".into()];
        config.youtube.client_id = "id".into();
        config.youtube.client_secret = "fake-d".into();
        config.youtube.device_flow.client_id = "id2".into();
        config.youtube.device_flow.client_secret = "fake-e".into();
        config.notifications.discord_bot_token = "fake-f".into();
        config.notifications.discord_webhook_url = "https://discord.test/h".into();
        config.notifications.discord_channel_id = "123".into();
        config.obs.ws_password = "fake-g".into();
        config.api.https_domain = Some("example.test".into());

        let plain = serde_json::to_value(&config).unwrap();
        let mut masked = plain.clone();
        redact_secrets(&mut masked);

        for (path, should_mask) in CONFIG_INVENTORY {
            let before = plain.pointer(&pointer_of(path)).unwrap();
            let after = masked.pointer(&pointer_of(path)).unwrap();
            if *should_mask {
                assert_eq!(
                    after, REDACTED,
                    "{path} is classified as a credential but was NOT masked"
                );
            } else {
                assert_eq!(
                    after, before,
                    "{path} is classified as readable but was masked"
                );
            }
        }
    }

    // -----------------------------------------------------------------
    // The access gate must not be rewritable through the gated API (#273).
    // -----------------------------------------------------------------

    fn stored_with_access() -> Value {
        json!({
            "api": {
                "port": 8910,
                "access": {
                    "mode": "enforce",
                    "team_domain": "example.cloudflareaccess.com",
                    "aud": ["aud-one"]
                }
            }
        })
    }

    #[test]
    fn a_patch_cannot_switch_the_gate_to_log_only() {
        let current = stored_with_access();
        let mut patched = current.clone();
        patched["api"]["access"]["mode"] = json!("log_only");
        sanitize_patch(&mut patched, &current);
        assert_eq!(
            patched["api"]["access"]["mode"], "enforce",
            "one unauthenticated LAN request must not be able to disable the gate"
        );
    }

    #[test]
    fn a_patch_cannot_repoint_the_team_domain_at_an_attacker() {
        // The worst case: the app would fetch the attacker's JWKS on restart
        // and accept tokens they mint.
        let current = stored_with_access();
        let mut patched = current.clone();
        patched["api"]["access"]["team_domain"] = json!("attacker.cloudflareaccess.com");
        patched["api"]["access"]["aud"] = json!(["attacker-aud"]);
        sanitize_patch(&mut patched, &current);
        assert_eq!(patched["api"]["access"], current["api"]["access"]);
    }

    #[test]
    fn a_patch_cannot_delete_the_access_subtree_to_fall_back_to_defaults() {
        let current = stored_with_access();
        let mut patched = current.clone();
        patched["api"].as_object_mut().unwrap().remove("access");
        sanitize_patch(&mut patched, &current);
        assert_eq!(patched["api"]["access"], current["api"]["access"]);
    }

    #[test]
    fn a_patch_cannot_smuggle_the_gate_out_by_retyping_its_parent() {
        // "api": 5 would leave nothing to merge into; the restore must still
        // put a well-formed access subtree back.
        let current = stored_with_access();
        let mut patched = json!({ "api": 5 });
        sanitize_patch(&mut patched, &current);
        assert_eq!(patched["api"]["access"], current["api"]["access"]);
    }

    #[test]
    fn a_patch_that_leaves_the_gate_alone_still_applies_everything_else() {
        let current = stored_with_access();
        let mut patched = current.clone();
        patched["api"]["port"] = json!(9999);
        sanitize_patch(&mut patched, &current);
        assert_eq!(
            patched["api"]["port"], 9999,
            "unrelated fields must still change"
        );
        assert_eq!(patched["api"]["access"], current["api"]["access"]);
    }

    #[test]
    fn sanitize_patch_still_restores_masked_credentials() {
        // It replaces restore_redacted at the call site, so it must not have
        // lost that half.
        let current = sample();
        let mut patched = current.clone();
        patched["s3"]["access_key_id"] = json!(REDACTED);
        sanitize_patch(&mut patched, &current);
        assert_eq!(patched["s3"]["access_key_id"], "fake-access");
    }

    fn pointer_of(dotted: &str) -> String {
        format!("/{}", dotted.replace('.', "/"))
    }
}

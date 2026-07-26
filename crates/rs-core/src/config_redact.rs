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
/// `webhook` is included because a Discord webhook URL is a bearer credential
/// in its own right (anyone holding it can post as the integration), even
/// though its name carries no "secret"/"token" word.
const SECRET_MARKERS: &[&str] = &[
    "secret",
    "token",
    "key",
    "password",
    "passphrase",
    "webhook",
];

/// Dotted paths that MATCH a marker but are not credentials, so they stay
/// readable in the API response. Keep this list as short as the UI/CI allow.
///
/// - `hetzner.ssh_key_name` / `hetzner.extra_ssh_key_names` — *names* of keys
///   registered in Hetzner Cloud, not key material.
/// - `api.tls_cert` / `api.tls_key` — file *paths* (`cert.pem` / `key.pem`),
///   resolved against the config directory. If either field is ever changed to
///   hold inline PEM material, DELETE it from this list — the material would
///   then be a credential.
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
/// Only string leaves are replaced: numbers, bools and `null` carry no
/// credential material, and masking them would break the typed round-trip
/// (callers deserialize this response back into `Config`). A `null` staying
/// `null` also keeps "unset" honest in the UI.
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
        Value::String(s) => *s = REDACTED.to_string(),
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
pub fn restore_redacted(patched: &mut Value, current: &Value) {
    restore_at("", patched, current);
}

fn restore_at(path: &str, patched: &mut Value, current: &Value) {
    if let Value::Object(map) = patched {
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
                "diag_token": "fake-diag",
                "port": 8910
            },
            "obs": { "ws_url": "ws://x", "ws_password": "fake-obs" }
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
    fn redact_then_restore_round_trips_to_the_original() {
        let current = sample();
        let mut public = current.clone();
        redact_secrets(&mut public);
        // The client sends the masked config straight back.
        let mut patched = public;
        restore_redacted(&mut patched, &current);
        assert_eq!(patched, current);
    }
}

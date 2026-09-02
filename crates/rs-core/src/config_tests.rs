use super::*;
use serial_test::serial;

#[test]
fn config_serde_roundtrip() {
    let config = Config::for_testing();
    let json = serde_json::to_string_pretty(&config).unwrap();
    let parsed: Config = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.client_uuid, config.client_uuid);
    assert_eq!(parsed.s3.bucket, config.s3.bucket);
    assert_eq!(parsed.inpoint.rtmp_port, config.inpoint.rtmp_port);
    assert_eq!(parsed.api.port, config.api.port);
    assert_eq!(parsed.hetzner.location, "fsn1");
    assert_eq!(parsed.delivery.delivery_delay_secs, 120);
}

#[test]
fn config_defaults() {
    let json = r#"{
        "client_uuid": "abc",
        "s3": {
            "bucket": "b",
            "region": "r",
            "endpoint": "e",
            "access_key_id": "k",
            "secret_access_key": "s"
        }
    }"#;
    let config: Config = serde_json::from_str(json).unwrap();
    assert_eq!(config.inpoint.rtmp_port, 1234);
    assert_eq!(config.inpoint.chunk_duration_ms, 1000);
    assert_eq!(config.api.port, 8910);
    assert_eq!(config.api.bind, "127.0.0.1");
    // #353: default is empty = "auto" (size by endpoint count). A real type
    // here would make the override always-on and defeat the tiering.
    assert_eq!(config.hetzner.server_type_override, "");
    assert_eq!(config.delivery.delivery_delay_secs, 120);
    assert_eq!(config.inpoint.chunk_format, "flv");
    assert!(config.obs.enabled);
    assert_eq!(config.obs.ws_url, "ws://127.0.0.1:4455");
    assert!(config.obs.ws_password.is_empty());
}

#[test]
fn stale_default_server_type_key_is_ignored_not_forced_as_override() {
    // #353 regression: every pre-#353 box has the DEAD `default_server_type`
    // field persisted with a real value (install.ps1 wrote "cx23"; a patched
    // config carries "cpx22"). That value was NEVER an operator choice.
    // Renaming the live field to `server_type_override` must make the stale
    // key inert (serde drops unknown fields), so the box resolves to AUTO
    // tiering — NOT a forced cpx22 that silently downgrades 3+-endpoint
    // events. If the field were still read under the old name, this would
    // deserialize "cpx22" into the override and fail.
    let json = r#"{
        "client_uuid": "x",
        "hetzner": { "api_token": "", "default_server_type": "cpx22" },
        "s3": {
            "bucket": "b", "region": "r",
            "endpoint": "e", "access_key_id": "k", "secret_access_key": "s"
        }
    }"#;
    let config: Config = serde_json::from_str(json).unwrap();
    assert_eq!(
        config.hetzner.server_type_override, "",
        "a stale `default_server_type` value must NOT become an active override"
    );
}

#[test]
fn config_save_and_load() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let config = Config::for_testing();
    config.save(&path).unwrap();
    let loaded = Config::load(&path).unwrap();
    assert_eq!(loaded.s3.bucket, config.s3.bucket);
    assert_eq!(loaded.hetzner.location, config.hetzner.location);
    assert_eq!(
        loaded.inpoint.chunk_duration_ms,
        config.inpoint.chunk_duration_ms
    );
}

#[serial]
#[test]
fn env_overrides() {
    // SAFETY: This test runs in isolation; env var mutation is acceptable.
    unsafe {
        std::env::set_var("RESTREAMER_CLIENT_UUID", "env-uuid");
        std::env::set_var("RESTREAMER_RTMP_PORT", "5678");
    }
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("config.json");
    let config = Config::for_testing();
    config.save(&path).unwrap();
    let loaded = Config::load(&path).unwrap();
    assert_eq!(loaded.client_uuid, "env-uuid");
    assert_eq!(loaded.inpoint.rtmp_port, 5678);
    // SAFETY: Cleaning up env vars set by this test.
    unsafe {
        std::env::remove_var("RESTREAMER_CLIENT_UUID");
        std::env::remove_var("RESTREAMER_RTMP_PORT");
    }
}

#[test]
fn validate_rejects_empty_client_uuid() {
    let config = Config::default();
    assert!(config.validate().is_err());
    assert!(config.validate().unwrap_err().contains("client_uuid"));
}

#[test]
fn validate_rejects_empty_s3_credentials() {
    let mut config = Config::for_testing();
    config.s3.access_key_id = String::new();
    assert!(config.validate().is_err());
    assert!(config.validate().unwrap_err().contains("access_key_id"));
}

#[test]
fn validate_accepts_valid_config() {
    let config = Config::for_testing();
    assert!(config.validate().is_ok());
}

#[test]
fn s3_region_guard_fires_on_nonstandard_region() {
    // #278: a stale per-install config.json can carry a degraded/wrong
    // region (the 2026-06-24 incident: streampp silently ran on nbg1).
    // The guard must report non-standard for exactly that case, and it
    // must NOT be a validate() error -- a non-standard region is a loud
    // signal, never a hard rejection (rescoped away from auto-override).
    let mut config = Config::for_testing();
    config.s3.region = "nbg1".to_string();
    assert!(
        !config.s3_region_is_standard(),
        "nbg1 must be flagged as non-standard"
    );
    assert!(
        config.validate().is_ok(),
        "a non-standard region must not fail validate() -- guard-only, not enforcement"
    );
}

#[test]
fn s3_region_guard_passes_on_standard_region() {
    let mut config = Config::for_testing();
    config.s3.region = STANDARD_S3_REGION.to_string();
    assert!(config.s3_region_is_standard());
}

#[test]
fn s3_region_guard_default_config_is_standard() {
    // Config::default() already hardcodes fsn1 (precedent:
    // HetznerConfig::location's default) -- the guard must agree.
    let config = Config::default();
    assert!(config.s3_region_is_standard());
    assert_eq!(config.s3.region, STANDARD_S3_REGION);
}

#[test]
fn validate_rejects_non_positive_skew_threshold() {
    // #354: a non-positive threshold would latch the ingest-skew banner
    // (and block Start Delivering) on a perfectly healthy stream.
    let mut config = Config::for_testing();
    config.inpoint.skew_threshold_ms = 0;
    let err = config.validate().unwrap_err();
    assert!(
        err.contains("skew_threshold_ms"),
        "Error should mention skew_threshold_ms: {err}"
    );

    config.inpoint.skew_threshold_ms = -1;
    assert!(config.validate().is_err());
}

#[test]
fn validate_rejects_ts_chunk_format() {
    let mut config = Config::for_testing();
    config.inpoint.chunk_format = "ts".to_string();
    let err = config.validate().unwrap_err();
    assert!(err.contains("ts"), "Error should mention ts: {err}");
}

#[test]
fn tls_config_defaults() {
    let json = r#"{
        "client_uuid": "test",
        "s3": { "bucket": "b", "region": "r", "endpoint": "e", "access_key_id": "a", "secret_access_key": "s" },
        "delivery": { "snapshot_label": "test" },
        "api": {}
    }"#;
    let config: Config = serde_json::from_str(json).unwrap();
    assert!(!config.api.tls);
    assert_eq!(config.api.https_port, 443);
    assert_eq!(config.api.tls_cert, "cert.pem");
    assert_eq!(config.api.tls_key, "key.pem");
    assert!(config.api.https_domain.is_none());
}

#[test]
fn tls_config_explicit() {
    let json = r#"{
        "client_uuid": "test",
        "s3": { "bucket": "b", "region": "r", "endpoint": "e", "access_key_id": "a", "secret_access_key": "s" },
        "delivery": { "snapshot_label": "test" },
        "api": {
            "tls": true,
            "https_port": 8443,
            "tls_cert": "my-cert.pem",
            "tls_key": "my-key.pem",
            "https_domain": "streamsnv.newlevel.media"
        }
    }"#;
    let config: Config = serde_json::from_str(json).unwrap();
    assert!(config.api.tls);
    assert_eq!(config.api.https_port, 8443);
    assert_eq!(config.api.tls_cert, "my-cert.pem");
    assert_eq!(config.api.tls_key, "my-key.pem");
    assert_eq!(
        config.api.https_domain.as_deref(),
        Some("streamsnv.newlevel.media")
    );
}

#[test]
fn event_s3_prefix_composes_client_uuid_and_event_name() {
    let config = Config {
        client_uuid: "abc-uuid".to_string(),
        ..Config::default()
    };
    assert_eq!(
        config.event_s3_prefix("sunday-service"),
        "abc-uuid/sunday-service"
    );
}

#[test]
fn client_s3_base_is_client_uuid_slash() {
    let config = Config {
        client_uuid: "abc-uuid".to_string(),
        ..Config::default()
    };
    assert_eq!(config.client_s3_base(), "abc-uuid/");
}

#[test]
fn notifications_default_is_empty_disabled() {
    // Absent `notifications` block -> empty webhook -> disabled (#261).
    let json = r#"{
        "client_uuid": "abc",
        "s3": { "bucket": "b", "region": "r", "endpoint": "e", "access_key_id": "k", "secret_access_key": "s" }
    }"#;
    let config: Config = serde_json::from_str(json).unwrap();
    assert!(config.notifications.discord_webhook_url.is_empty());
}

#[test]
fn notifications_webhook_roundtrips() {
    let json = r#"{
        "client_uuid": "abc",
        "s3": { "bucket": "b", "region": "r", "endpoint": "e", "access_key_id": "k", "secret_access_key": "s" },
        "notifications": { "discord_webhook_url": "https://discord.example/webhook/xyz" }
    }"#;
    let config: Config = serde_json::from_str(json).unwrap();
    assert_eq!(
        config.notifications.discord_webhook_url,
        "https://discord.example/webhook/xyz"
    );
    // Survives a save/load roundtrip.
    let reparsed: Config = serde_json::from_str(&serde_json::to_string(&config).unwrap()).unwrap();
    assert_eq!(
        reparsed.notifications.discord_webhook_url,
        "https://discord.example/webhook/xyz"
    );
}

#[test]
fn notifications_bot_fields_roundtrip_and_default_empty() {
    // Absent bot fields default to empty (bot mode disabled) — #306.
    let json_min = r#"{
        "client_uuid": "abc",
        "s3": { "bucket": "b", "region": "r", "endpoint": "e", "access_key_id": "k", "secret_access_key": "s" }
    }"#;
    let cfg_min: Config = serde_json::from_str(json_min).unwrap();
    assert!(cfg_min.notifications.discord_bot_token.is_empty());
    assert!(cfg_min.notifications.discord_channel_id.is_empty());

    // Bot fields parse and survive a save/load roundtrip — #306.
    let json = r#"{
        "client_uuid": "abc",
        "s3": { "bucket": "b", "region": "r", "endpoint": "e", "access_key_id": "k", "secret_access_key": "s" },
        "notifications": { "discord_bot_token": "Bot.tok.value", "discord_channel_id": "1373592666733940816" }
    }"#;
    let config: Config = serde_json::from_str(json).unwrap();
    assert_eq!(config.notifications.discord_bot_token, "Bot.tok.value");
    assert_eq!(
        config.notifications.discord_channel_id,
        "1373592666733940816"
    );
    let reparsed: Config = serde_json::from_str(&serde_json::to_string(&config).unwrap()).unwrap();
    assert_eq!(reparsed.notifications.discord_bot_token, "Bot.tok.value");
    assert_eq!(
        reparsed.notifications.discord_channel_id,
        "1373592666733940816"
    );
}

#[test]
fn s3_config_debug_redacts_credentials() {
    let config = Config::for_testing();
    let debug_str = format!("{:?}", config.s3);
    assert!(debug_str.contains("***"));
    assert!(!debug_str.contains("test-key"));
    assert!(!debug_str.contains("test-secret"));
}

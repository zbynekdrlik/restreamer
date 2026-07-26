use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

use crate::error::Result;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub client_uuid: String,
    pub s3: S3Config,
    #[serde(default)]
    pub hetzner: HetznerConfig,
    #[serde(default)]
    pub youtube: YouTubeOAuthConfig,
    #[serde(default)]
    pub inpoint: InpointConfig,
    #[serde(default)]
    pub api: ApiConfig,
    #[serde(default)]
    pub delivery: DeliveryConfig,
    #[serde(default)]
    pub obs: ObsConfig,
    #[serde(default)]
    pub notifications: NotificationsConfig,
}

/// Operator-facing outage notifications (#261, #306). All fields are runtime
/// secrets set by the operator in `config.json` — they MUST NOT be committed
/// anywhere in the repo.
///
/// Two delivery mechanisms are supported; the notifier picks one at build time
/// (`OutageNotifier::from_config`):
/// - **Bot token (#306, preferred)** — when both `discord_bot_token` and
///   `discord_channel_id` are set, alerts POST to the Discord REST API
///   (`channels/{id}/messages`) with an `Authorization: Bot <token>` header.
///   A thread IS a channel, so this targets the operator's existing alerts-snv
///   thread. This is the pattern camera-box already uses. Bot mode WINS when
///   both bot and webhook fields are set.
/// - **Webhook (#261, alternative)** — when only `discord_webhook_url` is set,
///   alerts POST the same `{"content": ...}` body to the webhook URL.
///
/// All fields empty (the default) disables notifications entirely, so the
/// feature ships dark until the operator fills one mechanism in.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct NotificationsConfig {
    /// Discord webhook URL for immediate outage alerts (#261, alternative to
    /// bot mode). Empty (the default) means "no webhook".
    #[serde(default)]
    pub discord_webhook_url: String,
    /// Discord bot token for posting to a channel/thread via the REST API
    /// (#306). Set together with `discord_channel_id` to enable bot mode.
    #[serde(default)]
    pub discord_bot_token: String,
    /// Discord channel/thread id to post alerts into (#306). A thread is a
    /// channel in the Discord API. Set together with `discord_bot_token`.
    #[serde(default)]
    pub discord_channel_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HetznerConfig {
    #[serde(default)]
    pub api_token: String,
    #[serde(default = "default_hetzner_location")]
    pub location: String,
    #[serde(default = "default_hetzner_server_type")]
    pub default_server_type: String,
    #[serde(default = "default_hetzner_snapshot_label")]
    pub snapshot_label: String,
    #[serde(default = "default_hetzner_ssh_key_name")]
    pub ssh_key_name: String,
    /// Additional SSH key names (registered in Hetzner Cloud) to install on
    /// every new delivery VPS, alongside `ssh_key_name`. Useful for ad-hoc
    /// debugging access without rotating the primary CI key.
    #[serde(default)]
    pub extra_ssh_key_names: Vec<String>,
}

fn default_hetzner_location() -> String {
    "fsn1".to_string()
}
fn default_hetzner_server_type() -> String {
    "cpx22".to_string()
}
fn default_hetzner_snapshot_label() -> String {
    "rs-delivery".to_string()
}
fn default_hetzner_ssh_key_name() -> String {
    "restreamer".to_string()
}

impl Default for HetznerConfig {
    fn default() -> Self {
        Self {
            api_token: String::new(),
            location: default_hetzner_location(),
            default_server_type: default_hetzner_server_type(),
            snapshot_label: default_hetzner_snapshot_label(),
            ssh_key_name: default_hetzner_ssh_key_name(),
            extra_ssh_key_names: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct YouTubeOAuthConfig {
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    #[serde(default)]
    pub device_flow: DeviceFlowConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct DeviceFlowConfig {
    #[serde(default)]
    pub client_id: String,
    #[serde(default)]
    pub client_secret: String,
    /// Daily quota units allowed against `liveStreams.list` (default 10000 per
    /// Google's published per-project budget). Read by the quota tracker.
    #[serde(default = "default_daily_quota")]
    pub daily_quota: u32,
}

impl Default for DeviceFlowConfig {
    fn default() -> Self {
        Self {
            client_id: String::new(),
            client_secret: String::new(),
            daily_quota: default_daily_quota(),
        }
    }
}

fn default_daily_quota() -> u32 {
    10_000
}

/// The project's standard S3 (Hetzner Object Storage) region. Follows the
/// same precedent as `HetznerConfig::location`'s default ("fsn1") -- fsn1 is
/// healthy; nbg1 is a known-degraded region (Hetzner status, open since
/// 2026-06-08) that caused a live production failure on 2026-06-24 when a
/// stale per-install `config.json` silently carried `s3.region=nbg1` across
/// an upgrade (#278). Not enforced/auto-overridden (an install's region is
/// still whatever `config.json` says) -- `Config::s3_region_is_standard`
/// below is the LOUD guard that flags drift instead.
pub const STANDARD_S3_REGION: &str = "fsn1";

#[derive(Clone, Serialize, Deserialize)]
pub struct S3Config {
    pub bucket: String,
    pub region: String,
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

impl std::fmt::Debug for S3Config {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("endpoint", &self.endpoint)
            .field("access_key_id", &"***")
            .field("secret_access_key", &"***")
            .finish()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InpointConfig {
    #[serde(default = "default_rtmp_port")]
    pub rtmp_port: u16,
    #[serde(default = "default_rtmp_bind")]
    pub rtmp_bind: String,
    #[serde(default = "default_chunk_duration_ms")]
    pub chunk_duration_ms: u64,
    #[serde(default = "default_read_buffer_bytes")]
    pub read_buffer_bytes: usize,
    /// Chunk storage format: "flv" (direct FLV, zero overhead) or "ts" (MPEG-TS legacy).
    #[serde(default = "default_chunk_format")]
    pub chunk_format: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiConfig {
    #[serde(default = "default_api_port")]
    pub port: u16,
    #[serde(default = "default_api_bind")]
    pub bind: String,
    #[serde(default)]
    pub tls: bool,
    #[serde(default = "default_https_port")]
    pub https_port: u16,
    #[serde(default = "default_tls_cert")]
    pub tls_cert: String,
    #[serde(default = "default_tls_key")]
    pub tls_key: String,
    #[serde(default)]
    pub https_domain: Option<String>,
    /// Origin-aware access control (#70 / #273 / #337 / #339).
    #[serde(default)]
    pub access: AccessConfig,
}

/// Cloudflare Access (Zero Trust) verification settings.
///
/// **Every value here is a PUBLIC identifier, not a credential** — which is the
/// whole point of the design chosen in #273: the box stores no shared secret at
/// all, so nothing on it can be stolen, leaked through `GET /api/v1/config`, or
/// rewritten through `PATCH /api/v1/config` to unlock the door.
///
/// Layer 1 is the Cloudflare Access application in front of the tunnel; this is
/// layer 2, which re-verifies the signed assertion inside the app so that a
/// second ingress rule, a port-forward, a second `cloudflared`, or a revived
/// tunnel on another box cannot bypass the edge.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessConfig {
    /// `enforce` (default) — internet-sourced requests need a valid Access JWT.
    /// `log_only` — classify and log, allow everything (the zero-rebuild
    /// rollback: behaviour identical to before #273).
    /// `lan_only` — reject every internet-sourced request outright, valid JWT
    /// or not (the opposite emergency lever, for a stolen phone session).
    #[serde(default = "default_access_mode")]
    pub mode: String,
    /// Zero Trust team domain; JWKS is fetched from
    /// `https://<team_domain>/cdn-cgi/access/certs` and the expected issuer is
    /// `https://<team_domain>`.
    #[serde(default = "default_access_team_domain")]
    pub team_domain: String,
    /// Accepted `aud` values — the Access application IDs. Both boxes list both
    /// AUDs so `streamsnv` and `streampp` can ship a byte-identical config.
    #[serde(default = "default_access_aud")]
    pub aud: Vec<String>,
}

fn default_access_mode() -> String {
    "enforce".to_string()
}

fn default_access_team_domain() -> String {
    "newlevelchurch.cloudflareaccess.com".to_string()
}

fn default_access_aud() -> Vec<String> {
    vec![
        // restreamer-snv -> streamsnv.newlevel.media
        "3d69cb15e165fef384d065feebe37f94918e2f4730756bc6c0ba0c054ff42d26".to_string(),
        // restreamer-pp  -> streampp.newlevel.media
        "238d9efbb4659d984e6b454d6ccf39156aa67007db1f2c7f709e153ce788dca0".to_string(),
    ]
}

impl Default for AccessConfig {
    fn default() -> Self {
        Self {
            mode: default_access_mode(),
            team_domain: default_access_team_domain(),
            aud: default_access_aud(),
        }
    }
}

fn default_rtmp_port() -> u16 {
    1234
}
fn default_rtmp_bind() -> String {
    "127.0.0.1".to_string()
}
fn default_chunk_duration_ms() -> u64 {
    1000
}
fn default_read_buffer_bytes() -> usize {
    102_400
}
fn default_chunk_format() -> String {
    "flv".to_string()
}
fn default_api_port() -> u16 {
    8910
}
fn default_api_bind() -> String {
    "127.0.0.1".to_string()
}
fn default_https_port() -> u16 {
    443
}
fn default_tls_cert() -> String {
    "cert.pem".to_string()
}
fn default_tls_key() -> String {
    "key.pem".to_string()
}

impl Default for InpointConfig {
    fn default() -> Self {
        Self {
            rtmp_port: default_rtmp_port(),
            rtmp_bind: default_rtmp_bind(),
            chunk_duration_ms: default_chunk_duration_ms(),
            read_buffer_bytes: default_read_buffer_bytes(),
            chunk_format: default_chunk_format(),
        }
    }
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            port: default_api_port(),
            bind: default_api_bind(),
            tls: false,
            https_port: default_https_port(),
            tls_cert: default_tls_cert(),
            tls_key: default_tls_key(),
            https_domain: None,
            access: AccessConfig::default(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryConfig {
    #[serde(default = "default_delivery_delay_secs")]
    pub delivery_delay_secs: u64,
}

fn default_delivery_delay_secs() -> u64 {
    120
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            delivery_delay_secs: default_delivery_delay_secs(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_obs_ws_url")]
    pub ws_url: String,
    #[serde(default)]
    pub ws_password: String,
}

fn default_obs_ws_url() -> String {
    "ws://127.0.0.1:4455".to_string()
}

impl Default for ObsConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            ws_url: default_obs_ws_url(),
            ws_password: String::new(),
        }
    }
}

impl Config {
    /// Load config from file, with env var overrides.
    pub fn load(path: &Path) -> Result<Self> {
        let content = std::fs::read_to_string(path)?;
        // Strip UTF-8 BOM if present (PowerShell writes BOM with -Encoding UTF8)
        let content = content.strip_prefix('\u{FEFF}').unwrap_or(&content);
        let mut config: Config = serde_json::from_str(content)?;
        config.apply_env_overrides();
        Ok(config)
    }

    /// Save config to file atomically (write to temp + rename).
    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let content = serde_json::to_string_pretty(self)?;
        let tmp_path = path.with_extension("json.tmp");
        std::fs::write(&tmp_path, &content)?;
        std::fs::rename(&tmp_path, path)?;
        Ok(())
    }

    /// Default config file path.
    pub fn default_path() -> PathBuf {
        if cfg!(windows) {
            PathBuf::from(r"C:\ProgramData\Restreamer\config.json")
        } else {
            PathBuf::from("/etc/restreamer/config.json")
        }
    }

    /// Directory where delivery VPS logs are persisted to disk as a backup
    /// to the `delivery_logs` DB table. Survives DB truncation and can be
    /// inspected with a plain text editor (no sqlite tooling needed).
    pub fn delivery_log_dir() -> PathBuf {
        let base = Self::default_path()
            .parent()
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("."));
        base.join("delivery-logs")
    }

    fn apply_env_overrides(&mut self) {
        if let Ok(v) = std::env::var("RESTREAMER_CLIENT_UUID") {
            self.client_uuid = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_S3_BUCKET") {
            self.s3.bucket = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_S3_REGION") {
            self.s3.region = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_S3_ENDPOINT") {
            self.s3.endpoint = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_S3_ACCESS_KEY_ID") {
            self.s3.access_key_id = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_S3_SECRET_ACCESS_KEY") {
            self.s3.secret_access_key = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_HETZNER_API_TOKEN") {
            self.hetzner.api_token = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_RTMP_PORT") {
            match v.parse() {
                Ok(port) => self.inpoint.rtmp_port = port,
                Err(e) => tracing::warn!("Invalid RESTREAMER_RTMP_PORT '{v}': {e}"),
            }
        }
        if let Ok(v) = std::env::var("RESTREAMER_RTMP_BIND") {
            self.inpoint.rtmp_bind = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_API_PORT") {
            match v.parse() {
                Ok(port) => self.api.port = port,
                Err(e) => tracing::warn!("Invalid RESTREAMER_API_PORT '{v}': {e}"),
            }
        }
        if let Ok(v) = std::env::var("RESTREAMER_API_BIND") {
            self.api.bind = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_DELIVERY_DELAY_SECS") {
            match v.parse() {
                Ok(secs) => self.delivery.delivery_delay_secs = secs,
                Err(e) => tracing::warn!("Invalid RESTREAMER_DELIVERY_DELAY_SECS '{v}': {e}"),
            }
        }
        if let Ok(v) = std::env::var("RESTREAMER_OBS_ENABLED") {
            self.obs.enabled = v == "1" || v.eq_ignore_ascii_case("true");
        }
        if let Ok(v) = std::env::var("RESTREAMER_OBS_WS_URL") {
            self.obs.ws_url = v;
        }
        if let Ok(v) = std::env::var("RESTREAMER_OBS_WS_PASSWORD") {
            self.obs.ws_password = v;
        }
    }

    /// Build the S3 prefix for an event's chunks: `{client_uuid}/{event_name}`.
    /// All chunk keys nest under this prefix so two Restreamer installations
    /// sharing one S3 bucket can't collide on identically-named events (#114).
    pub fn event_s3_prefix(&self, event_name: &str) -> String {
        format!("{}/{}", self.client_uuid, event_name)
    }

    /// Top-level S3 prefix for all of this installation's chunks: `{client_uuid}/`.
    /// Used by dashboard listings to enumerate this installation's events (#114).
    pub fn client_s3_base(&self) -> String {
        format!("{}/", self.client_uuid)
    }

    /// Validate that required configuration fields are present.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.client_uuid.is_empty() {
            return Err("client_uuid is required".to_string());
        }
        if self.s3.bucket.is_empty() {
            return Err("s3.bucket is required".to_string());
        }
        if self.s3.access_key_id.is_empty() {
            return Err("s3.access_key_id is required".to_string());
        }
        if self.s3.secret_access_key.is_empty() {
            return Err("s3.secret_access_key is required".to_string());
        }
        if self.inpoint.chunk_format == "ts" {
            return Err("chunk_format \"ts\" is no longer supported, use \"flv\"".to_string());
        }
        Ok(())
    }

    /// True when `s3.region` matches [`STANDARD_S3_REGION`]. A non-standard
    /// region is NOT a validation error -- an operator's box keeps running
    /// on whatever region its config carries (#278 explicitly rescoped away
    /// from silent auto-override). Callers use this as a LOUD signal: emit
    /// an `audit::Severity::Critical` row and surface the dashboard banner
    /// so a stale/degraded region can never again go unnoticed across an
    /// install or upgrade.
    pub fn s3_region_is_standard(&self) -> bool {
        self.s3.region == STANDARD_S3_REGION
    }

    /// Create a minimal config for testing.
    pub fn for_testing() -> Self {
        Self {
            client_uuid: "test-uuid-00000000".to_string(),
            s3: S3Config {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: "http://localhost:9000".to_string(),
                access_key_id: "test-key".to_string(),
                secret_access_key: "test-secret".to_string(),
            },
            hetzner: HetznerConfig::default(),
            youtube: YouTubeOAuthConfig::default(),
            inpoint: InpointConfig::default(),
            api: ApiConfig {
                port: 0, // random port for tests
                bind: "127.0.0.1".to_string(),
                ..ApiConfig::default()
            },
            delivery: DeliveryConfig::default(),
            obs: ObsConfig {
                enabled: false, // Disable in tests to avoid background connection attempts
                ..ObsConfig::default()
            },
            notifications: NotificationsConfig::default(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            client_uuid: String::new(),
            s3: S3Config {
                bucket: "restreamer-chunks-fsn1".to_string(),
                region: "fsn1".to_string(),
                endpoint: "https://fsn1.your-objectstorage.com".to_string(),
                access_key_id: String::new(),
                secret_access_key: String::new(),
            },
            hetzner: HetznerConfig::default(),
            youtube: YouTubeOAuthConfig::default(),
            inpoint: InpointConfig::default(),
            api: ApiConfig::default(),
            delivery: DeliveryConfig::default(),
            obs: ObsConfig::default(),
            notifications: NotificationsConfig::default(),
        }
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!(config.hetzner.default_server_type, "cpx22");
        assert_eq!(config.delivery.delivery_delay_secs, 120);
        assert_eq!(config.inpoint.chunk_format, "flv");
        assert!(config.obs.enabled);
        assert_eq!(config.obs.ws_url, "ws://127.0.0.1:4455");
        assert!(config.obs.ws_password.is_empty());
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
        let reparsed: Config =
            serde_json::from_str(&serde_json::to_string(&config).unwrap()).unwrap();
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
        let reparsed: Config =
            serde_json::from_str(&serde_json::to_string(&config).unwrap()).unwrap();
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
}

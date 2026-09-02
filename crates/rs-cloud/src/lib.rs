/// Hetzner Cloud API client for managing delivery VPS instances.
///
/// Handles full lifecycle: create from image/snapshot, health monitoring,
/// binary version checks, snapshot management, and teardown.
use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod hetzner;

#[derive(Debug, Error)]
pub enum CloudError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("API error: {status} - {message}")]
    Api { status: u16, message: String },
    #[error("server not found: {0}")]
    ServerNotFound(String),
    #[error("snapshot not found: {0}")]
    SnapshotNotFound(String),
    #[error("timeout waiting for server: {0}")]
    Timeout(String),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, CloudError>;

/// Configuration for Hetzner Cloud operations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HetznerConfig {
    pub api_token: String,
    pub location: String,
    pub default_server_type: String,
    pub snapshot_label: String,
    pub ssh_key_name: String,
}

impl Default for HetznerConfig {
    fn default() -> Self {
        Self {
            api_token: String::new(),
            location: "fsn1".to_string(),
            default_server_type: "cpx22".to_string(),
            snapshot_label: "rs-delivery".to_string(),
            ssh_key_name: "restreamer".to_string(),
        }
    }
}

/// Server status in our tracking database.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ServerStatus {
    Creating,
    Running,
    Stopping,
    Deleted,
    Failed,
}

impl std::fmt::Display for ServerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Creating => write!(f, "creating"),
            Self::Running => write!(f, "running"),
            Self::Stopping => write!(f, "stopping"),
            Self::Deleted => write!(f, "deleted"),
            Self::Failed => write!(f, "failed"),
        }
    }
}

impl std::str::FromStr for ServerStatus {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "creating" => Ok(Self::Creating),
            "running" => Ok(Self::Running),
            "stopping" => Ok(Self::Stopping),
            "deleted" => Ok(Self::Deleted),
            "failed" => Ok(Self::Failed),
            other => Err(format!("unknown server status: {other}")),
        }
    }
}

/// Represents a tracked delivery instance.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryInstance {
    pub id: i64,
    pub hetzner_id: i64,
    pub name: String,
    pub ipv4: String,
    pub status: ServerStatus,
    pub server_type: String,
    pub event_id: Option<i64>,
    pub created_at: String,
    pub last_health_at: Option<String>,
}

/// Select server type based on endpoint count.
pub fn select_server_type(endpoint_count: usize) -> &'static str {
    match endpoint_count {
        0..=2 => "cpx22",
        3..=7 => "cpx32",
        _ => "cpx42",
    }
}

/// Resolve the Hetzner server type for a delivery VPS (#353).
///
/// An explicit `config_override` (`hetzner.server_type_override`) WINS over the
/// endpoint-count tiering; an empty/whitespace override means "auto" and falls
/// back to [`select_server_type`]. A known-deprecated `cx23/cx33/cx43` override
/// is normalized to its `cpx22/cpx32/cpx42` successor (the `de4df3e9` rename)
/// so an old `config.json` cannot push a type the Hetzner API rejects.
pub fn resolve_server_type(config_override: &str, endpoint_count: usize) -> String {
    let trimmed = config_override.trim();
    if trimmed.is_empty() {
        // "auto" — size by endpoint count.
        return select_server_type(endpoint_count).to_string();
    }
    // Explicit override. Normalize the three known-deprecated cx* strings to
    // their cpx* successors (the de4df3e9 rename) so a stale config.json can't
    // push a retired type the Hetzner API rejects.
    match trimmed.to_ascii_lowercase().as_str() {
        "cx23" => {
            tracing::warn!("hetzner.server_type_override 'cx23' is deprecated; using 'cpx22'");
            "cpx22".to_string()
        }
        "cx33" => {
            tracing::warn!("hetzner.server_type_override 'cx33' is deprecated; using 'cpx32'");
            "cpx32".to_string()
        }
        "cx43" => {
            tracing::warn!("hetzner.server_type_override 'cx43' is deprecated; using 'cpx42'");
            "cpx42".to_string()
        }
        // Hetzner server types are all lowercase, so normalize case here too —
        // a "CPX32" typo would otherwise 400 at event start.
        other => other.to_string(),
    }
}

/// S3 credentials passed to delivery VPS via cloud-init environment file.
pub struct DeliveryS3Credentials {
    pub bucket: String,
    pub region: String,
    pub endpoint: String,
    pub access_key_id: String,
    pub secret_access_key: String,
}

/// Generate the environment file content for rs-delivery.
fn delivery_env_file(s3: &DeliveryS3Credentials, auth_token: &str) -> String {
    format!(
        "DELIVERY_S3_BUCKET={}\nDELIVERY_S3_REGION={}\nDELIVERY_S3_ENDPOINT={}\nDELIVERY_S3_ACCESS_KEY_ID={}\nDELIVERY_S3_SECRET_ACCESS_KEY={}\nDELIVERY_AUTH_TOKEN={}\n",
        s3.bucket, s3.region, s3.endpoint, s3.access_key_id, s3.secret_access_key, auth_token,
    )
}

/// Cloud-init script for bootstrapping a delivery VPS from scratch.
/// S3 credentials are written to an environment file on disk (mode 0600)
/// so they never travel over plaintext HTTP.
pub fn bootstrap_cloud_init(
    delivery_binary_url: &str,
    s3: &DeliveryS3Credentials,
    auth_token: &str,
) -> String {
    let env_content = delivery_env_file(s3, auth_token);
    format!(
        r#"#cloud-config
packages:
  - curl
  - unzip

write_files:
  - path: /opt/restreamer/rs-delivery.env
    permissions: '0600'
    content: |
{env_lines}
  - path: /opt/restreamer/log-uploader.sh
    permissions: '0755'
    content: |
      #!/bin/bash
      # Background watchdog: uploads rs-delivery.log to S3 every 15s so we
      # can post-mortem VPS startup crashes (rs-delivery binary dying before
      # its HTTP endpoint is reachable). Runs for the lifetime of the VPS.
      # Nice-to-have: do nothing if aws CLI isn't installed yet.
      set +e
      set -a; source /opt/restreamer/rs-delivery.env; set +a
      export AWS_ACCESS_KEY_ID="$DELIVERY_S3_ACCESS_KEY_ID"
      export AWS_SECRET_ACCESS_KEY="$DELIVERY_S3_SECRET_ACCESS_KEY"
      export AWS_DEFAULT_REGION="$DELIVERY_S3_REGION"
      HN=$(hostname)
      S3_LOG="s3://$DELIVERY_S3_BUCKET/delivery-logs/$HN.log"
      while true; do
        if command -v aws >/dev/null 2>&1 && [ -s /opt/restreamer/rs-delivery.log ]; then
          aws s3 cp /opt/restreamer/rs-delivery.log "$S3_LOG" \
            --endpoint-url "$DELIVERY_S3_ENDPOINT" --quiet 2>&1 | head -5
        fi
        sleep 15
      done
  - path: /opt/restreamer/install-awscli.sh
    permissions: '0755'
    content: |
      #!/bin/bash
      # Install AWS CLI v2 from the official bundled installer (Ubuntu 24.04
      # has no awscli apt package and pip install is blocked by PEP 668).
      # This is a NICE-TO-HAVE for log capture; failures here must NOT block
      # rs-delivery from starting.
      set +e
      curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o /tmp/awscliv2.zip
      unzip -qo /tmp/awscliv2.zip -d /tmp/
      /tmp/aws/install --update --bin-dir /usr/local/bin --install-dir /usr/local/aws-cli
      rm -rf /tmp/awscliv2.zip /tmp/aws
  - path: /opt/restreamer/setup.sh
    permissions: '0755'
    content: |
      #!/bin/bash
      # CRITICAL path: download rs-delivery and start it. Nothing here may
      # fail silently. Log capture (awscli install) runs separately in the
      # background and its failure does not block rs-delivery startup.
      set -ex
      mkdir -p /opt/restreamer
      echo "[setup] Downloading binary..."
      curl -fsSL -o /opt/restreamer/rs-delivery "{delivery_binary_url}"
      chmod +x /opt/restreamer/rs-delivery
      echo "[setup] Binary size: $(stat -c%s /opt/restreamer/rs-delivery) bytes"
      set -a; source /opt/restreamer/rs-delivery.env; set +a
      echo "[setup] Starting rs-delivery..."
      /opt/restreamer/rs-delivery > /opt/restreamer/rs-delivery.log 2>&1 &
      RS_PID=$!
      echo "[setup] Starting log uploader watchdog (waits for awscli)..."
      nohup /opt/restreamer/log-uploader.sh > /var/log/log-uploader.log 2>&1 &
      echo "[setup] Installing awscli in background..."
      nohup /opt/restreamer/install-awscli.sh > /var/log/install-awscli.log 2>&1 &
      sleep 3
      if ! kill -0 $RS_PID 2>/dev/null; then
        echo "[setup] ERROR: rs-delivery crashed! Log:" >&2
        cat /opt/restreamer/rs-delivery.log >&2
        exit 1
      fi
      echo "[setup] rs-delivery running as PID $RS_PID"

runcmd:
  - /opt/restreamer/setup.sh
"#,
        env_lines = env_content
            .lines()
            .map(|l| format!("      {l}"))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

/// Cloud-init script for starting delivery from an existing snapshot.
/// Downloads the latest binary from S3 to ensure the newest version runs,
/// while the snapshot provides curl/awscli and other dependencies
/// pre-installed. (#212 removed ffmpeg — the rust pusher is the only backend.)
/// S3 credentials are written to an environment file on disk (mode 0600).
pub fn snapshot_cloud_init(
    delivery_binary_url: &str,
    s3: &DeliveryS3Credentials,
    auth_token: &str,
) -> String {
    let env_content = delivery_env_file(s3, auth_token);
    format!(
        r#"#cloud-config
write_files:
  - path: /opt/restreamer/rs-delivery.env
    permissions: '0600'
    content: |
{env_lines}
  - path: /opt/restreamer/log-uploader.sh
    permissions: '0755'
    content: |
      #!/bin/bash
      # Background watchdog uploads rs-delivery.log to S3 every 15s so we
      # can post-mortem a crash of the rs-delivery binary before its HTTP
      # endpoint is reachable. Snapshots have awscli pre-installed.
      set +e
      set -a; source /opt/restreamer/rs-delivery.env; set +a
      export AWS_ACCESS_KEY_ID="$DELIVERY_S3_ACCESS_KEY_ID"
      export AWS_SECRET_ACCESS_KEY="$DELIVERY_S3_SECRET_ACCESS_KEY"
      export AWS_DEFAULT_REGION="$DELIVERY_S3_REGION"
      HN=$(hostname)
      S3_LOG="s3://$DELIVERY_S3_BUCKET/delivery-logs/$HN.log"
      while true; do
        if [ -s /opt/restreamer/rs-delivery.log ]; then
          aws s3 cp /opt/restreamer/rs-delivery.log "$S3_LOG" \
            --endpoint-url "$DELIVERY_S3_ENDPOINT" --quiet 2>&1 | head -5
        fi
        sleep 15
      done

runcmd:
  # Anchor the rs-delivery pattern to the binary path so we do not kill
  # the log-uploader.sh script (whose argv references rs-delivery.log).
  - pkill -f '^/opt/restreamer/rs-delivery$' || true
  - pkill -f log-uploader || true
  - curl -fsSL -o /opt/restreamer/rs-delivery "{delivery_binary_url}"
  - chmod +x /opt/restreamer/rs-delivery
  - bash -c 'set -a; source /opt/restreamer/rs-delivery.env; set +a; nohup /opt/restreamer/rs-delivery > /opt/restreamer/rs-delivery.log 2>&1 &'
  - bash -c 'nohup /opt/restreamer/log-uploader.sh > /var/log/log-uploader.log 2>&1 &'
"#,
        env_lines = env_content
            .lines()
            .map(|l| format!("      {l}"))
            .collect::<Vec<_>>()
            .join("\n"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_creds() -> DeliveryS3Credentials {
        DeliveryS3Credentials {
            bucket: "test-bucket".to_string(),
            region: "eu-central".to_string(),
            endpoint: "https://s3.example.com".to_string(),
            access_key_id: "AKIAEXAMPLE".to_string(),
            secret_access_key: "s3cr3t".to_string(),
        }
    }

    // ----- resolve_server_type (#353) -----

    #[test]
    fn resolve_empty_override_falls_back_to_endpoint_tiering() {
        // "unset" (empty / whitespace) => the endpoint-count tiering still applies.
        for (raw, count, want) in [
            ("", 0usize, "cpx22"),
            ("   ", 2, "cpx22"),
            ("", 3, "cpx32"),
            ("\t", 7, "cpx32"),
            ("", 8, "cpx42"),
        ] {
            assert_eq!(
                resolve_server_type(raw, count),
                want,
                "empty override {raw:?} with {count} endpoints must use tiering"
            );
        }
    }

    #[test]
    fn resolve_explicit_override_wins_over_tiering() {
        // The operator's explicit choice beats the endpoint count — a 5-endpoint
        // event (tiering would pick cpx32) is forced onto the cheaper cpx11.
        assert_eq!(resolve_server_type("cpx11", 5), "cpx11");
        assert_eq!(resolve_server_type("cpx22", 8), "cpx22");
        // Surrounding whitespace is trimmed; the value is lowercased (Hetzner
        // types are lowercase, so a "CPX32" typo must not reach the API as-is).
        assert_eq!(resolve_server_type("  cpx31  ", 1), "cpx31");
        assert_eq!(resolve_server_type("CPX32", 1), "cpx32");
    }

    #[test]
    fn resolve_normalizes_deprecated_cx_override() {
        // A stale config.json carrying a deprecated cx* string (old install.ps1
        // default was cx23) must map to its cpx* successor, not be forced verbatim
        // onto the Hetzner API (which rejects the retired cx line).
        assert_eq!(resolve_server_type("cx23", 5), "cpx22");
        assert_eq!(resolve_server_type("cx33", 0), "cpx32");
        assert_eq!(resolve_server_type("cx43", 1), "cpx42");
        assert_eq!(resolve_server_type("  CX23  ", 5), "cpx22");
    }

    // ----- delivery_env_file (#116) -----

    #[test]
    fn delivery_env_file_contains_all_fields_in_order() {
        let out = delivery_env_file(&test_creds(), "auth-tok-123");
        assert_eq!(
            out,
            "DELIVERY_S3_BUCKET=test-bucket\n\
             DELIVERY_S3_REGION=eu-central\n\
             DELIVERY_S3_ENDPOINT=https://s3.example.com\n\
             DELIVERY_S3_ACCESS_KEY_ID=AKIAEXAMPLE\n\
             DELIVERY_S3_SECRET_ACCESS_KEY=s3cr3t\n\
             DELIVERY_AUTH_TOKEN=auth-tok-123\n"
        );
    }

    // ----- bootstrap_cloud_init (#116) -----

    #[test]
    fn bootstrap_cloud_init_writes_env_file_with_0600_permissions() {
        // The env file carries S3 secrets in plaintext -- a mutation that
        // swapped '0600' for '0700' (world-readable-ish) would be a real
        // security regression. Pin the exact permission line.
        let out = bootstrap_cloud_init("https://cdn.example.com/rs-delivery", &test_creds(), "tok");
        assert!(
            out.contains("path: /opt/restreamer/rs-delivery.env\n    permissions: '0600'"),
            "env file must be mode 0600 (contains S3 secrets):\n{out}"
        );
    }

    #[test]
    fn bootstrap_cloud_init_embeds_binary_url_and_indented_env_content() {
        let creds = test_creds();
        let out = bootstrap_cloud_init("https://cdn.example.com/rs-delivery-v9", &creds, "tok-xyz");
        assert!(out.contains(
            r#"curl -fsSL -o /opt/restreamer/rs-delivery "https://cdn.example.com/rs-delivery-v9""#
        ));
        // env_lines must be indented 6 spaces under the YAML `content: |` block.
        assert!(out.contains("      DELIVERY_S3_BUCKET=test-bucket"));
        assert!(out.contains("      DELIVERY_S3_REGION=eu-central"));
        assert!(out.contains("      DELIVERY_AUTH_TOKEN=tok-xyz"));
        // #212: ffmpeg was removed from the apt list — the rust pusher is the
        // only push backend and nothing spawns the ffmpeg binary on the VPS.
        assert!(out.contains("packages:\n  - curl"));
        assert!(
            !out.contains("- ffmpeg"),
            "ffmpeg must no longer be installed on the delivery VPS (#212):\n{out}"
        );
        assert!(out.contains("runcmd:\n  - /opt/restreamer/setup.sh"));
    }

    #[test]
    fn bootstrap_cloud_init_starts_log_uploader_and_awscli_install() {
        let out = bootstrap_cloud_init("https://cdn.example.com/bin", &test_creds(), "tok");
        assert!(out.contains("nohup /opt/restreamer/log-uploader.sh"));
        assert!(out.contains("nohup /opt/restreamer/install-awscli.sh"));
        assert!(out.contains("path: /opt/restreamer/log-uploader.sh\n    permissions: '0755'"));
    }

    // ----- snapshot_cloud_init (#116) -----

    #[test]
    fn snapshot_cloud_init_writes_env_file_with_0600_permissions() {
        let out = snapshot_cloud_init("https://cdn.example.com/rs-delivery", &test_creds(), "tok");
        assert!(
            out.contains("path: /opt/restreamer/rs-delivery.env\n    permissions: '0600'"),
            "env file must be mode 0600 (contains S3 secrets):\n{out}"
        );
    }

    #[test]
    fn snapshot_cloud_init_kills_old_binary_before_restart() {
        let out = snapshot_cloud_init(
            "https://cdn.example.com/rs-delivery-v9",
            &test_creds(),
            "tok-xyz",
        );
        assert!(out.contains(r"pkill -f '^/opt/restreamer/rs-delivery$' || true"));
        assert!(out.contains(r"pkill -f log-uploader || true"));
        assert!(out.contains(
            r#"curl -fsSL -o /opt/restreamer/rs-delivery "https://cdn.example.com/rs-delivery-v9""#
        ));
        assert!(
            !out.contains("packages:"),
            "snapshot path must not reinstall packages (already in the snapshot)"
        );
    }

    #[test]
    fn snapshot_cloud_init_embeds_indented_env_content() {
        let creds = test_creds();
        let out = snapshot_cloud_init("https://cdn.example.com/bin", &creds, "tok-xyz");
        assert!(out.contains("      DELIVERY_S3_BUCKET=test-bucket"));
        assert!(out.contains("      DELIVERY_AUTH_TOKEN=tok-xyz"));
    }

    #[test]
    fn bootstrap_and_snapshot_produce_different_scripts() {
        let creds = test_creds();
        let a = bootstrap_cloud_init("https://cdn.example.com/bin", &creds, "tok");
        let b = snapshot_cloud_init("https://cdn.example.com/bin", &creds, "tok");
        assert_ne!(a, b);
    }
}

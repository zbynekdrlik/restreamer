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
  - ffmpeg
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
/// while the snapshot provides ffmpeg and other dependencies pre-installed.
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

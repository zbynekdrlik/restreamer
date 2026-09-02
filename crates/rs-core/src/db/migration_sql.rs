//! Pure-DDL migration SQL scripts (child of `db::migrations`, #125).
//!
//! Extracted from `migrations.rs` to keep that file under the 1000-line CI
//! cap. These are the migrations whose bodies are plain `CREATE TABLE/INDEX
//! IF NOT EXISTS` DDL (idempotent on their own); the dispatcher in the parent
//! module runs them via `execute_sql_statements`. Destructive rebuilds
//! (V3/V4/V16) stay in the parent module as guarded `migrate_vN` fns.

pub(crate) const MIGRATION_V1_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS client_profile (
    id        INTEGER PRIMARY KEY,
    user_uuid TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS streaming_events (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    identifier           TEXT UNIQUE,
    short_description    TEXT,
    date_of_event        TEXT NOT NULL DEFAULT (datetime('now')),
    server_ip            TEXT DEFAULT '',
    received_bytes       INTEGER NOT NULL DEFAULT 0,
    receiving_activated  INTEGER NOT NULL DEFAULT 0,
    delivering_activated INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS chunk_records (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    streaming_event_id INTEGER NOT NULL REFERENCES streaming_events(id) ON DELETE CASCADE,
    chunk_file_path    TEXT NOT NULL,
    data_size          INTEGER NOT NULL,
    created_at         TEXT NOT NULL DEFAULT (datetime('now')),
    md5                TEXT NOT NULL DEFAULT '',
    in_process         INTEGER NOT NULL DEFAULT 0,
    sent               INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_chunks_unsent ON chunk_records(streaming_event_id, sent, in_process)
    WHERE sent = 0 AND in_process = 0
"#;

pub(crate) const MIGRATION_V2_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS endpoint_configs (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    alias          TEXT NOT NULL UNIQUE,
    service_type   TEXT NOT NULL CHECK(service_type IN ('YT_HLS','FB','YT_RTMP','VIMEO','INSTAGRAM','TEST_FILE')),
    stream_key     TEXT NOT NULL DEFAULT '',
    enabled        INTEGER NOT NULL DEFAULT 1,
    position_last  INTEGER NOT NULL DEFAULT 0,
    delivered_bytes INTEGER NOT NULL DEFAULT 0,
    is_fast        INTEGER NOT NULL DEFAULT 0,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at     TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS event_endpoints (
    event_id    INTEGER NOT NULL REFERENCES streaming_events(id) ON DELETE CASCADE,
    endpoint_id INTEGER NOT NULL REFERENCES endpoint_configs(id) ON DELETE CASCADE,
    PRIMARY KEY (event_id, endpoint_id)
);

CREATE TABLE IF NOT EXISTS delivery_instances (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    hetzner_id     INTEGER NOT NULL UNIQUE,
    name           TEXT NOT NULL,
    ipv4           TEXT NOT NULL DEFAULT '',
    status         TEXT NOT NULL DEFAULT 'creating' CHECK(status IN ('creating','running','stopping','deleted')),
    server_type    TEXT NOT NULL DEFAULT 'cx23',
    event_id       INTEGER REFERENCES streaming_events(id) ON DELETE SET NULL,
    created_at     TEXT NOT NULL DEFAULT (datetime('now')),
    last_health_at TEXT,
    auth_token     TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS delivery_endpoint_status (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id     INTEGER NOT NULL REFERENCES delivery_instances(id) ON DELETE CASCADE,
    alias           TEXT NOT NULL,
    alive           INTEGER NOT NULL DEFAULT 0,
    buff_size_bytes INTEGER NOT NULL DEFAULT 0,
    current_chunk_id INTEGER NOT NULL DEFAULT 0,
    last_check_at   TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS youtube_oauth (
    id             INTEGER PRIMARY KEY DEFAULT 1,
    access_token   TEXT NOT NULL DEFAULT '',
    refresh_token  TEXT NOT NULL DEFAULT '',
    token_uri      TEXT NOT NULL DEFAULT 'https://oauth2.googleapis.com/token',
    client_id      TEXT NOT NULL DEFAULT '',
    client_secret  TEXT NOT NULL DEFAULT '',
    scopes         TEXT NOT NULL DEFAULT '',
    expires_at     TEXT
);

"#;

pub(crate) const MIGRATION_V13_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS delivery_logs (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id INTEGER NOT NULL,
    event_id    INTEGER,
    captured_at TEXT NOT NULL DEFAULT (datetime('now')),
    log_text    TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS delivery_restart_log (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id   INTEGER NOT NULL,
    event_id      INTEGER,
    alias         TEXT NOT NULL,
    timestamp_ms  INTEGER NOT NULL,
    chunk_id      INTEGER NOT NULL,
    lifetime_secs INTEGER NOT NULL,
    reason        TEXT NOT NULL,
    stderr_tail   TEXT,
    backoff_secs  INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_delivery_restart_log_instance
    ON delivery_restart_log(instance_id);

CREATE INDEX IF NOT EXISTS idx_delivery_logs_instance
    ON delivery_logs(instance_id)
"#;

pub(crate) const MIGRATION_V18_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS audit_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          TEXT    NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ','now')),
    severity    TEXT    NOT NULL,
    source      TEXT    NOT NULL,
    event_id    INTEGER,
    instance_id INTEGER,
    endpoint    TEXT,
    action      TEXT    NOT NULL,
    detail      TEXT    NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_audit_ts    ON audit_log(ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_event ON audit_log(event_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_sev   ON audit_log(severity, ts DESC);
"#;

pub(crate) const MIGRATION_V19_SQL: &str = r#"
CREATE TABLE IF NOT EXISTS delivery_endpoint_metrics (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    ts_ms                 INTEGER NOT NULL,
    instance_id           INTEGER NOT NULL,
    event_id              INTEGER NOT NULL,
    alias                 TEXT    NOT NULL,
    alive                 INTEGER NOT NULL,
    current_chunk_id      INTEGER NOT NULL,
    chunks_processed      INTEGER NOT NULL,
    chunk_delay_secs      REAL    NOT NULL,
    bytes_processed_total INTEGER NOT NULL,
    ffmpeg_restart_count  INTEGER NOT NULL,
    delivery_mode         TEXT
);
CREATE INDEX IF NOT EXISTS idx_dem_event_alias
    ON delivery_endpoint_metrics(event_id, alias, ts_ms DESC);
CREATE INDEX IF NOT EXISTS idx_dem_ts
    ON delivery_endpoint_metrics(ts_ms DESC);
"#;

pub(crate) const MIGRATION_V23_SQL: &str = r#"
CREATE INDEX IF NOT EXISTS idx_delivery_instances_event_id_active
    ON delivery_instances(event_id, id DESC)
    WHERE status != 'deleted';
"#;

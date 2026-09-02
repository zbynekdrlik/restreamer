use super::*;

#[test]
fn ws_event_serde_roundtrip() {
    let events = vec![
        WsEvent::InpointStatus {
            state: "receiving".to_string(),
            rtmp_connected: true,
            received_bytes: 1024,
            chunk_count: 5,
        },
        WsEvent::EndpointStatus {
            state: "uploading".to_string(),
            pending_chunks: 10,
            active_uploads: 2,
            buffer_duration: "00:00:10".to_string(),
        },
        WsEvent::ChunkReceived {
            id: 1,
            data_size: 512,
            md5: "abc123".to_string(),
        },
        WsEvent::ChunkUploaded { chunk_id: 1 },
        WsEvent::ChunkUploadAttempt {
            chunk_id: 2,
            attempt: 1,
        },
        WsEvent::ChunkUploadFailed {
            chunk_id: 3,
            error: "timeout".to_string(),
            permanent: false,
        },
        WsEvent::StreamingEvent {
            action: "created".to_string(),
            name: Some("evt-1".to_string()),
            receiving: true,
            delivering: false,
        },
        WsEvent::DeliveryStatus {
            instance_name: "rs-delivery-1".to_string(),
            status: "running".to_string(),
            server_ip: Some("1.2.3.4".to_string()),
            endpoint_count: 2,
            endpoints: vec![DeliveryEndpointMetrics {
                alias: "YouTube".to_string(),
                alive: true,
                current_chunk_id: 42,
                bytes_processed_total: 1048576,
                chunks_processed: 100,
                chunk_delay_secs: 3.2,
                stall_reason: None,
                ffmpeg_restart_count: 0,

                reconnect_count: 0,
                av_skew_ms: 0,
                fast_delay_target_secs: None,
                last_error: None,
                is_fast: false,
                delivery_mode: None,
                rescue_eta_secs: None,
                youtube_health: None,
                lifecycle: EndpointLifecycle::Live,
            }],
        },
        WsEvent::Error {
            service: "inpoint".to_string(),
            message: "connection lost".to_string(),
        },
        WsEvent::ActivityFeed {
            timestamp: "2026-01-01T00:00:00Z".to_string(),
            severity: "info".to_string(),
            message: "Stream started".to_string(),
            source: "system".to_string(),
        },
        WsEvent::PipelineState {
            state: "buffering".to_string(),
            event_id: Some(1),
            event_name: Some("Sunday Service".to_string()),
            target_delay_secs: 120,
            session_start: Some("2026-01-01T10:00:00Z".to_string()),
            local_buffer_chunks: 3,
            s3_queue_chunks: 15,
            cache_duration_secs: 75.0,
        },
        WsEvent::ObsStatus {
            connected: true,
            streaming: true,
            recording: false,
            stream_timecode: Some("00:05:23".to_string()),
            summary: "streaming".to_string(),
        },
        WsEvent::AuditAppended {
            id: 1,
            ts: "2026-01-01T00:00:00.000Z".to_string(),
            severity: "info".to_string(),
            source: "operator".to_string(),
            event_id: Some(1),
            instance_id: None,
            endpoint: None,
            action: "event_started".to_string(),
            detail: serde_json::json!({}),
        },
        WsEvent::MetricsSample {
            ts_ms: 0,
            event_id: 1,
            instance_id: 1,
            alias: "ep".to_string(),
            chunk_delay_secs: 0.0,
            current_chunk_id: 0,
            chunks_processed: 0,
            alive: true,
        },
    ];

    for event in events {
        let json = serde_json::to_string(&event).unwrap();
        let parsed: WsEvent = serde_json::from_str(&json).unwrap();
        let json2 = serde_json::to_string(&parsed).unwrap();
        assert_eq!(json, json2);
    }
}

#[test]
fn streaming_event_serde() {
    let event = StreamingEvent {
        id: 1,
        name: "test-event".to_string(),
        received_bytes: 0,
        receiving_activated: true,
        delivering_activated: false,
        cache_delay_secs: None,
        created_from: None,
        rescue_video_url: None,
    };
    let json = serde_json::to_string(&event).unwrap();
    let parsed: StreamingEvent = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.name, event.name);
    assert!(parsed.receiving_activated);
    assert_eq!(parsed.cache_delay_secs, None);
}

#[test]
fn chunk_stats_default() {
    let stats = ChunkStats::default();
    assert_eq!(stats.total_chunks, 0);
    assert_eq!(stats.pending_chunks, 0);
    assert_eq!(stats.buffer_duration_secs, 0.0);
}

#[test]
fn inpoint_state_defaults_to_disconnected() {
    let state = InpointState::new();
    assert!(!state.is_connected());
}

#[test]
fn inpoint_state_set_connected() {
    let state = InpointState::new();
    state.set_connected(true);
    assert!(state.is_connected());
}

#[test]
fn inpoint_state_clone_shares_state() {
    let state = InpointState::new();
    let clone = state.clone();
    state.set_connected(true);
    assert!(clone.is_connected());
}

#[test]
fn delivery_metrics_diagnostics_roundtrip() {
    let metrics = DeliveryEndpointMetrics {
        alias: "YouTube".to_string(),
        alive: true,
        current_chunk_id: 42,
        bytes_processed_total: 1048576,
        chunks_processed: 100,
        chunk_delay_secs: 3.2,
        stall_reason: Some("chunk_gap".to_string()),
        ffmpeg_restart_count: 5,

        reconnect_count: 0,
        av_skew_ms: 0,
        fast_delay_target_secs: None,
        last_error: Some("S3 timeout".to_string()),
        is_fast: true,
        delivery_mode: None,
        rescue_eta_secs: None,
        youtube_health: None,
        lifecycle: EndpointLifecycle::Live,
    };
    let json = serde_json::to_string(&metrics).unwrap();
    let parsed: DeliveryEndpointMetrics = serde_json::from_str(&json).unwrap();
    assert_eq!(parsed.stall_reason, Some("chunk_gap".to_string()));
    assert_eq!(parsed.ffmpeg_restart_count, 5);
    assert_eq!(parsed.last_error, Some("S3 timeout".to_string()));
    assert!(parsed.is_fast);
}

#[test]
fn delivery_metrics_missing_diagnostics_defaults() {
    let json = r#"{
        "alias": "Test",
        "alive": true,
        "current_chunk_id": 1,
        "bytes_processed_total": 100,
        "chunks_processed": 5,
        "chunk_delay_secs": 1.0
    }"#;
    let parsed: DeliveryEndpointMetrics = serde_json::from_str(json).unwrap();
    assert_eq!(parsed.stall_reason, None);
    assert_eq!(parsed.ffmpeg_restart_count, 0);
    assert_eq!(parsed.last_error, None);
}

#[test]
fn ws_event_delivery_with_diagnostics_roundtrip() {
    let event = WsEvent::DeliveryStatus {
        instance_name: "test-vps".to_string(),
        status: "running".to_string(),
        server_ip: Some("1.2.3.4".to_string()),
        endpoint_count: 1,
        endpoints: vec![DeliveryEndpointMetrics {
            alias: "YT".to_string(),
            alive: false,
            current_chunk_id: 15,
            bytes_processed_total: 582000,
            chunks_processed: 15,
            chunk_delay_secs: 211.0,
            stall_reason: Some("ffmpeg_crash_loop".to_string()),
            ffmpeg_restart_count: 10,

            reconnect_count: 0,
            av_skew_ms: 0,
            fast_delay_target_secs: None,
            last_error: Some("Connection refused".to_string()),
            is_fast: false,
            delivery_mode: None,
            rescue_eta_secs: None,
            youtube_health: None,
            lifecycle: EndpointLifecycle::Live,
        }],
    };
    let json = serde_json::to_string(&event).unwrap();
    let parsed: WsEvent = serde_json::from_str(&json).unwrap();
    let json2 = serde_json::to_string(&parsed).unwrap();
    assert_eq!(json, json2);
}

#[test]
fn delay_excludes_fast_endpoints() {
    let endpoints = [
        DeliveryEndpointMetrics {
            alias: "FastEP".to_string(),
            alive: true,
            current_chunk_id: 90,
            bytes_processed_total: 1000,
            chunks_processed: 90,
            chunk_delay_secs: 25.0,
            stall_reason: None,
            ffmpeg_restart_count: 0,

            reconnect_count: 0,
            av_skew_ms: 0,
            fast_delay_target_secs: None,
            last_error: None,
            is_fast: true,
            delivery_mode: None,
            rescue_eta_secs: None,
            youtube_health: None,
            lifecycle: EndpointLifecycle::Live,
        },
        DeliveryEndpointMetrics {
            alias: "BufferedEP".to_string(),
            alive: true,
            current_chunk_id: 10,
            bytes_processed_total: 500,
            chunks_processed: 10,
            chunk_delay_secs: 120.0,
            stall_reason: None,
            ffmpeg_restart_count: 0,

            reconnect_count: 0,
            av_skew_ms: 0,
            fast_delay_target_secs: None,
            last_error: None,
            is_fast: false,
            delivery_mode: None,
            rescue_eta_secs: None,
            youtube_health: None,
            lifecycle: EndpointLifecycle::Live,
        },
    ];
    let delay = endpoints
        .iter()
        .filter(|m| !m.is_fast && m.chunk_delay_secs > 0.0)
        .map(|m| m.chunk_delay_secs)
        .fold(f64::MAX, f64::min);
    let delay = if delay == f64::MAX { 0.0 } else { delay };
    assert_eq!(delay, 120.0);
}

#[test]
fn delay_all_fast_falls_back_to_zero() {
    let endpoints = [DeliveryEndpointMetrics {
        alias: "FastOnly".to_string(),
        alive: true,
        current_chunk_id: 90,
        bytes_processed_total: 1000,
        chunks_processed: 90,
        chunk_delay_secs: 25.0,
        stall_reason: None,
        ffmpeg_restart_count: 0,

        reconnect_count: 0,
        av_skew_ms: 0,
        fast_delay_target_secs: None,
        last_error: None,
        is_fast: true,
        delivery_mode: None,
        rescue_eta_secs: None,
        youtube_health: None,
        lifecycle: EndpointLifecycle::Live,
    }];
    let delay = endpoints
        .iter()
        .filter(|m| !m.is_fast && m.chunk_delay_secs > 0.0)
        .map(|m| m.chunk_delay_secs)
        .fold(f64::MAX, f64::min);
    let delay = if delay == f64::MAX { 0.0 } else { delay };
    assert_eq!(delay, 0.0);
}

#[test]
fn delivery_metrics_is_fast_defaults_false() {
    let json = r#"{
        "alias": "Test",
        "alive": true,
        "current_chunk_id": 1,
        "bytes_processed_total": 100,
        "chunks_processed": 5,
        "chunk_delay_secs": 1.0
    }"#;
    let parsed: DeliveryEndpointMetrics = serde_json::from_str(json).unwrap();
    assert!(!parsed.is_fast);
}

#[test]
fn inpoint_state_toggle() {
    let state = InpointState::new();
    state.set_connected(true);
    assert!(state.is_connected());
    state.set_connected(false);
    assert!(!state.is_connected());
}

#[test]
fn endpoint_prefetch_chunks_defaults_to_none_when_missing() {
    let json = r#"{
        "id": 1,
        "alias": "Kiko",
        "service_type": "RTMP",
        "stream_key": "rtmp://x/y",
        "enabled": true,
        "position_last": 0,
        "delivered_bytes": 0,
        "is_fast": true,
        "created_at": "2026-05-10T00:00:00Z",
        "updated_at": "2026-05-10T00:00:00Z"
    }"#;
    let parsed: EndpointConfig = serde_json::from_str(json).unwrap();
    assert!(
        parsed.prefetch_chunks.is_none(),
        "missing field must default to None"
    );
}

#[test]
fn endpoint_prefetch_chunks_round_trips_explicit_value() {
    let json = r#"{
        "id": 1,
        "alias": "Kiko",
        "service_type": "RTMP",
        "stream_key": "rtmp://x/y",
        "enabled": true,
        "position_last": 0,
        "delivered_bytes": 0,
        "is_fast": true,
        "prefetch_chunks": 3,
        "created_at": "2026-05-10T00:00:00Z",
        "updated_at": "2026-05-10T00:00:00Z"
    }"#;
    let parsed: EndpointConfig = serde_json::from_str(json).unwrap();
    assert_eq!(parsed.prefetch_chunks, Some(3));
    // True round-trip: re-serialize and re-parse to catch a future
    // accidental skip / skip_serializing_if attribute.
    let serialized = serde_json::to_string(&parsed).unwrap();
    assert!(serialized.contains("\"prefetch_chunks\":3"));
    let reparsed: EndpointConfig = serde_json::from_str(&serialized).unwrap();
    assert_eq!(reparsed.prefetch_chunks, Some(3));
}

#[test]
fn endpoint_prefetch_chunks_explicit_zero_round_trips() {
    // Operator may set K=0 to force bypass even on a fast endpoint.
    // Distinct from None (auto-resolve to K=1 on fast).
    let json = r#"{
        "id": 1,
        "alias": "x",
        "service_type": "RTMP",
        "stream_key": "rtmp://x/y",
        "enabled": true,
        "position_last": 0,
        "delivered_bytes": 0,
        "is_fast": true,
        "prefetch_chunks": 0,
        "created_at": "2026-05-10T00:00:00Z",
        "updated_at": "2026-05-10T00:00:00Z"
    }"#;
    let parsed: EndpointConfig = serde_json::from_str(json).unwrap();
    assert_eq!(parsed.prefetch_chunks, Some(0));
}

#[test]
fn endpoint_config_serde_preserves_youtube_oauth_id() {
    let json_some = r#"{
        "id": 1, "alias": "ytbb", "service_type": "YT_RTMP", "stream_key": "k",
        "enabled": true, "position_last": 0, "delivered_bytes": 0, "is_fast": false,
        "pusher": "rust", "youtube_oauth_id": 42,
        "created_at": "2026-05-12T00:00:00Z", "updated_at": "2026-05-12T00:00:00Z"
    }"#;
    let parsed: EndpointConfig = serde_json::from_str(json_some).unwrap();
    assert_eq!(parsed.youtube_oauth_id, Some(42));

    // Field absent => None (backward compat with pre-v26 config.json).
    let json_missing = r#"{
        "id": 1, "alias": "ytbb", "service_type": "YT_RTMP", "stream_key": "k",
        "enabled": true, "position_last": 0, "delivered_bytes": 0, "is_fast": false,
        "pusher": "rust",
        "created_at": "2026-05-12T00:00:00Z", "updated_at": "2026-05-12T00:00:00Z"
    }"#;
    let parsed2: EndpointConfig = serde_json::from_str(json_missing).unwrap();
    assert_eq!(parsed2.youtube_oauth_id, None);
}

#[test]
fn inpoint_state_clone_shares_ingest_skew_cells() {
    // #354: the orchestrator wiring comment claims the chunker's writes are
    // "already visible through every InpointState clone" because the skew
    // cells are `Arc`-shared -- prove that claim directly, since it's the
    // whole reason no separate cross-component wiring (like
    // `rtmp_stable_since`'s explicit re-wire) was needed for this feature.
    let state = InpointState::new();
    let clone = state.clone();

    assert_eq!(state.ingest_skew_ms(), 0);
    assert!(!state.ingest_skew_active());

    state.set_ingest_skew_ms(25_470);
    state.set_ingest_skew_active(true);

    assert_eq!(
        clone.ingest_skew_ms(),
        25_470,
        "a write through the original must be visible through the clone"
    );
    assert!(
        clone.ingest_skew_active(),
        "a write through the original must be visible through the clone"
    );

    // And the reverse direction, proving it's a shared cell, not a
    // one-way/coincidental read.
    clone.set_ingest_skew_ms(0);
    clone.set_ingest_skew_active(false);
    assert_eq!(state.ingest_skew_ms(), 0);
    assert!(!state.ingest_skew_active());
}

// #260: `StreamingEvent::rescue_video_missing()` — the single predicate shared
// by the go-live audit warning and (mirrored) the dashboard banner.
fn event_with_rescue(url: Option<&str>) -> StreamingEvent {
    StreamingEvent {
        id: 1,
        name: "9316".to_string(),
        received_bytes: 0,
        receiving_activated: true,
        delivering_activated: false,
        cache_delay_secs: None,
        created_from: None,
        rescue_video_url: url.map(str::to_string),
    }
}

#[test]
fn rescue_video_missing_true_when_none() {
    assert!(
        event_with_rescue(None).rescue_video_missing(),
        "a NULL rescue_video_url is the 9316 case — must warn"
    );
}

#[test]
fn rescue_video_missing_true_when_empty_or_whitespace() {
    assert!(event_with_rescue(Some("")).rescue_video_missing());
    assert!(
        event_with_rescue(Some("   \t ")).rescue_video_missing(),
        "whitespace-only is not a usable URL"
    );
}

#[test]
fn rescue_video_missing_false_when_url_set() {
    assert!(
        !event_with_rescue(Some("https://s3.example/rescue.flv")).rescue_video_missing(),
        "a real URL means a custom rescue clip is configured — no warning"
    );
    assert!(
        !event_with_rescue(Some("  https://s3.example/rescue.flv  ")).rescue_video_missing(),
        "surrounding whitespace must not falsely flag a configured URL as missing"
    );
}

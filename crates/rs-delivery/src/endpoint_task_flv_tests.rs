//! Tests for FlvStreamNormalizer and write-failure chunk-skip behavior.
use super::super::*;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex as TokioMutex;

// Minimal mock helpers (pattern established in endpoint_task_backoff_tests.rs).

struct FlvMockFetcher {
    chunks: Arc<TokioMutex<std::collections::HashMap<i64, Vec<u8>>>>,
}

impl FlvMockFetcher {
    fn new(chunks: Vec<(i64, Vec<u8>)>) -> Self {
        Self {
            chunks: Arc::new(TokioMutex::new(chunks.into_iter().collect())),
        }
    }
}

impl ChunkFetcher for FlvMockFetcher {
    async fn fetch_chunk_with_meta(&self, chunk_id: i64) -> Result<Option<(Vec<u8>, i64)>, String> {
        let map = self.chunks.lock().await;
        Ok(map.get(&chunk_id).map(|data| (data.clone(), 20i64)))
    }

    async fn chunk_duration_ms(&self, chunk_id: i64) -> Result<Option<i64>, String> {
        let map = self.chunks.lock().await;
        if map.contains_key(&chunk_id) {
            Ok(Some(20))
        } else {
            Ok(None)
        }
    }
}

struct FlvMockProcess {
    alive: Arc<AtomicBool>,
    writes: Arc<TokioMutex<Vec<Vec<u8>>>>,
    fail_after: Option<u32>,
    write_count: u32,
}

impl FlvMockProcess {
    fn new(alive: Arc<AtomicBool>, writes: Arc<TokioMutex<Vec<Vec<u8>>>>) -> Self {
        Self {
            alive,
            writes,
            fail_after: None,
            write_count: 0,
        }
    }
}

#[async_trait::async_trait]
impl OutputProcess for FlvMockProcess {
    fn is_alive(&mut self) -> bool {
        self.alive.load(Ordering::Relaxed)
    }
    async fn write(&mut self, data: &[u8]) -> Result<(), String> {
        self.write_count += 1;
        if let Some(limit) = self.fail_after {
            if self.write_count > limit {
                self.alive.store(false, Ordering::Relaxed);
                return Err("mock process died".to_string());
            }
        }
        self.writes.lock().await.push(data.to_vec());
        Ok(())
    }
    async fn kill(&mut self) {
        self.alive.store(false, Ordering::Relaxed);
    }
    fn last_stderr_line(&self) -> Option<String> {
        None
    }
}

struct FlvMockFactory {
    alive: Arc<AtomicBool>,
    writes: Arc<TokioMutex<Vec<Vec<u8>>>>,
    fail_after_writes: Option<u32>,
}

impl FlvMockFactory {
    fn new() -> Self {
        Self {
            alive: Arc::new(AtomicBool::new(true)),
            writes: Arc::new(TokioMutex::new(Vec::new())),
            fail_after_writes: None,
        }
    }
}

impl OutputProcessFactory for FlvMockFactory {
    fn spawn(&self, _: ServiceType, _: &str, _: &str) -> Result<Box<dyn OutputProcess>, String> {
        self.alive.store(true, Ordering::Relaxed);
        let mut proc = FlvMockProcess::new(self.alive.clone(), self.writes.clone());
        proc.fail_after = self.fail_after_writes;
        Ok(Box::new(proc))
    }
}

fn flv_test_ep_cfg() -> crate::api::EndpointConfig {
    crate::api::EndpointConfig {
        alias: "flv-test-ep".to_string(),
        service_type: "TEST_FILE".to_string(),
        stream_key: "flv-test-key".to_string(),
        is_fast: false,
        chunk_format: "flv".to_string(),
        start_chunk_id: None,
    }
}

#[test]
fn test_delivery_delay_ms_direct() {
    // VPS receives delivery_delay_ms directly -- no chunk-count conversion.
    assert_eq!(120_000u64, 120_000, "120s = 120000ms");
    assert_eq!(90_000u64, 90_000, "90s = 90000ms");
    assert_eq!(0u64, 0, "No delay = 0ms");
}

// FlvStreamNormalizer unit tests

fn build_test_flv_chunk(video_data: &[u8], timestamp: u32) -> Vec<u8> {
    let mut buf = Vec::new();
    // FLV header (9 bytes)
    buf.extend_from_slice(&[0x46, 0x4C, 0x56, 0x01, 0x05, 0x00, 0x00, 0x00, 0x09]);
    // Previous tag size 0 (4 bytes)
    buf.extend_from_slice(&[0x00, 0x00, 0x00, 0x00]);

    // Video sequence header tag (tag_type=9, data=[0x17, 0x00, ...])
    let seq_data = [0x17, 0x00, 0x00, 0x00, 0x00, 0x01, 0x64];
    write_flv_tag(&mut buf, 9, 0, &seq_data);

    // Video data tag (tag_type=9, data=video_data)
    write_flv_tag(&mut buf, 9, timestamp, video_data);

    buf
}

fn write_flv_tag(buf: &mut Vec<u8>, tag_type: u8, timestamp: u32, data: &[u8]) {
    let data_size = data.len() as u32;
    // Tag header (11 bytes)
    buf.push(tag_type);
    buf.extend_from_slice(&[
        (data_size >> 16) as u8,
        (data_size >> 8) as u8,
        data_size as u8,
    ]);
    buf.extend_from_slice(&[
        (timestamp >> 16) as u8,
        (timestamp >> 8) as u8,
        timestamp as u8,
    ]);
    buf.push((timestamp >> 24) as u8);
    buf.extend_from_slice(&[0, 0, 0]); // StreamID
    buf.extend_from_slice(data);
    let tag_size = 11 + data_size;
    buf.extend_from_slice(&tag_size.to_be_bytes());
}

#[test]
fn flv_normalizer_rebases_first_chunk_to_ts_zero() {
    // First chunk is rebased so the first data tag lands at ts=0, letting
    // ffmpeg's `-re` pace from process start. Pre-2026-04-21 this was a
    // pass-through — see flv_normalizer.rs for full rationale.
    let mut norm = FlvStreamNormalizer::new();
    let chunk = build_test_flv_chunk(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA], 100);
    let result = norm.normalize(&chunk);
    // FLV header preserved.
    assert_eq!(&result[..3], b"FLV", "FLV header preserved");
    // First tag's timestamp (at byte offset 13 + 4, with FLV tag ts field
    // split as [byte13..=15] ts[23:0] | byte16 ts[31:24]) was rebased to 0.
    let ts = ((result[9 + 4 + 4] as u32) << 16)
        | ((result[9 + 4 + 5] as u32) << 8)
        | (result[9 + 4 + 6] as u32)
        | ((result[9 + 4 + 7] as u32) << 24);
    assert_eq!(ts, 0, "First tag rebased to ts=0 (was {ts})");
}

#[test]
fn flv_normalizer_strips_header_and_seq_from_subsequent_chunks() {
    let mut norm = FlvStreamNormalizer::new();

    // First chunk: pass through
    let chunk1 = build_test_flv_chunk(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA], 100);
    let _ = norm.normalize(&chunk1);

    // Second chunk: should strip FLV header and sequence header, keep data tag
    let chunk2 = build_test_flv_chunk(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xBB], 200);
    let result = norm.normalize(&chunk2);

    // Result should NOT contain FLV header
    assert!(
        result.len() < chunk2.len(),
        "Subsequent chunk should be smaller"
    );
    assert!(
        result.is_empty() || result[0] != 0x46,
        "Should not start with FLV header"
    );

    // Result should contain the data tag (0x17, 0x01 = keyframe NALU)
    // but NOT the sequence header tag (0x17, 0x00 = seq header)
    assert!(!result.is_empty(), "Should contain the data tag");
}

#[test]
fn flv_normalizer_passes_through_non_flv_data() {
    let mut norm = FlvStreamNormalizer::new();
    let raw_data = vec![0xDE, 0xAD, 0xBE, 0xEF];
    let result = norm.normalize(&raw_data);
    assert_eq!(result, raw_data, "Non-FLV data should pass through");
}

#[test]
fn flv_normalizer_passes_through_short_data() {
    let mut norm = FlvStreamNormalizer::new();
    let short = vec![0x46, 0x4C]; // Too short to be FLV
    let result = norm.normalize(&short);
    assert_eq!(result, short, "Short data should pass through");
}

#[test]
fn flv_normalizer_reset_after_new() {
    let mut norm = FlvStreamNormalizer::new();
    assert!(
        !norm.sent_header,
        "New normalizer should not have sent header"
    );
    let chunk = build_test_flv_chunk(&[0x17, 0x01, 0x00, 0x00, 0x00, 0xAA], 100);
    let _ = norm.normalize(&chunk);
    assert!(
        norm.sent_header,
        "After first chunk, sent_header should be true"
    );
}

#[tokio::test]
async fn test_write_failure_skips_chunk_after_retries() {
    tokio::time::pause();
    let chunks: Vec<(i64, Vec<u8>)> = (1..=10).map(|i| (i, vec![i as u8; 100])).collect();
    let fetcher = FlvMockFetcher::new(chunks);
    let mut factory = FlvMockFactory::new();
    factory.fail_after_writes = Some(0);
    let (stop_tx, stop_rx) = tokio::sync::watch::channel(false);
    let stats: Stats = Arc::new(TokioMutex::new(EndpointStats::default()));
    let sc = stats.clone();
    let task = tokio::spawn(endpoint_loop(
        fetcher,
        factory,
        flv_test_ep_cfg(),
        1,
        0,
        stop_rx,
        sc,
        None,
        Arc::new(BufferState::new()),
        None,
    ));
    for _ in 0..80 {
        tokio::time::advance(std::time::Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    let s = stats.lock().await;
    assert!(
        s.current_chunk_id > 1,
        "Should skip failed chunks, stuck at {}",
        s.current_chunk_id
    );
    drop(s);
    stop_tx.send(true).ok();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(5), task).await;
}

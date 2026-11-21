/// S3 chunk fetcher for sequential chunk retrieval.
use crate::api::S3Config;
use s3::Bucket;
use s3::Region;
use s3::creds::Credentials;
use s3::error::S3Error;
use std::time::Duration;
use thiserror::Error;

/// Bound for one S3 request. Without a client-side bound a wedged/blackhole
/// GET hangs on the reqwest client until the OS TCP timeout (minutes),
/// holding a `max_concurrent` fetch semaphore permit, which starves #252
/// recovery fetches and blocks the #276 live-edge hot path.
///
/// For the chunk GET it is an IDLE bound (connect, and every body read): a
/// body that keeps flowing completes however slow it is, a connection that
/// sends nothing for this long fails into the retry. It used to be a TOTAL
/// 20 s request timeout, which cut slow-but-flowing 4.8 MB bodies every 20 s
/// and restarted them from byte 0 (#432: chunk 145 lost for ~2 min, the
/// YouTube output froze 48 s). HEAD has no body, so its total bound stays.
const S3_REQUEST_TIMEOUT_SECS: u64 = 20;

/// Lifetime of the presigned URL of one chunk GET. The request starts at
/// once; this only has to outlive the connect.
const PRESIGN_EXPIRY_SECS: u32 = 300;

/// The whole `source()` chain of an error, so a body cut reads "error
/// decoding response body: operation timed out" instead of only its head.
fn error_chain(e: &(dyn std::error::Error + 'static)) -> String {
    let mut msg = e.to_string();
    let mut cur = e.source();
    while let Some(src) = cur {
        msg.push_str(": ");
        msg.push_str(&src.to_string());
        cur = src.source();
    }
    msg
}

/// An S3 error that means "the object does not exist". Only the typed HTTP
/// status counts: without rust-s3's `fail-on-err` feature a 404 arrives as an
/// `Ok` response with status 404 (handled by the callers); with it, as
/// `HttpFailWithBody(404, _)`. Never the error TEXT: reqwest's message carries
/// the URL, so a timeout on chunk 404 or 1404 used to read as a clean 404,
/// i.e. genuine exhaustion and a false rescue on the VPS (#383).
fn is_not_found(e: &S3Error) -> bool {
    matches!(e, S3Error::HttpFailWithBody(404, _))
}

/// The answer for a failed S3 call: absent for a typed 404, else a fetch
/// error (it then goes into the caller's retry). Its own function so the
/// typed-404 branch is unit-testable: our rust-s3 build never produces it.
fn absent_or_fetch_error<T>(e: S3Error) -> Result<Option<T>, S3FetchError> {
    if is_not_found(&e) {
        Ok(None)
    } else {
        Err(S3FetchError::Fetch(e.to_string()))
    }
}

/// Typed errors for S3 fetching operations.
#[derive(Debug, Error)]
pub enum S3FetchError {
    #[error("S3 credentials error: {0}")]
    Credentials(String),
    #[error("S3 bucket error: {0}")]
    Bucket(String),
    #[error("S3 fetch error: {0}")]
    Fetch(String),
}

/// Chunk data with duration + lifecycle stages from S3 object metadata.
pub struct ChunkData {
    pub data: Vec<u8>,
    pub duration_ms: i64,
    /// Stage A: host clock millis since epoch when the chunker wrote the
    /// chunk to local FS. Backfilled from `x-amz-meta-host-emit-ts` on
    /// the S3 GET response. NULL/None when the chunk was uploaded by a
    /// pre-lifecycle host. Cross-host with VPS clock — see spec section 4.3.
    pub host_emit_ts: Option<i64>,
    /// Stage B: host clock millis since epoch when the uploader received
    /// the S3 200 OK. NOT carried via S3 header (the value is unknown
    /// at PUT time — only after PUT returns). The VPS backfills this
    /// field from the host's `chunk_records.s3_upload_complete_ts` DB
    /// row inside `LifecycleAwarePusher` (Task 18). Always None on the
    /// S3 fetch path; populated downstream.
    pub s3_upload_complete_ts: Option<i64>,
}

pub struct S3Fetcher {
    bucket: Box<Bucket>,
    /// The chunk GET client: idle-bounded, never total-bounded (#432).
    http: reqwest::Client,
    event_identifier: String,
}

impl S3Fetcher {
    pub fn new(config: &S3Config, event_identifier: &str) -> Result<Self, S3FetchError> {
        Self::new_with_timeout(
            config,
            event_identifier,
            Duration::from_secs(S3_REQUEST_TIMEOUT_SECS),
        )
    }

    /// Construct a fetcher with an explicit request bound (`S3_REQUEST_TIMEOUT_SECS`
    /// in production): the HEAD total timeout and the chunk GET's connect and
    /// idle-read timeout. Dependency-injection seam so the bounds are
    /// unit-testable fast.
    fn new_with_timeout(
        config: &S3Config,
        event_identifier: &str,
        request_timeout: Duration,
    ) -> Result<Self, S3FetchError> {
        let region = Region::Custom {
            region: config.region.clone(),
            endpoint: config.endpoint.clone(),
        };
        let credentials = Credentials::new(
            Some(&config.access_key_id),
            Some(&config.secret_access_key),
            None,
            None,
            None,
        )
        .map_err(|e| S3FetchError::Credentials(e.to_string()))?;

        // Bound the per-GET fetch so a wedged/blackhole connection fails fast
        // into the existing retry-with-backoff instead of hanging on the reqwest
        // client for minutes (holding a fetch permit — #252/#276). MUST use
        // `with_request_timeout`, NOT `set_request_timeout`: on the tokio backend
        // the latter only sets a dead struct field, while the former rebuilds the
        // cached reqwest client with `.timeout(request_timeout)` (which bounds the
        // whole request, connect + body). Order matters — `with_path_style` then
        // clones that timeout-bearing client.
        let bucket = Bucket::new(&config.bucket, region, credentials)
            .map_err(|e| S3FetchError::Bucket(e.to_string()))?
            .with_request_timeout(request_timeout)
            .map_err(|e| S3FetchError::Bucket(e.to_string()))?
            .with_path_style();

        let http = reqwest::Client::builder()
            .connect_timeout(request_timeout)
            .read_timeout(request_timeout)
            .build()
            .map_err(|e| S3FetchError::Bucket(error_chain(&e)))?;

        Ok(Self {
            bucket,
            http,
            event_identifier: event_identifier.to_string(),
        })
    }

    /// Fetch a chunk with metadata (duration_ms from S3 object metadata header).
    /// GETs the key `{event}/{seq}.bin` through a presigned URL with the
    /// idle-bounded client (#432), so the `x-amz-meta-*` headers come along.
    pub async fn fetch_chunk_with_meta(
        &self,
        chunk_id: i64,
    ) -> Result<Option<ChunkData>, S3FetchError> {
        let key = format!("{}/{}.bin", self.event_identifier, chunk_id);
        let url = self
            .bucket
            .presign_get(&key, PRESIGN_EXPIRY_SECS, None)
            .await
            .map_err(|e| S3FetchError::Fetch(format!("presign: {e}")))?;

        let response = self
            .http
            .get(&url)
            .send()
            .await
            .map_err(|e| S3FetchError::Fetch(format!("reqwest: {}", error_chain(&e))))?;
        match response.status().as_u16() {
            200 => {}
            404 => return Ok(None),
            code => return Err(S3FetchError::Fetch(format!("status {code}"))),
        }
        let header_i64 = |name: &str| {
            response
                .headers()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<i64>().ok())
        };
        let duration_ms = header_i64("x-amz-meta-duration-ms").unwrap_or(0);
        let host_emit_ts = header_i64("x-amz-meta-host-emit-ts");
        let data = response.bytes().await.map_err(|e| {
            S3FetchError::Fetch(format!("reqwest: {} (chunk {chunk_id})", error_chain(&e)))
        })?;
        // s3_upload_complete_ts (stage B) cannot be carried via S3 header —
        // unknown at PUT time. Always None here; backfilled from
        // chunk_records DB in LifecycleAwarePusher.
        Ok(Some(ChunkData {
            data: data.to_vec(),
            duration_ms,
            host_emit_ts,
            s3_upload_complete_ts: None,
        }))
    }

    /// Get chunk duration via HEAD request (no data download).
    /// Returns `Ok(Some(duration_ms))` for 200, `Ok(None)` for 404.
    pub async fn head_chunk_duration(&self, chunk_id: i64) -> Result<Option<i64>, S3FetchError> {
        let key = format!("{}/{}.bin", self.event_identifier, chunk_id);

        match self.bucket.head_object(&key).await {
            Ok((head, 200)) => {
                let duration_ms = head
                    .metadata
                    .as_ref()
                    .and_then(|m| m.get("duration-ms"))
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(0);
                Ok(Some(duration_ms))
            }
            Ok((_, 404)) => Ok(None),
            Ok((_, code)) => Err(S3FetchError::Fetch(format!("HEAD status {}", code))),
            Err(e) => absent_or_fetch_error(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn chunk_key_format() {
        // Direct key format: {event}/{seq}.bin
        let key = format!("{}/{}.bin", "evt-123", 42);
        assert_eq!(key, "evt-123/42.bin");
    }

    /// #252/#276 regression: a wedged/blackhole S3 GET MUST be bounded by a
    /// client-side request timeout, not hang until the OS TCP timeout (minutes)
    /// holding a fetch semaphore permit (which starves crash-recovery fetches so
    /// a non-fast endpoint never returns to live — the #252 gap — and blocks the
    /// #276 live-edge hot path).
    ///
    /// A local `TcpListener` ACCEPTS the connection (so connect succeeds and the
    /// GET is sent) but never writes a response — the exact wedged nbg1 GET.
    /// Deterministic + fast: an injected 1s bound; the outer 8s ceiling detects a
    /// regression (unbounded GET -> ceiling trips -> RED; bounded -> the fetcher
    /// returns Err(Fetch) in ~1s -> GREEN). This asserts EFFECTIVE behavior, so
    /// it also fails a naive `set_request_timeout` "fix" (a no-op on the tokio
    /// backend — it sets a dead struct field, never rebuilding the reqwest
    /// client; only `with_request_timeout` rebuilds it).
    #[tokio::test]
    async fn wedged_s3_get_is_bounded_by_request_timeout() {
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _accept = tokio::spawn(async move {
            // Hold every accepted socket open, never respond — a true blackhole.
            let mut held = Vec::new();
            while let Ok((sock, _)) = listener.accept().await {
                held.push(sock);
            }
        });

        let config = crate::api::S3Config {
            bucket: "b".to_string(),
            region: "us-east-1".to_string(),
            endpoint: format!("http://{addr}"),
            access_key_id: "k".to_string(),
            secret_access_key: "s".to_string(),
        };

        let fetcher = S3Fetcher::new_with_timeout(&config, "evt", Duration::from_secs(1)).unwrap();

        let result =
            tokio::time::timeout(Duration::from_secs(8), fetcher.fetch_chunk_with_meta(1)).await;

        assert!(
            result.is_ok(),
            "S3 GET must be bounded by a client-side request timeout, not hang on a \
             wedged connection (held a fetch permit -> #252 recovery starvation)"
        );
        assert!(
            matches!(result.unwrap(), Err(S3FetchError::Fetch(_))),
            "a wedged GET must surface as a fetch error (timeout), not a false 404/None"
        );
    }

    /// #432 regression: a body that keeps flowing must complete however long it
    /// takes. Main run 44628833530 lost chunk 145 for ~2 min: every GET of the
    /// 4.8 MB object was cut at the 20 s TOTAL request timeout while its body
    /// was still arriving ("error decoding response body" every ~20 s), and
    /// each retry restarted from byte 0, so the YouTube output froze 48 s.
    ///
    /// A local server sends a 2 MB body in 20 pieces with 150 ms gaps (~3 s in
    /// total) under an injected 1 s bound: never idle for 1 s, but 3x longer
    /// than the bound in total.
    #[tokio::test]
    async fn a_slow_but_flowing_s3_body_completes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        const PIECES: usize = 20;
        const PIECE: usize = 100_000;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _server = tokio::spawn(async move {
            while let Ok((mut sock, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let mut req = [0u8; 4096];
                    let _ = sock.read(&mut req).await;
                    let head = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\
                         x-amz-meta-duration-ms: 2000\r\n\
                         x-amz-meta-host-emit-ts: 1715380800000\r\n\r\n",
                        PIECES * PIECE
                    );
                    if sock.write_all(head.as_bytes()).await.is_err() {
                        return;
                    }
                    for _ in 0..PIECES {
                        tokio::time::sleep(Duration::from_millis(150)).await;
                        if sock.write_all(&[7u8; PIECE]).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });

        let config = crate::api::S3Config {
            bucket: "b".to_string(),
            region: "us-east-1".to_string(),
            endpoint: format!("http://{addr}"),
            access_key_id: "k".to_string(),
            secret_access_key: "s".to_string(),
        };
        let fetcher = S3Fetcher::new_with_timeout(&config, "evt", Duration::from_secs(1)).unwrap();

        let result =
            tokio::time::timeout(Duration::from_secs(20), fetcher.fetch_chunk_with_meta(145))
                .await
                .expect("the fetch itself must end");

        let chunk = result
            .expect("a slow but flowing body must not fail (#432)")
            .expect("the object exists");
        assert_eq!(chunk.data.len(), PIECES * PIECE);
        assert!(chunk.data.iter().all(|&b| b == 7));
        assert_eq!(chunk.duration_ms, 2000);
        assert_eq!(chunk.host_emit_ts, Some(1_715_380_800_000));
    }

    /// Review of #432: a body that stops after the headers must fail at the
    /// idle bound, not hang (reqwest bounds it with a different mechanism
    /// than a server that never answers).
    #[tokio::test]
    async fn a_body_that_stops_midway_is_bounded() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let _server = tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((mut sock, _)) = listener.accept().await {
                let mut req = [0u8; 4096];
                let _ = sock.read(&mut req).await;
                let _ = sock
                    .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 1000000\r\n\r\n")
                    .await;
                let _ = sock.write_all(&[7u8; 10_000]).await;
                held.push(sock); // never sends the rest
            }
        });

        let config = crate::api::S3Config {
            bucket: "b".to_string(),
            region: "us-east-1".to_string(),
            endpoint: format!("http://{addr}"),
            access_key_id: "k".to_string(),
            secret_access_key: "s".to_string(),
        };
        let fetcher = S3Fetcher::new_with_timeout(&config, "evt", Duration::from_secs(1)).unwrap();

        let result =
            tokio::time::timeout(Duration::from_secs(8), fetcher.fetch_chunk_with_meta(1)).await;
        match result {
            Ok(Err(S3FetchError::Fetch(m))) => {
                assert!(
                    m.contains("timed out"),
                    "the cut must read as a timeout: {m}"
                )
            }
            Ok(_) => panic!("a stalled body must fail"),
            Err(_) => panic!("a stalled body must be bounded by the idle timeout"),
        }
    }

    /// Review of #432: a failed chunk GET goes into the audit row and the
    /// dashboard, so its message must not carry the presigned URL (access key
    /// id + a live signature), whose digits could also misclassify the error.
    #[tokio::test]
    async fn a_failed_get_message_carries_no_presigned_url() {
        let fetcher = refusing_fetcher().await;
        match fetcher.fetch_chunk_with_meta(7).await {
            Err(S3FetchError::Fetch(m)) => {
                assert!(!m.contains("X-Amz-"), "presigned query leaked: {m}");
                assert!(!m.contains("evt/7.bin"), "the URL leaked: {m}");
            }
            other => panic!("expected Err(Fetch), got {:?}", other.map(|o| o.is_some())),
        }
    }

    #[test]
    fn only_a_typed_http_404_is_not_found() {
        assert!(is_not_found(&S3Error::HttpFailWithBody(404, String::new())));
        assert!(!is_not_found(&S3Error::HttpFailWithBody(
            500,
            "404".to_string()
        )));
        assert!(!is_not_found(&S3Error::HttpFail));
    }

    #[test]
    fn a_failed_call_is_absent_only_for_a_typed_404() {
        let absent: Result<Option<i64>, _> =
            absent_or_fetch_error(S3Error::HttpFailWithBody(404, String::new()));
        assert!(matches!(absent, Ok(None)));
        let failed: Result<Option<i64>, _> =
            absent_or_fetch_error(S3Error::HttpFailWithBody(503, "404".to_string()));
        assert!(matches!(failed, Err(S3FetchError::Fetch(m)) if m.contains("503")));
    }

    /// A fetcher whose endpoint refuses every connection (the port was bound
    /// and released): each request fails at once with a transport error.
    async fn refusing_fetcher() -> S3Fetcher {
        let port = {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            l.local_addr().unwrap().port()
        };
        let config = crate::api::S3Config {
            bucket: "b".to_string(),
            region: "us-east-1".to_string(),
            endpoint: format!("http://127.0.0.1:{port}"),
            access_key_id: "k".to_string(),
            secret_access_key: "s".to_string(),
        };
        S3Fetcher::new_with_timeout(&config, "evt", Duration::from_secs(2)).unwrap()
    }

    /// A transport error is never "absent", even when its text contains "404":
    /// reqwest's message carries the URL, so a failed GET of chunk 404 / 1404 /
    /// 4041 read as a clean 404 = genuine exhaustion on the VPS (a false rescue
    /// while the stream still exists on S3). Main CI 37686250955 hit the same
    /// misread through a random port containing "404".
    #[tokio::test]
    async fn a_failed_get_of_chunk_404_is_an_error_not_absent() {
        let fetcher = refusing_fetcher().await;
        for chunk_id in [404, 1404, 4041] {
            let r = fetcher.fetch_chunk_with_meta(chunk_id).await;
            assert!(
                matches!(r, Err(S3FetchError::Fetch(_))),
                "chunk {chunk_id}: a refused GET must be Err(Fetch), got {:?}",
                r.map(|o| o.is_some())
            );
        }
    }

    #[tokio::test]
    async fn a_failed_head_of_chunk_404_is_an_error_not_absent() {
        let fetcher = refusing_fetcher().await;
        for chunk_id in [404, 1404, 4041] {
            let r = fetcher.head_chunk_duration(chunk_id).await;
            assert!(
                matches!(r, Err(S3FetchError::Fetch(_))),
                "chunk {chunk_id}: a refused HEAD must be Err(Fetch), got {r:?}"
            );
        }
    }

    #[test]
    fn chunk_data_has_lifecycle_header_fields() {
        // Compile-time assertion: ChunkData carries host_emit_ts and
        // s3_upload_complete_ts (both Option<i64> millis since epoch).
        // The fetcher backfills them from x-amz-meta-* response headers.
        let cd = ChunkData {
            data: vec![],
            duration_ms: 2000,
            host_emit_ts: Some(1715380800000),
            s3_upload_complete_ts: Some(1715380800120),
        };
        assert_eq!(cd.host_emit_ts, Some(1715380800000));
        assert_eq!(cd.s3_upload_complete_ts, Some(1715380800120));
    }
}

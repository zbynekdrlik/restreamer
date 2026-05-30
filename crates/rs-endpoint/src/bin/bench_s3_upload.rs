//! Microbenchmark: measure raw S3 upload throughput from this host
//! to the configured endpoint. Intended to be run MANUALLY from
//! stream.lan against the active Hetzner region (fsn1 after the
//! 2026-05-30 migration) to validate the >= 20 chunks/s acceptance
//! criterion of issue #118.
//!
//! Usage:
//!   S3_BUCKET=restreamer-chunks-fsn1 S3_ENDPOINT=https://fsn1.your-objectstorage.com \
//!   S3_REGION=fsn1 S3_ACCESS_KEY=... S3_SECRET=... \
//!   cargo run --release -p rs-endpoint --bin bench_s3_upload -- --concurrency 16 --count 200

use rs_core::config::S3Config;
use rs_endpoint::s3::S3Client;
use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let concurrency: usize = parse_arg(&args, "--concurrency").unwrap_or(16);
    let count: usize = parse_arg(&args, "--count").unwrap_or(200);

    let cfg = S3Config {
        bucket: std::env::var("S3_BUCKET")?,
        region: std::env::var("S3_REGION")?,
        endpoint: std::env::var("S3_ENDPOINT")?,
        access_key_id: std::env::var("S3_ACCESS_KEY")?,
        secret_access_key: std::env::var("S3_SECRET")?,
    };
    let s3 = std::sync::Arc::new(S3Client::new(&cfg)?);

    let tmp = std::env::temp_dir().join("bench_s3_upload.bin");
    let data: Vec<u8> = (0..102_400).map(|i| (i % 251) as u8).collect();
    tokio::fs::write(&tmp, &data).await?;

    eprintln!("Starting: concurrency={concurrency} count={count}");
    let sem = std::sync::Arc::new(tokio::sync::Semaphore::new(concurrency));
    let started = Instant::now();
    let mut handles = Vec::with_capacity(count);
    for i in 0..count {
        let s3 = s3.clone();
        let sem = sem.clone();
        let path = tmp.clone();
        handles.push(tokio::spawn(async move {
            let _p = sem.acquire_owned().await.unwrap();
            s3.upload_chunk(&path, "bench", i as i64, 2000).await
        }));
    }
    let mut errors = 0;
    for h in handles {
        if h.await?.is_err() {
            errors += 1;
        }
    }
    let elapsed = started.elapsed();
    let rate = count as f64 / elapsed.as_secs_f64();
    let mbps = (count as f64 * 102_400.0 / elapsed.as_secs_f64()) / 1_000_000.0;
    println!(
        "Uploaded {count} chunks in {:.2}s => {:.2} chunks/s ({:.2} MB/s), errors={errors}",
        elapsed.as_secs_f64(),
        rate,
        mbps
    );

    let _ = s3.delete_event_chunks("bench").await;
    Ok(())
}

fn parse_arg<T: std::str::FromStr>(args: &[String], name: &str) -> Option<T> {
    let idx = args.iter().position(|a| a == name)?;
    args.get(idx + 1)?.parse().ok()
}

//! `scripts/install.ps1` writes the `config.json` a FRESH box boots with. Its
//! hardcoded S3 defaults are invisible to the Rust type system, so they rotted:
//! until 2026-07-27 the script defaulted to bucket `restreamer-chunks` with
//! region `eu-central-1` while the project had migrated to the fsn1 bucket
//! `restreamer-chunks-fsn1` (#278). Hetzner buckets are region-bound, so that
//! combination pointed at nothing — and once the old nbg1 bucket was deleted
//! (it had been billed unused since the migration) a fresh install could not
//! upload a single chunk.
//!
//! The oracle is `Config::default()` — the values a binary actually boots with
//! when `config.json` is absent. Pinning the script to a literal repeated here
//! would re-create #348: the next region migration would edit `config.rs`, and
//! script + test would agree with each other while disagreeing with the code.

use rs_core::config::{Config, STANDARD_S3_REGION};

fn install_script() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/install.ps1");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("scripts/install.ps1 must be readable at {path:?}: {e}"))
}

/// Everything after `#`, plus a whole `<# … #>` block, is a PowerShell comment.
/// Both the parser and the scan below work on the CODE part only, so an inline
/// comment can neither corrupt a parsed value nor trip the scan.
fn strip_comments(script: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut in_block = false;
    for line in script.lines() {
        let mut code = line;
        if in_block {
            match code.find("#>") {
                Some(i) => {
                    in_block = false;
                    code = &code[i + 2..];
                }
                None => {
                    out.push(String::new());
                    continue;
                }
            }
        }
        let code = match code.find("<#") {
            Some(i) => {
                in_block = !code[i..].contains("#>");
                &code[..i]
            }
            None => code,
        };
        out.push(code.split('#').next().unwrap_or("").to_string());
    }
    out
}

/// The `s3 = @{ … }` hashtable, comment-free. Scoping to it matters: a later
/// `delivery`/`cloud` block with its own `bucket =` key placed above `s3` would
/// otherwise silently become what this test validates.
fn s3_block(script: &str) -> Vec<String> {
    let lines = strip_comments(script);
    let start = lines
        .iter()
        .position(|l| {
            let t = l.trim_start();
            t.starts_with("s3") && t.contains('=') && t.contains("@{")
        })
        .expect("install.ps1 must have an `s3 = @{` default block");
    let mut depth = 0usize;
    let mut block = Vec::new();
    for line in &lines[start..] {
        depth += line.matches('{').count();
        block.push(line.clone());
        depth -= line.matches('}').count();
        if depth == 0 && block.len() > 1 {
            return block;
        }
    }
    panic!("install.ps1 `s3 = @{{` block is never closed");
}

/// Value of a `key = "value"` line. `split_once` — not `split('=')` — so a
/// value that itself contains `=` (base64 padding, a query string) survives.
fn ps_value(block: &[String], key: &str) -> String {
    let line = block
        .iter()
        .find(|l| {
            let t = l.trim_start();
            t.starts_with(key) && t[key.len()..].trim_start().starts_with('=')
        })
        .unwrap_or_else(|| panic!("install.ps1 s3 block has no `{key} = ...` default"));
    line.split_once('=')
        .expect("the find predicate already proved an `=` is present")
        .1
        .trim()
        .trim_matches('"')
        .to_string()
}

#[test]
fn install_script_s3_defaults_match_the_binary_defaults() {
    let block = s3_block(&install_script());
    let want = Config::default().s3;

    // Positive control: a drifted oracle would let every assertion below pass
    // vacuously against an empty string.
    assert!(
        !want.bucket.is_empty() && !want.region.is_empty() && !want.endpoint.is_empty(),
        "Config::default() has empty S3 defaults — the oracle went blind"
    );

    assert_eq!(
        ps_value(&block, "bucket"),
        want.bucket,
        "install.ps1 default S3 bucket drifted from Config::default()"
    );
    assert_eq!(
        ps_value(&block, "region"),
        want.region,
        "install.ps1 default S3 region drifted from Config::default()"
    );
    assert_eq!(
        ps_value(&block, "endpoint"),
        want.endpoint,
        "install.ps1 default S3 endpoint drifted from Config::default()"
    );
    assert_eq!(
        want.region, STANDARD_S3_REGION,
        "Config::default() itself drifted from STANDARD_S3_REGION"
    );
}

/// The deleted nbg1 bucket must not reappear in EXECUTABLE script lines.
/// Comments are exempt by design — install.ps1 documents why the value changed,
/// and this file's own module docs name the old bucket too.
#[test]
fn install_script_never_references_the_deleted_nbg1_bucket() {
    let standard_bucket = Config::default().s3.bucket;

    for (i, code) in strip_comments(&install_script()).iter().enumerate() {
        let line_no = i + 1;
        assert!(
            !code.contains("nbg1"),
            "install.ps1:{line_no} names the degraded nbg1 region in executable code: {code}"
        );
        // `restreamer-chunks` NOT followed by `-` is the deleted bucket. Testing
        // the suffix directly (rather than "the line also mentions the standard
        // bucket") keeps a line that names BOTH from passing.
        let mut rest = code.as_str();
        while let Some(at) = rest.find("restreamer-chunks") {
            let tail = &rest[at + "restreamer-chunks".len()..];
            assert!(
                tail.starts_with('-'),
                "install.ps1:{line_no} references the deleted bucket \
                 `restreamer-chunks` (standard is `{standard_bucket}`): {code}"
            );
            rest = tail;
        }
    }
}

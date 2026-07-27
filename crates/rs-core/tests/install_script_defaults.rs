//! `scripts/install.ps1` writes the `config.json` a FRESH box boots with. Its
//! hardcoded S3 defaults are invisible to the Rust type system, so they rotted:
//! until 2026-07-27 the script defaulted to bucket `restreamer-chunks` with
//! region `eu-central-1` while the project had migrated to the fsn1 bucket
//! `restreamer-chunks-fsn1` (#278). Hetzner buckets are region-bound, so that
//! combination pointed at nothing — and once the old nbg1 bucket was deleted
//! (it had been billed unused since the migration) a fresh install could not
//! upload a single chunk.
//!
//! This test pins the script's defaults to the same standard the code uses.

use rs_core::config::STANDARD_S3_REGION;

/// The bucket the project actually writes to. Region-bound by Hetzner, hence
/// the region suffix in the name.
const STANDARD_S3_BUCKET: &str = "restreamer-chunks-fsn1";

fn install_script() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../scripts/install.ps1")
        .canonicalize()
        .expect("scripts/install.ps1 must exist relative to crates/rs-core");
    std::fs::read_to_string(&path).expect("scripts/install.ps1 must be readable")
}

/// Pull the value out of a `key = "value"` line in the PowerShell hashtable.
fn ps_default(script: &str, key: &str) -> String {
    script
        .lines()
        .find(|l| {
            let t = l.trim_start();
            t.starts_with(key) && t[key.len()..].trim_start().starts_with('=')
        })
        .unwrap_or_else(|| panic!("install.ps1 has no `{key} = ...` default"))
        .split('=')
        .nth(1)
        .unwrap()
        .trim()
        .trim_matches('"')
        .to_string()
}

#[test]
fn install_script_s3_defaults_match_the_project_standard() {
    let script = install_script();

    assert_eq!(
        ps_default(&script, "bucket"),
        STANDARD_S3_BUCKET,
        "install.ps1 default S3 bucket drifted from the standard bucket"
    );
    assert_eq!(
        ps_default(&script, "region"),
        STANDARD_S3_REGION,
        "install.ps1 default S3 region drifted from rs_core STANDARD_S3_REGION"
    );
    assert_eq!(
        ps_default(&script, "endpoint"),
        format!("https://{STANDARD_S3_REGION}.your-objectstorage.com"),
        "install.ps1 default S3 endpoint must be the standard region's endpoint"
    );
}

/// The deleted bucket must never reappear anywhere in the install script —
/// including in the Hetzner block or a comment that a later edit copies.
#[test]
fn install_script_never_references_the_deleted_nbg1_bucket() {
    let script = install_script();

    for line in script.lines() {
        let is_comment = line.trim_start().starts_with('#');
        assert!(
            is_comment || !line.contains("nbg1"),
            "install.ps1 references the degraded nbg1 region outside a comment: {line}"
        );
        // `restreamer-chunks` without the region suffix is the deleted bucket.
        let bare_old_bucket =
            line.contains("restreamer-chunks") && !line.contains(STANDARD_S3_BUCKET) && !is_comment;
        assert!(
            !bare_old_bucket,
            "install.ps1 references the deleted bucket `restreamer-chunks`: {line}"
        );
    }
}

//! gen_rescue_flv — one-shot generator + verifier for the rescue stream FLV asset.
//!
//! Default mode: regenerates `crates/rs-delivery/assets/default_rescue.flv` from
//! scratch by spawning `ffmpeg` ONCE with bit-exact flags.
//!
//! `--check` mode: regenerates into a scratch path, SHA256-compares against the
//! committed asset, exits 0 only if bytes match. Used by CI to guarantee the
//! committed asset is reproducible from the same ffmpeg invocation.
//!
//! The committed asset is later `include_bytes!`'d into rs-delivery so ffmpeg
//! is NEVER invoked at runtime — only at dev/CI time via this binary.
//!
//! Run from the repo root:
//!   cargo run --bin gen_rescue_flv --manifest-path crates/rs-delivery/Cargo.toml
//!   cargo run --bin gen_rescue_flv --manifest-path crates/rs-delivery/Cargo.toml -- --check

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

const ASSET_PATH: &str = "crates/rs-delivery/assets/default_rescue.flv";
const LOGO_PATH: &str = "crates/rs-delivery/assets/logo.png";
const OVERLAY_TEXT: &str = "Stream temporarily interrupted - please wait";
const DURATION_SECS: u32 = 5;

/// Resolve the absolute path of a repo-relative file.
///
/// Cargo runs this binary from the workspace root when invoked via
/// `cargo run --manifest-path …`, but a manual `cd crates/rs-delivery && cargo run`
/// would resolve to a different cwd. Anchor to `CARGO_MANIFEST_DIR` so behavior
/// is identical regardless of where the user invokes from.
fn repo_path(rel: &str) -> PathBuf {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    // CARGO_MANIFEST_DIR = .../crates/rs-delivery, repo root is two dirs up.
    let repo_root = Path::new(manifest_dir)
        .parent() // .../crates
        .and_then(Path::parent) // repo root
        .expect("CARGO_MANIFEST_DIR has at least two parents");
    repo_root.join(rel)
}

/// Build the ffmpeg argument list. The same flags must produce identical bytes
/// every invocation — that's what `--check` enforces.
///
/// Reproducibility flags:
///   -fflags +bitexact  : strip non-deterministic muxer fields (encoder name, timestamps in header)
///   -flags +bitexact   : codec-level bit-exact mode
///   -flags:v +bitexact : explicit on video stream
///   -flags:a +bitexact : explicit on audio stream
fn build_ffmpeg_args(output: &Path, logo: Option<&Path>) -> Vec<String> {
    // Video filter graph:
    //   - solid #1a1a1a background, 1920x1080 @ 30fps for DURATION_SECS
    //   - centered text overlay (white, ~48px, with subtle shadow)
    //   - optional logo overlay above the text (if logo.png exists)
    //
    // ffmpeg's drawtext requires the text be escaped (':' and '\'' are special).
    // OVERLAY_TEXT contains a '-' which is safe; no escaping needed.
    let mut filter = format!(
        "color=c=0x1a1a1a:s=1920x1080:d={dur}:r=30,format=yuv420p[bg]",
        dur = DURATION_SECS
    );

    // Text overlay. Use a font that ships with most Linux distros AND the CI
    // runners. Fall back to whatever fontconfig picks if path missing.
    let text_filter = format!(
        "[bg]drawtext=text='{text}':fontcolor=white:fontsize=48:\
         x=(w-text_w)/2:y=(h-text_h)/2:\
         shadowcolor=black:shadowx=2:shadowy=2[txt]",
        text = OVERLAY_TEXT
    );
    filter.push(',');
    filter.push_str(&text_filter);

    let final_label = if logo.is_some() {
        // Overlay logo above text: pin logo center-x, y = text_y - logo_h - 40
        // (logo input arrives on stream [1:v]).
        filter.push_str(";[1:v]scale=200:-1[logo]");
        filter.push_str(";[txt][logo]overlay=x=(W-w)/2:y=(H-h)/2-150:format=auto[out]");
        "[out]"
    } else {
        "[txt]"
    };

    let mut args: Vec<String> = vec![
        "-y".into(), // overwrite output
        "-hide_banner".into(),
        "-nostdin".into(),
        // Silent audio source: AAC 48kHz stereo, exactly DURATION_SECS long.
        "-f".into(),
        "lavfi".into(),
        "-i".into(),
        format!("anullsrc=channel_layout=stereo:sample_rate=48000:d={DURATION_SECS}"),
    ];

    if let Some(logo_path) = logo {
        // Logo as a looped still image input — provides [1:v] to the filter graph.
        args.push("-loop".into());
        args.push("1".into());
        args.push("-t".into());
        args.push(DURATION_SECS.to_string());
        args.push("-i".into());
        args.push(logo_path.to_string_lossy().into_owned());
    }

    // Bit-exact / determinism flags BEFORE encoder options so they apply globally.
    args.extend([
        "-fflags".into(),
        "+bitexact".into(),
        "-flags".into(),
        "+bitexact".into(),
        "-flags:v".into(),
        "+bitexact".into(),
        "-flags:a".into(),
        "+bitexact".into(),
    ]);

    // Filter graph + map.
    args.extend([
        "-filter_complex".into(),
        filter,
        "-map".into(),
        final_label.into(),
        "-map".into(),
        "0:a".into(),
    ]);

    // Video encoding: H.264 main profile, 1500k, 2s keyframe (gop=60 @ 30fps).
    args.extend([
        "-c:v".into(),
        "libx264".into(),
        "-profile:v".into(),
        "main".into(),
        "-preset".into(),
        "medium".into(),
        "-b:v".into(),
        "1500k".into(),
        "-maxrate".into(),
        "1500k".into(),
        "-bufsize".into(),
        "3000k".into(),
        "-g".into(),
        "60".into(),
        "-keyint_min".into(),
        "60".into(),
        "-sc_threshold".into(),
        "0".into(),
        "-pix_fmt".into(),
        "yuv420p".into(),
        "-r".into(),
        "30".into(),
        "-x264-params".into(),
        // Disable x264's variable elements that aren't covered by global bitexact.
        "log=-1".into(),
    ]);

    // Audio encoding: AAC 48kHz stereo, 64k. Use fdk-aac? No — not available on
    // all distros. Use libfaac? No. Use built-in 'aac' — deterministic at fixed bitrate.
    args.extend([
        "-c:a".into(),
        "aac".into(),
        "-b:a".into(),
        "64k".into(),
        "-ar".into(),
        "48000".into(),
        "-ac".into(),
        "2".into(),
    ]);

    // FLV output.
    args.extend([
        "-f".into(),
        "flv".into(),
        output.to_string_lossy().into_owned(),
    ]);

    args
}

/// Spawn ffmpeg ONCE with the build_ffmpeg_args() flag set. Returns Err on
/// non-zero exit (stderr captured for diagnostics).
fn run_ffmpeg(output: &Path, logo: Option<&Path>) -> Result<(), String> {
    let args = build_ffmpeg_args(output, logo);
    eprintln!("gen_rescue_flv: spawning ffmpeg with {} args", args.len());

    let out = Command::new("ffmpeg")
        .args(&args)
        .output()
        .map_err(|e| format!("failed to spawn ffmpeg: {e} — is ffmpeg installed and on PATH?"))?;

    if !out.status.success() {
        let stderr = String::from_utf8_lossy(&out.stderr);
        return Err(format!(
            "ffmpeg exited with status {}\n--- stderr (last 4 KiB) ---\n{}",
            out.status,
            tail_chars(&stderr, 4096)
        ));
    }
    Ok(())
}

fn tail_chars(s: &str, n: usize) -> &str {
    if s.len() <= n {
        s
    } else {
        // Char-boundary safe truncation from the end.
        let start = s.len() - n;
        let mut start = start;
        while !s.is_char_boundary(start) && start < s.len() {
            start += 1;
        }
        &s[start..]
    }
}

fn sha256_file(path: &Path) -> Result<String, String> {
    let bytes =
        std::fs::read(path).map_err(|e| format!("failed to read {}: {e}", path.display()))?;
    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    Ok(format!("{:x}", hasher.finalize()))
}

fn generate(output: &Path) -> Result<u64, String> {
    let logo = repo_path(LOGO_PATH);
    let logo_arg = if logo.exists() {
        eprintln!("gen_rescue_flv: using logo {}", logo.display());
        Some(logo.as_path())
    } else {
        eprintln!(
            "gen_rescue_flv: no logo at {} — text-only output",
            logo.display()
        );
        None
    };

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
    }

    run_ffmpeg(output, logo_arg)?;

    let meta = std::fs::metadata(output)
        .map_err(|e| format!("output {} missing after ffmpeg run: {e}", output.display()))?;
    let size = meta.len();
    // Sanity window: a 5s 1080p30 H.264+AAC FLV of a static frame compresses
    // very efficiently (libx264 obeys the -b:v target as a CAP, not a floor).
    // Realistic range: ~50KB (mostly-static frame, well below the 1500k cap)
    // up to ~2MB (worst case with motion / logo overlay).
    if !(20 * 1024..=2 * 1024 * 1024).contains(&size) {
        return Err(format!(
            "output size {} bytes is outside expected 20KB..2MB window (something's wrong)",
            size
        ));
    }
    Ok(size)
}

fn mode_generate() -> Result<(), String> {
    let asset = repo_path(ASSET_PATH);
    let size = generate(&asset)?;
    let hash = sha256_file(&asset)?;
    println!(
        "WROTE {} ({} bytes, sha256={})",
        asset.display(),
        size,
        hash
    );
    Ok(())
}

fn mode_check() -> Result<(), String> {
    let committed = repo_path(ASSET_PATH);
    if !committed.exists() {
        return Err(format!(
            "committed asset missing: {} — run without --check to generate it",
            committed.display()
        ));
    }
    let committed_hash = sha256_file(&committed)?;

    // Generate to a scratch path in the system temp dir (e.g. /tmp).
    let scratch =
        std::env::temp_dir().join(format!("gen_rescue_flv_check_{}.flv", std::process::id()));
    let _cleanup = ScratchCleanup(scratch.clone());
    generate(&scratch)?;
    let fresh_hash = sha256_file(&scratch)?;

    if committed_hash == fresh_hash {
        println!(
            "OK {} matches freshly generated bytes (sha256={})",
            committed.display(),
            committed_hash
        );
        Ok(())
    } else {
        Err(format!(
            "MISMATCH\n  committed: {} sha256={}\n  generated: {} sha256={}\n\
             ffmpeg output is not bit-reproducible — adjust flags and re-generate.",
            committed.display(),
            committed_hash,
            scratch.display(),
            fresh_hash,
        ))
    }
}

/// Best-effort delete of the scratch file when --check exits.
struct ScratchCleanup(PathBuf);
impl Drop for ScratchCleanup {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();
    let result = match args.get(1).map(String::as_str) {
        Some("--check") => mode_check(),
        Some("--help") | Some("-h") => {
            println!(
                "Usage:\n  gen_rescue_flv            regenerate {}\n  gen_rescue_flv --check    verify committed asset matches a fresh build",
                ASSET_PATH
            );
            return ExitCode::SUCCESS;
        }
        Some(other) => Err(format!("unknown arg: {other} (use --help)")),
        None => mode_generate(),
    };

    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("ERROR: {e}");
            ExitCode::FAILURE
        }
    }
}

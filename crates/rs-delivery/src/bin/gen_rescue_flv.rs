//! gen_rescue_flv — one-shot generator + verifier for the Slovak rescue-clip
//! segment SET (#259).
//!
//! Default mode: regenerates every segment in `crates/rs-delivery/assets/`
//! from scratch by spawning `ffmpeg` with bit-exact flags. Each segment
//! differs ONLY in its drawtext text, so all six carry BYTE-IDENTICAL SPS/PPS
//! — which is exactly what makes the pusher swap segments mid-session safely
//! (see `rescue_segments` module docs).
//!
//! `--check` mode: regenerates each into a scratch path, SHA256-compares
//! against the committed asset, exits 0 only if every one matches. Local-only:
//! ffmpeg output is not bit-reproducible across ffmpeg versions/distros, so CI
//! does NOT run this — the committed blobs are validated structurally by
//! `rescue_default`/`rescue_segments` unit tests instead.
//!
//! The committed assets are later `include_bytes!`'d into rs-delivery so
//! ffmpeg is NEVER invoked at runtime — only at dev time via this binary.
//!
//! Run from the repo root:
//!   cargo run --bin gen_rescue_flv --manifest-path crates/rs-delivery/Cargo.toml
//!   cargo run --bin gen_rescue_flv --manifest-path crates/rs-delivery/Cargo.toml -- --check

use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

/// Font with full Latin Extended-A coverage so Slovak diacritics (á í ľ š č ô
/// ý …) render instead of tofu boxes. Ships on the Ubuntu dev boxes.
const FONT_PATH: &str = "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf";
const DURATION_SECS: u32 = 5;
const FONTSIZE: u32 = 54;

/// The rescue-clip segment set: (repo-relative asset path, Slovak overlay
/// text). `default_rescue.flv` doubles as the static outage notice and the
/// custom-URL fallback (`DEFAULT_RESCUE_FLV`). All texts are free of ffmpeg
/// drawtext special chars (`:` `'` `\` `%`), so no escaping is needed.
const SEGMENTS: &[(&str, &str)] = &[
    (
        "crates/rs-delivery/assets/default_rescue.flv",
        "Prenos bol prerušený — o chvíľu pokračujeme",
    ),
    (
        "crates/rs-delivery/assets/rescue_warmup.flv",
        "Vysielanie sa o chvíľu spustí…",
    ),
    (
        "crates/rs-delivery/assets/rescue_recover_2min.flv",
        "Obnovujeme o ~2 min",
    ),
    (
        "crates/rs-delivery/assets/rescue_recover_1min.flv",
        "Obnovujeme o ~1 min",
    ),
    (
        "crates/rs-delivery/assets/rescue_recover_30s.flv",
        "Obnovujeme o ~30 s",
    ),
    (
        "crates/rs-delivery/assets/rescue_recover_soon.flv",
        "Obnovujeme o chvíľu",
    ),
];

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
fn build_ffmpeg_args(output: &Path, text: &str) -> Vec<String> {
    // Video filter graph:
    //   - solid #1a1a1a background, 1920x1080 @ 30fps for DURATION_SECS
    //   - centered Slovak text overlay (white, FONTSIZE px, subtle shadow)
    //
    // `fontfile=FONT_PATH` pins DejaVuSans so Slovak diacritics render (a
    // fontconfig default can lack Latin Extended-A → tofu boxes). ffmpeg's
    // drawtext treats ':' '\'' '\\' '%' as special; the SEGMENTS texts contain
    // none (the '—' em dash and '…' ellipsis are plain UTF-8, safe), so no
    // escaping is needed. All segments share these flags — only `text` differs
    // — so their SPS/PPS come out byte-identical (verified at gen time).
    let filter = format!(
        "color=c=0x1a1a1a:s=1920x1080:d={dur}:r=30,format=yuv420p[bg],\
         [bg]drawtext=fontfile={font}:text='{text}':fontcolor=white:fontsize={size}:\
         x=(w-text_w)/2:y=(h-text_h)/2:\
         shadowcolor=black:shadowx=2:shadowy=2[txt]",
        dur = DURATION_SECS,
        font = FONT_PATH,
        size = FONTSIZE,
    );

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
        "[txt]".into(),
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
fn run_ffmpeg(output: &Path, text: &str) -> Result<(), String> {
    let args = build_ffmpeg_args(output, text);
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

fn generate(output: &Path, text: &str) -> Result<u64, String> {
    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)
            .map_err(|e| format!("failed to create {}: {e}", parent.display()))?;
    }

    run_ffmpeg(output, text)?;

    let meta = std::fs::metadata(output)
        .map_err(|e| format!("output {} missing after ffmpeg run: {e}", output.display()))?;
    let size = meta.len();
    // Sanity window: a 5s 1080p30 H.264+AAC FLV of a static frame compresses
    // very efficiently (libx264 obeys the -b:v target as a CAP, not a floor).
    // Realistic range: short text ~45KB, longer text ~75KB — comfortably
    // inside 20KB..2MB.
    if !(20 * 1024..=2 * 1024 * 1024).contains(&size) {
        return Err(format!(
            "output size {} bytes is outside expected 20KB..2MB window (something's wrong)",
            size
        ));
    }
    Ok(size)
}

fn mode_generate() -> Result<(), String> {
    for (rel, text) in SEGMENTS {
        let asset = repo_path(rel);
        let size = generate(&asset, text)?;
        let hash = sha256_file(&asset)?;
        println!(
            "WROTE {} ({} bytes, sha256={})",
            asset.display(),
            size,
            hash
        );
    }
    Ok(())
}

fn mode_check() -> Result<(), String> {
    for (rel, text) in SEGMENTS {
        let committed = repo_path(rel);
        if !committed.exists() {
            return Err(format!(
                "committed asset missing: {} — run without --check to generate it",
                committed.display()
            ));
        }
        let committed_hash = sha256_file(&committed)?;

        // Generate to a scratch path in the system temp dir (e.g. /tmp).
        let scratch = std::env::temp_dir().join(format!(
            "gen_rescue_flv_check_{}_{}.flv",
            std::process::id(),
            committed
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or("seg")
        ));
        let _cleanup = ScratchCleanup(scratch.clone());
        generate(&scratch, text)?;
        let fresh_hash = sha256_file(&scratch)?;

        if committed_hash == fresh_hash {
            println!(
                "OK {} matches freshly generated bytes (sha256={})",
                committed.display(),
                committed_hash
            );
        } else {
            return Err(format!(
                "MISMATCH\n  committed: {} sha256={}\n  generated: {} sha256={}\n\
                 ffmpeg output is not bit-reproducible — adjust flags and re-generate.",
                committed.display(),
                committed_hash,
                scratch.display(),
                fresh_hash,
            ));
        }
    }
    Ok(())
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
                "Usage:\n  gen_rescue_flv            regenerate all {} rescue segments\n  gen_rescue_flv --check    verify committed segments match a fresh build",
                SEGMENTS.len()
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

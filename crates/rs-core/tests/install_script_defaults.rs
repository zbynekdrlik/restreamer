//! `scripts/install.ps1` writes the `config.json` a FRESH box boots with. Its
//! hardcoded S3 defaults are invisible to the Rust type system, so they rotted:
//! until 2026-07-27 the script defaulted to bucket `restreamer-chunks` with
//! region `eu-central-1` while the project had migrated to the fsn1 bucket
//! `restreamer-chunks-fsn1` (#278). Hetzner buckets are region-bound, so that
//! combination pointed at nothing — and once the old nbg1 bucket was deleted
//! (it had been billed unused since the migration) a fresh install could not
//! upload a single chunk.
//!
//! Two independent oracles, because either alone is escapable:
//!
//! 1. `Config::default()` — the values a binary boots with when `config.json`
//!    is absent. Pinning the script to a literal repeated in this file would
//!    re-create #348: the next migration would edit `config.rs`, and script +
//!    test would agree with each other while disagreeing with the code.
//! 2. The region-DERIVED shape of the other two values. Hetzner buckets are
//!    region-bound, so `<bucket>` must end `-<region>` and the endpoint host
//!    must be `<region>.your-objectstorage.com`. Without this, a copy-paste
//!    slip that mirrors the SAME wrong endpoint into both config.rs and the
//!    script (fsn1 bucket, hel1 endpoint) passes oracle 1 silently.

use rs_core::config::{Config, STANDARD_S3_REGION};

/// One physical line of the script, split into what the PowerShell parser would
/// see. `code` keeps string literals intact (the values live in them); `skeleton`
/// blanks their contents, so a brace, a `;` or a `bucket =` inside a string can
/// neither shift the block depth nor be mistaken for a separator or a key.
struct CodeLine {
    code: String,
    skeleton: String,
    /// The line's `;`-separated statements. A PowerShell hashtable may be
    /// written on one line (`s3 = @{ bucket = "x"; region = "y" }`), so key
    /// lookup works per statement, not per line.
    segments: Vec<CodeLine>,
}

impl CodeLine {
    fn leaf(code: String, skeleton: String) -> Self {
        Self {
            code,
            skeleton,
            segments: Vec::new(),
        }
    }

    /// The statements to search for a `key = value`: the `;`-separated parts if
    /// there are several, else the line itself.
    fn statements(&self) -> Vec<&CodeLine> {
        if self.segments.is_empty() {
            vec![self]
        } else {
            self.segments.iter().collect()
        }
    }
}

/// Strip PowerShell comments — `#` to end-of-line and `<# … #>` blocks — with
/// enough string awareness that a `#` inside a literal (a URL fragment, a
/// password) is NOT treated as a comment, and a `<#` inside a `#` comment does
/// NOT open a block. Output stays 1:1 with input lines, so line numbers hold.
fn code_lines(script: &str) -> Vec<CodeLine> {
    let mut out = Vec::new();
    let mut in_block = false;

    for line in script.lines() {
        let mut code = String::new();
        let mut skeleton = String::new();
        let bytes: Vec<char> = line.chars().collect();
        let mut i = 0;
        let mut quote: Option<char> = None;

        while i < bytes.len() {
            let c = bytes[i];

            if in_block {
                if c == '#' && bytes.get(i + 1) == Some(&'>') {
                    in_block = false;
                    i += 2;
                } else {
                    i += 1;
                }
                continue;
            }

            if let Some(q) = quote {
                code.push(c);
                skeleton.push(if c == q { c } else { ' ' });
                if c == q {
                    quote = None;
                }
                i += 1;
                continue;
            }

            // Outside any string: comments start here and nowhere else.
            if c == '<' && bytes.get(i + 1) == Some(&'#') {
                in_block = true;
                i += 2;
                continue;
            }
            if c == '#' {
                break; // rest of the line is a comment
            }
            if c == '"' || c == '\'' {
                quote = Some(c);
            }
            code.push(c);
            skeleton.push(c);
            i += 1;
        }

        let segments = split_statements(&code, &skeleton);
        out.push(CodeLine {
            code,
            skeleton,
            segments,
        });
    }
    out
}

/// Split a line on the statement separators `;` `{` `}` that sit OUTSIDE string
/// literals — located in the skeleton, then applied to both strings by char
/// index (their byte lengths can differ, since blanking a multi-byte char in the
/// skeleton shortens it). Braces count as separators so a one-line hashtable
/// (`s3 = @{ bucket = "x"; region = "y" }`) yields its keys as statements.
fn split_statements(code: &str, skeleton: &str) -> Vec<CodeLine> {
    const SEPARATORS: [char; 3] = [';', '{', '}'];
    let sk: Vec<char> = skeleton.chars().collect();
    if !sk.iter().any(|c| SEPARATORS.contains(c)) {
        return Vec::new();
    }
    let cd: Vec<char> = code.chars().collect();
    debug_assert_eq!(
        cd.len(),
        sk.len(),
        "code and skeleton must stay char-aligned"
    );

    let mut out = Vec::new();
    let mut start = 0;
    for end in sk
        .iter()
        .enumerate()
        .filter(|(_, c)| SEPARATORS.contains(c))
        .map(|(i, _)| i)
        .chain(std::iter::once(sk.len()))
    {
        if end > start {
            out.push(CodeLine::leaf(
                cd[start..end].iter().collect(),
                sk[start..end].iter().collect(),
            ));
        }
        start = end + 1;
    }
    out
}

/// The `s3 = @{ … }` hashtable. Scoping to it matters: a `bucket =` key in any
/// block placed above `s3` would otherwise silently become what is validated.
fn s3_block(lines: &[CodeLine]) -> Vec<&CodeLine> {
    let start = lines
        .iter()
        .position(|l| {
            // `strip_prefix` + `=`, not `starts_with("s3")` — otherwise a decoy
            // `s3_archive = @{` above the real block matches first.
            l.skeleton
                .trim_start()
                .strip_prefix("s3")
                .is_some_and(|rest| rest.trim_start().starts_with('='))
                && l.skeleton.contains("@{")
        })
        .expect("install.ps1 must have an `s3 = @{` default block");

    let mut depth = 0i32;
    let mut block = Vec::new();
    for line in &lines[start..] {
        depth += line.skeleton.matches('{').count() as i32;
        depth -= line.skeleton.matches('}').count() as i32;
        block.push(line);
        if depth <= 0 {
            return block;
        }
    }
    panic!("install.ps1 `s3 = @{{` block is never closed");
}

/// Value of a `key = "value"` line. `split_once` — not `split('=')` — so a value
/// that itself contains `=` (base64 padding, a query string) survives.
fn ps_value(block: &[&CodeLine], key: &str) -> String {
    let stmt = block
        .iter()
        .flat_map(|l| l.statements())
        .find(|l| {
            let t = l.skeleton.trim_start();
            t.starts_with(key) && t[key.len()..].trim_start().starts_with('=')
        })
        .unwrap_or_else(|| panic!("install.ps1 s3 block has no `{key} = ...` default"));

    let raw = stmt
        .code
        .split_once('=')
        .expect("the find predicate already proved an `=` is present")
        .1
        .trim();

    // Both quote styles are valid PowerShell; strip one matching pair only, so a
    // malformed `"""x"""` fails the comparison instead of being tidied away.
    for q in ['"', '\''] {
        if raw.len() >= 2 && raw.starts_with(q) && raw.ends_with(q) {
            return raw[1..raw.len() - 1].to_string();
        }
    }
    raw.to_string()
}

#[test]
fn install_script_s3_defaults_match_the_binary_defaults() {
    let lines = code_lines(&install_script());
    let block = s3_block(&lines);
    let want = Config::default().s3;

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
}

/// Oracle 2 — the region-derived shape. Guards the copy-paste slip that mirrors
/// the same wrong value into BOTH config.rs and the script, which oracle 1
/// cannot see.
#[test]
fn binary_defaults_are_internally_consistent_with_the_region() {
    let want = Config::default().s3;

    assert_eq!(
        want.region, STANDARD_S3_REGION,
        "Config::default() drifted from STANDARD_S3_REGION"
    );
    assert_eq!(
        want.endpoint,
        format!("https://{}.your-objectstorage.com", want.region),
        "S3 endpoint host must be the configured region's — Hetzner buckets are region-bound"
    );
    assert!(
        want.bucket.ends_with(&format!("-{}", want.region)),
        "S3 bucket `{}` must carry the `-{}` region suffix — buckets are region-bound, \
         and a name without it was the deleted nbg1 bucket (#348)",
        want.bucket,
        want.region
    );
}

/// The deleted nbg1 bucket must not reappear in EXECUTABLE script lines.
/// Comments are exempt by design — install.ps1 documents why the value changed.
#[test]
fn install_script_never_references_the_deleted_nbg1_bucket() {
    let standard_bucket = Config::default().s3.bucket;
    let lines = code_lines(&install_script());

    // Positive control: without it, anything that reduced `code_lines` to blanks
    // (an unterminated block comment, the defaults moving to a JSON template)
    // would leave this test green while checking nothing at all.
    assert!(
        lines.iter().any(|l| l.code.contains(&standard_bucket)),
        "no executable line names `{standard_bucket}` — the scan below is checking nothing"
    );

    for (i, line) in lines.iter().enumerate() {
        let line_no = i + 1;
        let code = &line.code;
        assert!(
            !code.contains("nbg1"),
            "install.ps1:{line_no} names the degraded nbg1 region in executable code: {code}"
        );
        // Every `restreamer-chunks…` occurrence must be the standard bucket
        // EXACTLY — `-hel1` is as wrong as the bare deleted name.
        let mut rest = code.as_str();
        while let Some(at) = rest.find("restreamer-chunks") {
            let from = &rest[at..];
            assert!(
                from.starts_with(&standard_bucket),
                "install.ps1:{line_no} references a non-standard bucket \
                 (standard is `{standard_bucket}`): {code}"
            );
            rest = &from["restreamer-chunks".len()..];
        }
    }
}

/// True if an EXECUTABLE `New-NetFirewallRule` statement opens inbound TCP on
/// `port` with an Allow action. `code_lines` has already stripped comments, so a
/// rule that only appears in a comment (documented but never run) cannot satisfy
/// this — the rule must be live script code.
fn declares_inbound_tcp_allow_rule(lines: &[CodeLine], port: u16) -> bool {
    let port_flag = format!("-LocalPort {port}");
    lines.iter().any(|l| {
        let c = &l.code;
        c.contains("New-NetFirewallRule")
            && c.contains(&port_flag)
            && c.contains("-Protocol TCP")
            && c.contains("-Direction Inbound")
            && c.contains("-Action Allow")
    })
}

/// #108: a fresh install must open the LAN-facing ports itself. Windows' default
/// inbound policy blocks unsolicited TCP, so without these rules the dashboard
/// (8910) is unreachable from other hosts and remote OBS cannot push RTMP (1234)
/// — verified live on stream.lan, where only an 8910 rule (from the CI deploy's
/// own block) existed and 1234 had no rule at all.
#[test]
fn install_script_opens_firewall_for_dashboard_and_rtmp_ports() {
    let lines = code_lines(&install_script());
    assert!(
        declares_inbound_tcp_allow_rule(&lines, 8910),
        "install.ps1 must declare an inbound TCP Allow firewall rule for the \
         dashboard/API port 8910 (LAN access is blocked without it, #108)"
    );
    assert!(
        declares_inbound_tcp_allow_rule(&lines, 1234),
        "install.ps1 must declare an inbound TCP Allow firewall rule for the \
         RTMP ingest port 1234 (remote OBS push is blocked without it, #108)"
    );
}

/// The firewall rules must be idempotent — re-running the installer (redeploys
/// are routine) must not stack duplicate rules. The proven pattern (mirrored
/// from the CI deploy block) is a `Remove-NetFirewallRule` before each
/// `New-NetFirewallRule` for the same DisplayName.
#[test]
fn install_script_firewall_rules_are_idempotent() {
    let lines = code_lines(&install_script());
    let news = lines
        .iter()
        .filter(|l| l.code.contains("New-NetFirewallRule"))
        .count();
    let removes = lines
        .iter()
        .filter(|l| l.code.contains("Remove-NetFirewallRule"))
        .count();
    assert!(
        news >= 2,
        "expected at least 2 New-NetFirewallRule statements (8910 + 1234), found {news}"
    );
    assert!(
        removes >= news,
        "each New-NetFirewallRule must be preceded by a Remove-NetFirewallRule for the same \
         DisplayName so re-running install.ps1 does not stack duplicate rules \
         (found {removes} Remove vs {news} New)"
    );
}

fn install_script() -> String {
    let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("../../scripts/install.ps1");
    std::fs::read_to_string(&path)
        .unwrap_or_else(|e| panic!("scripts/install.ps1 must be readable at {path:?}: {e}"))
}

//! Windows half of the RTMP port-holder lookup (#106): run `netstat -ano`
//! and `tasklist`, and hand their output to the cross-platform parsers in
//! `rtmp_bind.rs`, which are unit-tested on every platform.
//!
//! Only raw command glue lives here. cargo-mutants does not evaluate `cfg`,
//! so a mutant in this `#[cfg(windows)]` file would be generated on Linux,
//! never compiled, and reported MISSED; `.cargo/mutants.toml` excludes the
//! file for that reason (#367).

use std::process::Command;

use super::{format_holder, image_name_from_tasklist, listening_pid_from_netstat};

pub(super) fn identify_port_holder_windows(port: u16) -> Option<String> {
    let out = Command::new("netstat").args(["-ano"]).output().ok()?;
    let pid = listening_pid_from_netstat(&String::from_utf8_lossy(&out.stdout), port)?;
    let name = Command::new("tasklist")
        .args(["/FI", &format!("PID eq {pid}"), "/FO", "CSV", "/NH"])
        .output()
        .ok()
        .and_then(|o| image_name_from_tasklist(&String::from_utf8_lossy(&o.stdout)));
    Some(format_holder(&pid, name.as_deref()))
}

// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use std::path::PathBuf;

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "Restreamer")]
#[command(about = "Church live-streaming infrastructure client")]
struct Args {
    /// Run in headless/service mode without GUI (for CI/service deployments)
    #[arg(long)]
    headless: bool,

    /// Install a newer, signed release when the box is idle, then exit
    /// (Linux, run as root by restreamer-upgrade.service, #323)
    #[arg(long)]
    upgrade_check: bool,

    /// Verify a release .deb against its .sig with the built-in updater key,
    /// then exit (release CI, #323)
    #[arg(long, num_args = 2, value_names = ["DEB", "SIG"])]
    verify_update: Option<Vec<PathBuf>>,

    /// Restart the desktop user's restreamer unit the way the upgrade check
    /// does, then exit (CI test of that path, #323)
    #[arg(long, hide = true)]
    restart_app: bool,
}

fn main() {
    let args = Args::parse();

    if args.upgrade_check {
        std::process::exit(rs_runtime::self_upgrade::run_cli());
    }
    if let Some([deb, sig]) = args.verify_update.as_deref() {
        std::process::exit(rs_runtime::self_upgrade::verify_files_cli(deb, sig));
    }
    if args.restart_app {
        std::process::exit(rs_runtime::self_upgrade::restart_app_cli());
    }
    if args.headless {
        restreamer_lib::run_headless();
    } else {
        restreamer_lib::run();
    }
}

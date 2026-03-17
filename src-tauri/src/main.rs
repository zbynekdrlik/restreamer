// Prevents additional console window on Windows in release, DO NOT REMOVE!!
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "Restreamer")]
#[command(about = "Church live-streaming infrastructure client")]
struct Args {
    /// Run in headless/service mode without GUI (for CI/service deployments)
    #[arg(long)]
    headless: bool,
}

fn main() {
    let args = Args::parse();

    if args.headless {
        restreamer_lib::run_headless();
    } else {
        restreamer_lib::run();
    }
}

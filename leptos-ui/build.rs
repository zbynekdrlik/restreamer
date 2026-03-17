use std::process::Command;

fn main() {
    // Get version from environment or fall back to Cargo.toml version
    let version =
        std::env::var("BUILD_VERSION").unwrap_or_else(|_| env!("CARGO_PKG_VERSION").to_string());

    // Get build timestamp from environment or generate current time
    let timestamp = std::env::var("BUILD_TIMESTAMP").unwrap_or_else(|_| {
        let output = Command::new("date")
            .args(["+%b %d %H:%M"])
            .output()
            .ok()
            .and_then(|o| String::from_utf8(o.stdout).ok())
            .map(|s| s.trim().to_string())
            .unwrap_or_else(|| "dev".to_string());
        output
    });

    println!("cargo:rustc-env=BUILD_VERSION={}", version);
    println!("cargo:rustc-env=BUILD_TIMESTAMP={}", timestamp);

    // Re-run if these env vars change
    println!("cargo:rerun-if-env-changed=BUILD_VERSION");
    println!("cargo:rerun-if-env-changed=BUILD_TIMESTAMP");
}

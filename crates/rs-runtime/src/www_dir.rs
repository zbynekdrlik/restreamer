//! Where the bundled dashboard (`www`, the Leptos/WASM build) lives,
//! relative to the running executable.

use std::path::{Path, PathBuf};

/// Directories that may hold the dashboard, most specific first.
/// - Windows: `www` next to `Restreamer.exe` (`C:\Program Files\Restreamer\www`).
/// - Linux .deb (stream-pp): the exe is `/usr/bin/Restreamer`; Tauri puts
///   `bundle.resources` under `/usr/lib/<productName>/`, so `www` is
///   `/usr/lib/Restreamer/www`.
pub(crate) fn www_candidates(exe_dir: &Path) -> Vec<PathBuf> {
    vec![
        exe_dir.join("www"),
        exe_dir
            .join("..")
            .join("lib")
            .join("Restreamer")
            .join("www"),
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn windows_layout_is_www_next_to_the_exe() {
        let dir = Path::new(r"C:\Program Files\Restreamer");
        assert_eq!(www_candidates(dir)[0], dir.join("www"));
    }

    #[test]
    fn linux_deb_layout_is_the_tauri_resource_dir() {
        let dir = Path::new("/usr/bin");
        assert!(www_candidates(dir).contains(&PathBuf::from("/usr/bin/../lib/Restreamer/www")));
    }
}

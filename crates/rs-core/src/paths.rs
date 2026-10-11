//! Where Restreamer keeps its config and its data, per platform.
//!
//! Windows (the tray app runs as the desktop user but writes a machine-wide
//! dir the installer creates): both are `C:\ProgramData\Restreamer`.
//!
//! Linux (the tray app runs in the desktop user's session, e.g. stream-pp):
//! the user's XDG dirs, because `/etc` and `/var/lib` are root-owned and a
//! desktop app cannot write them.
//! - config: `$RESTREAMER_CONFIG_DIR`, else `$XDG_CONFIG_HOME/restreamer`,
//!   else `$HOME/.config/restreamer`
//! - data (DB, chunks, logs): `$RESTREAMER_DATA_DIR`, else
//!   `$XDG_DATA_HOME/restreamer`, else `$HOME/.local/share/restreamer`
//! - none of them set: panic at startup. Guessing a CWD-relative dir would
//!   silently start on a fresh empty DB wherever the process happened to run.
//!
//! Every caller goes through these two functions; the paths used to be
//! hardcoded in six places.

use std::path::PathBuf;

const WINDOWS_DIR: &str = r"C:\ProgramData\Restreamer";
const NO_HOME: &str =
    "no RESTREAMER_*_DIR, XDG_*_HOME or HOME is set: refusing to guess where config and data live";

/// The directory holding `config.json` (and `av-gate/`, `delivery-logs/`).
pub fn config_dir() -> PathBuf {
    if cfg!(windows) {
        PathBuf::from(WINDOWS_DIR)
    } else {
        unix_dir(&env_var, Kind::Config).expect(NO_HOME)
    }
}

/// The directory holding `restreamer.db`, `chunks/` and the logs.
pub fn data_dir() -> PathBuf {
    if cfg!(windows) {
        PathBuf::from(WINDOWS_DIR)
    } else {
        unix_dir(&env_var, Kind::Data).expect(NO_HOME)
    }
}

fn env_var(name: &str) -> Option<String> {
    std::env::var(name).ok()
}

#[derive(Clone, Copy)]
enum Kind {
    Config,
    Data,
}

/// The non-Windows rule, with the environment passed in so it is testable.
/// An empty variable counts as unset (the XDG spec says so for the XDG ones).
/// `None` when not even HOME is set.
fn unix_dir(env: &dyn Fn(&str) -> Option<String>, kind: Kind) -> Option<PathBuf> {
    let set = |name: &str| env(name).filter(|v| !v.is_empty());
    let (own, xdg, home_rel) = match kind {
        Kind::Config => ("RESTREAMER_CONFIG_DIR", "XDG_CONFIG_HOME", ".config"),
        Kind::Data => ("RESTREAMER_DATA_DIR", "XDG_DATA_HOME", ".local/share"),
    };
    if let Some(dir) = set(own) {
        return Some(PathBuf::from(dir));
    }
    if let Some(base) = set(xdg) {
        return Some(PathBuf::from(base).join("restreamer"));
    }
    set("HOME").map(|home| PathBuf::from(home).join(home_rel).join("restreamer"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn env_of(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |name| map.get(name).cloned()
    }

    #[test]
    fn linux_defaults_to_the_users_xdg_dirs_under_home() {
        let env = env_of(&[("HOME", "/home/newlevel")]);
        assert_eq!(
            unix_dir(&env, Kind::Config),
            Some(PathBuf::from("/home/newlevel/.config/restreamer"))
        );
        assert_eq!(
            unix_dir(&env, Kind::Data),
            Some(PathBuf::from("/home/newlevel/.local/share/restreamer"))
        );
    }

    #[test]
    fn xdg_base_dirs_win_over_home() {
        let env = env_of(&[
            ("HOME", "/home/newlevel"),
            ("XDG_CONFIG_HOME", "/cfg"),
            ("XDG_DATA_HOME", "/data"),
        ]);
        assert_eq!(
            unix_dir(&env, Kind::Config),
            Some(PathBuf::from("/cfg/restreamer"))
        );
        assert_eq!(
            unix_dir(&env, Kind::Data),
            Some(PathBuf::from("/data/restreamer"))
        );
    }

    #[test]
    fn restreamer_overrides_win_over_xdg_and_are_used_as_is() {
        let env = env_of(&[
            ("HOME", "/home/newlevel"),
            ("XDG_CONFIG_HOME", "/cfg"),
            ("XDG_DATA_HOME", "/data"),
            ("RESTREAMER_CONFIG_DIR", "/srv/rs-config"),
            ("RESTREAMER_DATA_DIR", "/srv/rs-data"),
        ]);
        assert_eq!(
            unix_dir(&env, Kind::Config),
            Some(PathBuf::from("/srv/rs-config"))
        );
        assert_eq!(
            unix_dir(&env, Kind::Data),
            Some(PathBuf::from("/srv/rs-data"))
        );
    }

    #[test]
    fn empty_variables_count_as_unset() {
        let env = env_of(&[
            ("HOME", "/home/newlevel"),
            ("XDG_CONFIG_HOME", ""),
            ("RESTREAMER_DATA_DIR", ""),
        ]);
        assert_eq!(
            unix_dir(&env, Kind::Config),
            Some(PathBuf::from("/home/newlevel/.config/restreamer"))
        );
        assert_eq!(
            unix_dir(&env, Kind::Data),
            Some(PathBuf::from("/home/newlevel/.local/share/restreamer"))
        );
    }

    #[test]
    fn no_home_and_no_overrides_is_none_not_a_guess() {
        let env = env_of(&[]);
        assert_eq!(unix_dir(&env, Kind::Data), None);
        assert_eq!(unix_dir(&env, Kind::Config), None);
        // An override alone is enough, HOME is not needed.
        let env = env_of(&[("RESTREAMER_DATA_DIR", "/srv/rs-data")]);
        assert_eq!(
            unix_dir(&env, Kind::Data),
            Some(PathBuf::from("/srv/rs-data"))
        );
    }

    #[test]
    fn env_var_reads_the_real_process_environment() {
        // PATH is set and non-empty for every test runner; reading it back
        // through env_var proves the real environment is consulted.
        let path = std::env::var("PATH").expect("PATH set for the test runner");
        assert!(!path.is_empty());
        assert_eq!(env_var("PATH"), Some(path));
        assert_eq!(env_var("RESTREAMER_PATHS_TEST_SURELY_UNSET_VAR"), None);
    }

    #[test]
    fn windows_keeps_programdata_for_both() {
        if cfg!(windows) {
            assert_eq!(config_dir(), PathBuf::from(WINDOWS_DIR));
            assert_eq!(data_dir(), PathBuf::from(WINDOWS_DIR));
        } else {
            assert!(config_dir().ends_with("restreamer"));
            assert!(data_dir().ends_with("restreamer"));
        }
    }
}

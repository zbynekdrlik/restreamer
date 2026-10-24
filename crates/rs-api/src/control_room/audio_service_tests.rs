use super::*;

const UD: &str = "/home/newlevel/.local/share/restreamer/browser-profiles/control-rooms";

fn audio(pid: u32, start: u64, user_data: &str) -> Proc {
    Proc {
        pid,
        cmdline: format!(
            "/opt/google/chrome/chrome --type=utility --utility-sub-type=audio.mojom.AudioService \
             --lang=en-US --service-sandbox-type=none --user-data-dir={user_data}"
        ),
        start,
    }
}

fn pulse(pid: u32, start: u64) -> Proc {
    Proc {
        pid,
        cmdline: "/usr/bin/pipewire-pulse".into(),
        start,
    }
}

#[test]
fn the_start_time_is_field_22_even_with_a_tricky_command_name() {
    let stat = "71529 (chrome) S 71177 71177 71177 0 -1 4194560 3016 0 0 0 14 6 0 0 20 0 7 0 \
                1234567 1234 56 18446744073709551615";
    assert_eq!(stat_start(stat), Some(1234567));
    let tricky = "42 (a b) c) S 1 42 42 0 -1 0 0 0 0 0 0 0 0 0 20 0 1 0 987 1 1";
    assert_eq!(stat_start(tricky), Some(987));
    assert_eq!(stat_start("42 (x) S 1 2"), None);
    assert_eq!(stat_start("garbage"), None);
}

#[test]
fn an_audio_service_older_than_pipewire_pulse_is_stale() {
    // stream-snv 24.10.2026: the service started 22.10 17:33, pipewire-pulse 23.10 15:32.
    let procs = vec![audio(71529, 100, UD), pulse(773538, 500)];
    assert_eq!(stale_audio_services(&procs, Path::new(UD)), vec![71529]);
}

#[test]
fn a_service_newer_than_pipewire_pulse_is_left_alone() {
    let procs = vec![pulse(773538, 500), audio(1395936, 900, UD)];
    assert!(stale_audio_services(&procs, Path::new(UD)).is_empty());
}

#[test]
fn only_the_room_chrome_is_touched_and_only_its_audio_service() {
    let mut other = audio(9, 100, "/home/newlevel/.config/google-chrome");
    other.cmdline.push_str(" --profile-directory=Default");
    let renderer = Proc {
        pid: 10,
        cmdline: format!("/opt/google/chrome/chrome --type=renderer --user-data-dir={UD}"),
        start: 100,
    };
    // A user-data-dir that only STARTS with ours is another Chrome.
    let prefix = audio(11, 100, &format!("{UD}-copy"));
    let procs = vec![other, renderer, prefix, pulse(2, 500)];
    assert!(stale_audio_services(&procs, Path::new(UD)).is_empty());
}

#[test]
fn without_pipewire_pulse_nothing_is_stale() {
    // PulseAudio itself, or no sound server: no restart to compare with.
    let procs = vec![audio(71529, 100, UD)];
    assert!(stale_audio_services(&procs, Path::new(UD)).is_empty());
}

#[test]
fn the_newest_pipewire_pulse_decides() {
    let procs = vec![pulse(1, 50), audio(71529, 100, UD), pulse(2, 700)];
    assert_eq!(stale_audio_services(&procs, Path::new(UD)), vec![71529]);
}

#[test]
fn proc_lists_this_test_process_with_its_start_time() {
    let me = std::process::id();
    let procs = read_procs();
    let mine = procs
        .iter()
        .find(|p| p.pid == me)
        .expect("own pid in /proc");
    assert!(mine.start > 0);
    assert!(!mine.cmdline.is_empty());
    assert!(!mine.cmdline.contains('\0'));
}

#[test]
fn a_stale_audio_service_is_ended_and_a_fresh_one_kept() {
    let dir = tempfile::tempdir().unwrap();
    let ud = format!("--user-data-dir={}", dir.path().display());
    let spawn = || {
        std::process::Command::new("sh")
            .args([
                "-c",
                "sleep 30",
                "sh",
                "--utility-sub-type=audio.mojom.AudioService",
                &ud,
            ])
            .spawn()
            .unwrap()
    };
    let mut stale = spawn();
    let mut fresh = spawn();
    let mut procs: Vec<Proc> = read_procs()
        .into_iter()
        .filter(|p| p.pid == stale.id() || p.pid == fresh.id())
        .collect();
    assert_eq!(procs.len(), 2, "both test children are in /proc");
    // pipewire-pulse restarted after the first, before the second.
    let first = procs.iter().find(|p| p.pid == stale.id()).unwrap().start;
    for p in &mut procs {
        if p.pid == fresh.id() {
            p.start = first + 10;
        }
    }
    procs.push(pulse(1, first + 5));

    assert_eq!(restart_stale(&procs, dir.path()), vec![stale.id()]);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let status = loop {
        if let Some(s) = stale.try_wait().unwrap() {
            break s;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the stale service was not ended"
        );
        std::thread::sleep(std::time::Duration::from_millis(20));
    };
    assert!(!status.success(), "ended by a signal");
    assert!(
        fresh.try_wait().unwrap().is_none(),
        "the fresh service keeps running"
    );
    fresh.kill().unwrap();
    fresh.wait().unwrap();
}

//! The desktop app's own "open file" dialog (#414).
//!
//! A control room's page asks for a file ("Upload thumbnail"). On the box's
//! own screen (the kiosk window) a browser cannot open its picker by itself,
//! so the operator got only a bar with a second button to find. The desktop
//! app installs its native dialog here; the room relay opens it for a viewer
//! that is the box itself. A process without a GUI (headless, CI) has none.

use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

/// A dialog in flight: the file the operator chose, `None` when cancelled.
pub type Pick = Pin<Box<dyn Future<Output = Option<PathBuf>> + Send>>;

/// Shows the box's "open file" dialog on the box's screen.
pub trait NativeFilePicker: Send + Sync {
    fn pick_file(&self) -> Pick;
}

static INSTALLED: OnceLock<Arc<dyn NativeFilePicker>> = OnceLock::new();

/// Installs the desktop app's dialog, once. Returns false when one already was.
pub fn install(picker: Arc<dyn NativeFilePicker>) -> bool {
    INSTALLED.set(picker).is_ok()
}

/// The installed dialog, if this process has a GUI.
pub fn installed() -> Option<Arc<dyn NativeFilePicker>> {
    INSTALLED.get().cloned()
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Fixed(&'static str);

    impl NativeFilePicker for Fixed {
        fn pick_file(&self) -> Pick {
            let path = PathBuf::from(self.0);
            Box::pin(async move { Some(path) })
        }
    }

    #[tokio::test]
    async fn the_first_installed_dialog_stays() {
        assert!(install(Arc::new(Fixed("/a.png"))));
        assert!(!install(Arc::new(Fixed("/b.png"))), "installed once");
        let picker = installed().expect("installed");
        assert_eq!(picker.pick_file().await, Some(PathBuf::from("/a.png")));
    }
}

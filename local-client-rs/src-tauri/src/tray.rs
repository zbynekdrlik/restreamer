use tauri::image::Image;
use tauri::menu::{Menu, MenuItem};
use tauri::tray::TrayIconBuilder;
use tauri::{App, Manager};

/// Generate a 32x32 RGBA icon with the given color.
fn make_icon(r: u8, g: u8, b: u8, label: char) -> Vec<u8> {
    let size = 32usize;
    let mut pixels = vec![0u8; size * size * 4];
    for y in 0..size {
        for x in 0..size {
            let idx = (y * size + x) * 4;
            // Simple circle with letter placeholder
            let cx = (x as f64 - 16.0).abs();
            let cy = (y as f64 - 16.0).abs();
            let dist = (cx * cx + cy * cy).sqrt();
            if dist < 14.0 {
                pixels[idx] = r;
                pixels[idx + 1] = g;
                pixels[idx + 2] = b;
                pixels[idx + 3] = 255;
            }
        }
    }
    pixels
}

pub fn setup_tray(app: &App) -> Result<(), Box<dyn std::error::Error>> {
    let version = env!("CARGO_PKG_VERSION");

    // Inpoint tray icon
    let inpoint_menu = Menu::with_items(
        app,
        &[
            &MenuItem::new(app, format!("Restreamer v{version}"), false, None::<&str>)?,
            &MenuItem::new(app, "Status: Waiting", false, None::<&str>)?,
            &MenuItem::with_id(app, "open_dashboard", "Open Dashboard", true, None::<&str>)?,
            &MenuItem::with_id(
                app,
                "restart_inpoint",
                "Restart Inpoint",
                true,
                None::<&str>,
            )?,
            &MenuItem::with_id(
                app,
                "delete_chunks",
                "Delete All Chunks",
                true,
                None::<&str>,
            )?,
            &MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?,
        ],
    )?;

    let inpoint_icon_data = make_icon(255, 165, 0, 'I'); // Orange = waiting
    let inpoint_icon = Image::new_owned(inpoint_icon_data, 32, 32);

    let _inpoint_tray = TrayIconBuilder::with_id("inpoint")
        .icon(inpoint_icon)
        .menu(&inpoint_menu)
        .tooltip("Restreamer Inpoint")
        .on_menu_event(move |app, event| match event.id().as_ref() {
            "open_dashboard" => {
                if let Some(window) = app.get_webview_window("main") {
                    let _ = window.show();
                    let _ = window.set_focus();
                }
            }
            "quit" => {
                app.exit(0);
            }
            _ => {}
        })
        .build(app)?;

    // Endpoint tray icon
    let endpoint_menu = Menu::with_items(
        app,
        &[
            &MenuItem::new(app, "Status: Waiting", false, None::<&str>)?,
            &MenuItem::new(app, "Pending: 0 chunks", false, None::<&str>)?,
            &MenuItem::with_id(
                app,
                "restart_endpoint",
                "Restart Endpoint",
                true,
                None::<&str>,
            )?,
        ],
    )?;

    let endpoint_icon_data = make_icon(255, 165, 0, 'E'); // Orange = waiting
    let endpoint_icon = Image::new_owned(endpoint_icon_data, 32, 32);

    let _endpoint_tray = TrayIconBuilder::with_id("endpoint")
        .icon(endpoint_icon)
        .menu(&endpoint_menu)
        .tooltip("Restreamer Endpoint")
        .build(app)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn make_icon_produces_correct_size() {
        let icon = make_icon(255, 0, 0, 'X');
        assert_eq!(icon.len(), 32 * 32 * 4);
    }

    #[test]
    fn make_icon_has_nonzero_alpha() {
        let icon = make_icon(0, 255, 0, 'T');
        // At least some pixels should have alpha > 0 (the circle)
        let has_visible = icon.chunks(4).any(|pixel| pixel[3] > 0);
        assert!(has_visible);
    }
}

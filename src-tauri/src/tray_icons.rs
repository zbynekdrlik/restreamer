//! Programmatic tray icon generation with state-based colors and animation.
//!
//! Generates 32x32 RGBA icons at runtime — no external asset files needed.
//! Each icon is a rounded rectangle with a white "R" lettermark and an
//! optional animated status dot in the bottom-right corner.

/// Icon dimensions.
const SIZE: u32 = 32;
/// Bytes per pixel (RGBA).
const BPP: usize = 4;
/// Total buffer length: 32 * 32 * 4 = 4096.
pub const ICON_BYTES: usize = (SIZE as usize) * (SIZE as usize) * BPP;

/// Corner radius for the rounded rectangle background.
const CORNER_RADIUS: i32 = 6;

// --- State colors (Fluent Design palette) ---

const COLOR_GRAY: [u8; 4] = [0x6B, 0x72, 0x80, 0xFF]; // Idle
const COLOR_BLUE: [u8; 4] = [0x3B, 0x82, 0xF6, 0xFF]; // Ready
const COLOR_GREEN: [u8; 4] = [0x10, 0xB9, 0x81, 0xFF]; // Receiving
const COLOR_RED: [u8; 4] = [0xEF, 0x44, 0x44, 0xFF]; // Error
const COLOR_WHITE: [u8; 4] = [0xFF, 0xFF, 0xFF, 0xFF];
const COLOR_AMBER: [u8; 4] = [0xF5, 0x9E, 0x0B, 0xFF]; // Upload indicator

/// Application states that map to distinct tray icon visuals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TrayState {
    /// Gray — no active event.
    Idle,
    /// Blue — event active, waiting for RTMP connection.
    Ready,
    /// Green — RTMP connected, chunks being created. Pulsing dot animation.
    Receiving,
    /// Green + amber dot — chunks pending upload.
    Uploading,
    /// Red — database or service errors. Blinking dot animation.
    Error,
}

impl TrayState {
    /// Whether this state has animation frames.
    pub fn is_animated(self) -> bool {
        matches!(self, TrayState::Receiving | TrayState::Error)
    }

    /// Number of distinct animation frames for this state.
    pub fn frame_count(self) -> u8 {
        match self {
            TrayState::Receiving => 3, // pulse: bright → medium → dim
            TrayState::Error => 2,     // blink: on → off
            _ => 1,                    // static
        }
    }
}

/// Generate a 32x32 RGBA icon for the given state and animation frame.
///
/// Returns `Vec<u8>` of length [`ICON_BYTES`] (4096).
pub fn generate_icon(state: TrayState, animation_frame: u8) -> Vec<u8> {
    let mut buf = vec![0u8; ICON_BYTES];

    let bg_color = match state {
        TrayState::Idle => COLOR_GRAY,
        TrayState::Ready => COLOR_BLUE,
        TrayState::Receiving | TrayState::Uploading => COLOR_GREEN,
        TrayState::Error => COLOR_RED,
    };

    fill_rounded_rect(&mut buf, bg_color, CORNER_RADIUS);
    draw_letter_r(&mut buf, COLOR_WHITE);

    // Status dot for animated / indicator states
    match state {
        TrayState::Receiving => {
            // Pulsing white dot: alpha cycles through 3 levels
            let alpha = match animation_frame % 3 {
                0 => 0xFF,
                1 => 0xAA,
                _ => 0x55,
            };
            let dot_color = [0xFF, 0xFF, 0xFF, alpha];
            draw_status_dot(&mut buf, dot_color);
        }
        TrayState::Error => {
            // Blinking white dot: on/off
            if animation_frame % 2 == 0 {
                draw_status_dot(&mut buf, COLOR_WHITE);
            }
            // frame 1 = dot hidden (background shows through)
        }
        TrayState::Uploading => {
            // Static amber indicator dot
            draw_status_dot(&mut buf, COLOR_AMBER);
        }
        _ => {} // no dot for Idle / Ready
    }

    buf
}

// ---- Drawing helpers ----

/// Fill the canvas with a rounded rectangle of the given color.
fn fill_rounded_rect(buf: &mut [u8], color: [u8; 4], radius: i32) {
    let r = radius;
    for y in 0..SIZE as i32 {
        for x in 0..SIZE as i32 {
            // Check if point is inside the rounded rect
            let inside = is_inside_rounded_rect(x, y, SIZE as i32, SIZE as i32, r);
            if inside {
                set_pixel(buf, x as u32, y as u32, color);
            }
        }
    }
}

/// Test whether (x, y) lies inside a rounded rectangle of the given size.
fn is_inside_rounded_rect(x: i32, y: i32, w: i32, h: i32, r: i32) -> bool {
    // Interior bands (always inside)
    if x >= r && x < w - r {
        return true; // horizontal band
    }
    if y >= r && y < h - r {
        return true; // vertical band
    }

    // Corner circles
    let corners = [
        (r, r),                 // top-left
        (w - r - 1, r),         // top-right
        (r, h - r - 1),         // bottom-left
        (w - r - 1, h - r - 1), // bottom-right
    ];

    for (cx, cy) in corners {
        let dx = x - cx;
        let dy = y - cy;
        if dx * dx + dy * dy <= r * r {
            return true;
        }
    }

    false
}

/// Draw a geometric "R" letterform on the icon.
///
/// The glyph occupies roughly columns 8..24, rows 6..26 of the 32x32 canvas.
/// It's a blocky, modern letterform with 2-3px strokes.
fn draw_letter_r(buf: &mut [u8], color: [u8; 4]) {
    // Vertical stroke (left stem): x=9..12, y=7..25
    fill_rect(buf, 9, 7, 12, 25, color);

    // Top horizontal bar: x=9..22, y=7..10
    fill_rect(buf, 9, 7, 22, 10, color);

    // Middle horizontal bar: x=9..22, y=15..18
    fill_rect(buf, 9, 15, 22, 18, color);

    // Upper-right vertical (bowl): x=19..22, y=7..18
    fill_rect(buf, 19, 7, 22, 18, color);

    // Diagonal leg: from (12,18) to (22,25), 3px wide
    for row in 18..26 {
        let progress = row - 18; // 0..7
        let x_start = 12 + progress * 10 / 8; // 12 → 22
        let x_end = x_start + 3;
        fill_rect(
            buf,
            x_start as u32,
            row as u32,
            x_end.min(24) as u32,
            (row + 1) as u32,
            color,
        );
    }
}

/// Draw a filled circle (status indicator dot) at the bottom-right of the icon.
///
/// Center at (25, 25), radius 3 pixels.
fn draw_status_dot(buf: &mut [u8], color: [u8; 4]) {
    let cx: i32 = 25;
    let cy: i32 = 25;
    let r: i32 = 3;

    for y in (cy - r)..=(cy + r) {
        for x in (cx - r)..=(cx + r) {
            let dx = x - cx;
            let dy = y - cy;
            if dx * dx + dy * dy <= r * r {
                set_pixel(buf, x as u32, y as u32, color);
            }
        }
    }
}

/// Fill a rectangular region with the given color.
fn fill_rect(buf: &mut [u8], x1: u32, y1: u32, x2: u32, y2: u32, color: [u8; 4]) {
    for y in y1..y2 {
        for x in x1..x2 {
            set_pixel(buf, x, y, color);
        }
    }
}

/// Set a single pixel in the RGBA buffer. Out-of-bounds writes are silently ignored.
fn set_pixel(buf: &mut [u8], x: u32, y: u32, color: [u8; 4]) {
    if x >= SIZE || y >= SIZE {
        return;
    }
    let offset = ((y * SIZE + x) as usize) * BPP;
    // Alpha compositing: blend new color over existing pixel
    let src_a = color[3] as u16;
    if src_a == 0 {
        return;
    }
    if src_a == 255 || buf[offset + 3] == 0 {
        // Fully opaque source or transparent destination — direct write
        buf[offset..offset + 4].copy_from_slice(&color);
    } else {
        // Alpha blend
        let dst_a = buf[offset + 3] as u16;
        let out_a = src_a + dst_a * (255 - src_a) / 255;
        if out_a == 0 {
            return;
        }
        for i in 0..3 {
            let src = color[i] as u16;
            let dst = buf[offset + i] as u16;
            buf[offset + i] = ((src * src_a + dst * dst_a * (255 - src_a) / 255) / out_a) as u8;
        }
        buf[offset + 3] = out_a as u8;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generate_icon_correct_size() {
        for state in [
            TrayState::Idle,
            TrayState::Ready,
            TrayState::Receiving,
            TrayState::Uploading,
            TrayState::Error,
        ] {
            let icon = generate_icon(state, 0);
            assert_eq!(icon.len(), ICON_BYTES, "icon size for {state:?}");
        }
    }

    #[test]
    fn generate_icon_not_transparent() {
        for state in [
            TrayState::Idle,
            TrayState::Ready,
            TrayState::Receiving,
            TrayState::Uploading,
            TrayState::Error,
        ] {
            let icon = generate_icon(state, 0);
            let has_visible = icon.chunks(4).any(|px| px[3] > 0);
            assert!(has_visible, "icon for {state:?} has no visible pixels");
        }
    }

    #[test]
    fn different_states_produce_different_icons() {
        let idle = generate_icon(TrayState::Idle, 0);
        let ready = generate_icon(TrayState::Ready, 0);
        let receiving = generate_icon(TrayState::Receiving, 0);
        let error = generate_icon(TrayState::Error, 0);

        assert_ne!(idle, ready, "idle != ready");
        assert_ne!(idle, receiving, "idle != receiving");
        assert_ne!(idle, error, "idle != error");
        assert_ne!(ready, receiving, "ready != receiving");
        assert_ne!(ready, error, "ready != error");
    }

    #[test]
    fn animation_frames_differ_for_receiving() {
        let f0 = generate_icon(TrayState::Receiving, 0);
        let f1 = generate_icon(TrayState::Receiving, 1);
        let f2 = generate_icon(TrayState::Receiving, 2);

        assert_ne!(f0, f1, "receiving frame 0 != frame 1");
        assert_ne!(f1, f2, "receiving frame 1 != frame 2");
        assert_ne!(f0, f2, "receiving frame 0 != frame 2");
    }

    #[test]
    fn animation_frames_differ_for_error() {
        let f0 = generate_icon(TrayState::Error, 0);
        let f1 = generate_icon(TrayState::Error, 1);

        assert_ne!(f0, f1, "error frame 0 != frame 1 (blink)");
    }

    #[test]
    fn static_states_ignore_animation_frame() {
        let idle0 = generate_icon(TrayState::Idle, 0);
        let idle5 = generate_icon(TrayState::Idle, 5);
        assert_eq!(idle0, idle5, "idle should be same at any frame");

        let ready0 = generate_icon(TrayState::Ready, 0);
        let ready3 = generate_icon(TrayState::Ready, 3);
        assert_eq!(ready0, ready3, "ready should be same at any frame");

        let upload0 = generate_icon(TrayState::Uploading, 0);
        let upload2 = generate_icon(TrayState::Uploading, 2);
        assert_eq!(upload0, upload2, "uploading should be same at any frame");
    }

    #[test]
    fn set_pixel_bounds_check() {
        let mut buf = vec![0u8; ICON_BYTES];
        // These should not panic
        set_pixel(&mut buf, 32, 0, COLOR_WHITE);
        set_pixel(&mut buf, 0, 32, COLOR_WHITE);
        set_pixel(&mut buf, 100, 100, COLOR_WHITE);
        // Buffer should remain all zeros
        assert!(buf.iter().all(|&b| b == 0));
    }

    #[test]
    fn is_animated_correct() {
        assert!(!TrayState::Idle.is_animated());
        assert!(!TrayState::Ready.is_animated());
        assert!(TrayState::Receiving.is_animated());
        assert!(!TrayState::Uploading.is_animated());
        assert!(TrayState::Error.is_animated());
    }

    #[test]
    fn frame_count_correct() {
        assert_eq!(TrayState::Idle.frame_count(), 1);
        assert_eq!(TrayState::Ready.frame_count(), 1);
        assert_eq!(TrayState::Receiving.frame_count(), 3);
        assert_eq!(TrayState::Uploading.frame_count(), 1);
        assert_eq!(TrayState::Error.frame_count(), 2);
    }

    #[test]
    fn receiving_frames_cycle_at_modulo_3() {
        // Frame 3 should equal frame 0 (modulo wrapping)
        let f0 = generate_icon(TrayState::Receiving, 0);
        let f3 = generate_icon(TrayState::Receiving, 3);
        assert_eq!(f0, f3, "frame 3 should wrap to same as frame 0");
    }

    #[test]
    fn error_frames_cycle_at_modulo_2() {
        let f0 = generate_icon(TrayState::Error, 0);
        let f2 = generate_icon(TrayState::Error, 2);
        assert_eq!(f0, f2, "frame 2 should wrap to same as frame 0");
    }

    #[test]
    fn uploading_has_amber_dot() {
        let uploading = generate_icon(TrayState::Uploading, 0);
        let receiving = generate_icon(TrayState::Receiving, 0);
        // They use the same green background but differ in the status dot
        assert_ne!(
            uploading, receiving,
            "uploading should differ from receiving"
        );
    }

    #[test]
    fn icon_has_nonzero_alpha_in_center() {
        // The center pixel (16, 16) should have full alpha (inside the rounded rect)
        let icon = generate_icon(TrayState::Idle, 0);
        let offset = (16 * 32 + 16) * 4;
        assert_eq!(
            icon[offset + 3],
            0xFF,
            "center pixel should be fully opaque"
        );
    }
}

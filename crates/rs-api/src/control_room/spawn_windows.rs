//! Windows: start the control-room Chrome so that it inherits NO handle from
//! Restreamer (#388).
//!
//! `std`/tokio `Command` always calls `CreateProcessW` with
//! `bInheritHandles = TRUE`. Restreamer's listening sockets are inheritable,
//! so Chrome, which outlives Restreamer on purpose, kept 8910 and 1234 open. The
//! next Restreamer then failed to bind them with os error 10048 (stream-snv,
//! 2026-10-14). Chrome needs no stdio from us, so it gets
//! `bInheritHandles = FALSE`.
//!
//! Only raw FFI glue lives here: the command line comes from the
//! cross-platform, unit-tested `browser::windows_command_line`. cargo-mutants
//! does not evaluate `cfg`, so `.cargo/mutants.toml` excludes this file.

use std::io;
use std::os::windows::ffi::OsStrExt;
use std::path::Path;
use std::time::Duration;

use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, WAIT_OBJECT_0, WAIT_TIMEOUT};
use windows_sys::Win32::System::Threading::{
    CreateProcessW, GetExitCodeProcess, PROCESS_INFORMATION, STARTUPINFOW, WaitForSingleObject,
};

use super::browser::windows_command_line;

/// A started process; the handle closes on drop (the process keeps running).
pub struct Child(HANDLE);

// The process handle is a kernel object handle, valid from any thread.
unsafe impl Send for Child {}

impl Drop for Child {
    fn drop(&mut self) {
        unsafe { CloseHandle(self.0) };
    }
}

impl Child {
    /// Wait up to `timeout` for the process to exit: `Some(exit code)`, or
    /// `None` when it is still running.
    pub fn wait(&self, timeout: Duration) -> io::Result<Option<u32>> {
        let ms = u32::try_from(timeout.as_millis()).unwrap_or(u32::MAX);
        match unsafe { WaitForSingleObject(self.0, ms) } {
            WAIT_OBJECT_0 => {
                let mut code = 0u32;
                if unsafe { GetExitCodeProcess(self.0, &mut code) } == 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(Some(code))
            }
            WAIT_TIMEOUT => Ok(None),
            _ => Err(io::Error::last_os_error()),
        }
    }
}

/// Start `program` with `args`, inheriting no handle.
pub fn spawn(program: &Path, args: &[String]) -> io::Result<Child> {
    let app: Vec<u16> = program
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect();
    // CreateProcessW may write into the command-line buffer.
    let mut line: Vec<u16> = windows_command_line(&program.to_string_lossy(), args)
        .encode_utf16()
        .chain(std::iter::once(0))
        .collect();
    let mut si: STARTUPINFOW = unsafe { std::mem::zeroed() };
    si.cb = std::mem::size_of::<STARTUPINFOW>() as u32;
    let mut pi: PROCESS_INFORMATION = unsafe { std::mem::zeroed() };
    let ok = unsafe {
        CreateProcessW(
            app.as_ptr(),
            line.as_mut_ptr(),
            std::ptr::null(),
            std::ptr::null(),
            0, // bInheritHandles = FALSE: the whole point of this module
            0,
            std::ptr::null(),
            std::ptr::null(),
            &si,
            &mut pi,
        )
    };
    if ok == 0 {
        return Err(io::Error::last_os_error());
    }
    unsafe { CloseHandle(pi.hThread) };
    Ok(Child(pi.hProcess))
}

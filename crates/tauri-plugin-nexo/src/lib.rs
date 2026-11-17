use tauri::{Runtime, plugin::Builder, plugin::TauriPlugin};

#[cfg(any(windows, test))]
mod client;
#[cfg(test)]
mod tests;
#[cfg(windows)]
mod windows;

#[cfg(windows)]
pub use client::{Handle, QuitDecision, QuitResponder};
#[cfg(windows)]
pub use nexo_protocol::{AppInfo, MenuItem, QuitReason};
#[cfg(windows)]
pub use windows::{Options, configured, current_user_sid, handle};

/// Preserve the original, unconfigured no-op plugin.
pub fn init<R: Runtime>() -> TauriPlugin<R> {
    Builder::new("nexo").build()
}

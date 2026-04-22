//! Test-only submodule roll-up for `endpoint_task`. Keeps a single
//! `#[cfg(test)]` declaration at the end of `endpoint_task.rs` (file-size
//! gate) while still splitting tests across focused files.

#[path = "endpoint_task_backoff_tests.rs"]
mod backoff_tests;
#[path = "endpoint_task_flv_tests.rs"]
mod flv_tests;
#[path = "endpoint_task_rescue_tests.rs"]
mod rescue_tests;
#[path = "endpoint_task_tests.rs"]
mod tests;

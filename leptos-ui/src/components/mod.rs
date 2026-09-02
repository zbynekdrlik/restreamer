//! UI components for the Restreamer dashboard.

pub mod add_endpoint_modal;
pub mod oauth_authorize;
pub mod audit_panel;
pub mod change_key_modal;
mod confirm_modal;
pub mod disk_pressure_banner;
pub mod endpoint_history;
pub mod endpoint_remove_confirm_modal;
mod endpoint_tree;
mod endpoints;
mod header;
pub mod ingest_skew_banner;
mod operator_dashboard;
pub mod outage_banner;
pub mod pacing_panel;
pub mod s3_region_banner;
mod settings;
mod templates;
pub mod upload_strip;
mod uploads;
pub mod zero_endpoint_banner;

pub use confirm_modal::ConfirmModal;
pub use endpoints::EndpointsView;
pub use header::Header;
pub use operator_dashboard::OperatorDashboard;
pub use settings::SettingsView;
pub use uploads::UploadsView;

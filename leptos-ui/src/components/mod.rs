//! UI components for the Restreamer dashboard.

pub mod add_endpoint_modal;
pub mod audit_panel;
mod confirm_modal;
pub mod endpoint_history;
pub mod endpoint_remove_confirm_modal;
mod endpoints;
mod header;
mod operator_dashboard;
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
pub use templates::TemplatesView;
pub use uploads::UploadsView;

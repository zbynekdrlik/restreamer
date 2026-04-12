//! UI components for the Restreamer dashboard.

mod confirm_modal;
mod endpoints;
mod header;
mod operator_dashboard;
mod settings;
mod templates;

pub use confirm_modal::ConfirmModal;
pub use endpoints::EndpointsView;
pub use header::Header;
pub use operator_dashboard::OperatorDashboard;
pub use settings::SettingsView;
pub use templates::TemplatesView;

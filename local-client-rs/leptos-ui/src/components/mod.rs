//! UI components for the Restreamer dashboard.

mod dashboard;
mod endpoints;
mod events;
mod header;
mod log_viewer;

pub use dashboard::DashboardView;
pub use endpoints::EndpointsView;
pub use events::EventsView;
pub use header::Header;
pub use log_viewer::LogsView;

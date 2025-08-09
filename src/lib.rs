mod channel_manager;
mod composed_extension_codec;
pub(crate) mod context;
mod errors;
mod flight_service;
mod plan;

#[cfg(any(feature = "integration", test))]
pub mod test_utils;

pub use channel_manager::{BoxCloneSyncChannel, ChannelManager, ChannelResolver};
pub use flight_service::{ArrowFlightEndpoint, SessionBuilder, NoopSessionBuilder};
pub use plan::{assign_stages, ArrowFlightReadExec};

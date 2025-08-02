use async_trait::async_trait;
use datafusion::common::internal_datafusion_err;
use datafusion::error::DataFusionError;
use datafusion::prelude::SessionConfig;
use delegate::delegate;
use std::sync::Arc;
use tonic::body::BoxBody;
use url::Url;

pub struct ChannelManager(Arc<dyn ChannelResolver + Send + Sync>);

impl ChannelManager {
    pub fn new(resolver: impl ChannelResolver + Send + Sync + 'static) -> Self {
        Self(Arc::new(resolver))
    }
}

pub type BoxCloneSyncChannel = tower::util::BoxCloneSyncService<
    http::Request<BoxBody>,
    http::Response<BoxBody>,
    tonic::transport::Error,
>;

/// Abstracts networking details so that users can implement their own network resolution
/// mechanism.
#[async_trait]
pub trait ChannelResolver {
    /// Gets all available worker URLs. Used during stage assignment.
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError>;
    /// For a given URL, get a channel for communicating to it.
    async fn get_channel_for_url(&self, url: &Url) -> Result<BoxCloneSyncChannel, DataFusionError>;
}

impl ChannelManager {
    pub fn try_from_session(session: &SessionConfig) -> Result<Arc<Self>, DataFusionError> {
        session
            .get_extension::<ChannelManager>()
            .ok_or_else(|| internal_datafusion_err!("No extension ChannelManager"))
    }

    delegate! {
        to self.0 {
            pub fn get_urls(&self) -> Result<Vec<Url>, DataFusionError>;
            pub async fn get_channel_for_url(&self, url: &Url) -> Result<BoxCloneSyncChannel, DataFusionError>;
        }
    }
}

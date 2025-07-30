use datafusion::execution::SessionStateBuilder;

/// Trait called by the Arrow Flight endpoint that handles distributed parts of a DataFusion
/// plan for building a DataFusion's [datafusion::prelude::SessionContext].
pub trait SessionBuilder {
    /// Takes a [SessionStateBuilder] and adds whatever is necessary for it to work, like
    /// custom extension codecs, custom physical optimization rules, UDFs, UDAFs, config
    /// extensions, etc...
    ///
    /// Example: adding some custom extension plan codecs
    ///
    /// ```rust
    ///
    /// # use std::sync::Arc;
    /// # use datafusion::execution::runtime_env::RuntimeEnv;
    /// # use datafusion::execution::SessionStateBuilder;
    /// # use datafusion_distributed_experiment::{PhysicalExtensionCodecExt, SessionBuilder};
    ///
    /// #[derive(Debug)]
    /// struct CustomExecCodec {
    ///   runtime: Arc<RuntimeEnv>,
    /// }
    ///
    /// #[derive(Clone)]
    /// struct CustomSessionBuilder;
    /// impl SessionBuilder for CustomSessionBuilder {
    ///     fn on_new_session(&self, mut builder: SessionStateBuilder) -> SessionStateBuilder {
    ///         let runtime = builder.runtime_env().get_or_insert_default();
    ///         let config = builder.config().get_or_insert_default();
    ///
    ///         let codec: Arc<dyn PhysicalExtensionCodecExt> = Arc::new(CustomExecCodec {
    ///             runtime: runtime.clone()
    ///         });
    ///         config.set_extension(Arc::new(codec));
    ///         builder
    ///     }
    /// }
    /// ```
    fn on_new_session(&self, builder: SessionStateBuilder) -> SessionStateBuilder;
}

/// Noop implementation of the [SessionBuilder]. Used by default if no [SessionBuilder] is provided
/// while building the Arrow Flight endpoint.
pub struct NoopSessionBuilder;

impl SessionBuilder for NoopSessionBuilder {
    fn on_new_session(&self, builder: SessionStateBuilder) -> SessionStateBuilder {
        builder
    }
}

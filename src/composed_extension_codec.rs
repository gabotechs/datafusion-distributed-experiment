use datafusion::error::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use std::sync::Arc;

// Taken from:
// https://github.com/apache/datafusion/blob/0eebc0c7c0ffcd1514f5c6d0f8e2b6d0c69a07f5/datafusion-examples/examples/composed_extension_codec.rs#L236-L291

/// A PhysicalExtensionCodec that tries one of multiple inner codecs
/// until one works
#[derive(Debug, Clone, Default)]
pub(crate) struct ComposedPhysicalExtensionCodec {
    codecs: Vec<Arc<dyn PhysicalExtensionCodec>>,
}

impl ComposedPhysicalExtensionCodec {
    pub(crate) fn push(&mut self, codec: impl PhysicalExtensionCodec + 'static) {
        self.codecs.push(Arc::new(codec));
    }

    pub(crate) fn push_from_config(&mut self, config: &SessionConfig) {
        if let Some(user_codec) = config.get_extension::<Arc<dyn PhysicalExtensionCodec>>() {
            self.codecs.push(user_codec.as_ref().clone());
        }
    }

    fn try_any<T>(
        &self,
        mut f: impl FnMut(&dyn PhysicalExtensionCodec) -> Result<T, DataFusionError>,
    ) -> Result<T, DataFusionError> {
        let mut last_err = None;
        for codec in &self.codecs {
            match f(codec.as_ref()) {
                Ok(node) => return Ok(node),
                Err(err) => last_err = Some(err),
            }
        }

        Err(last_err.unwrap_or_else(|| {
            DataFusionError::NotImplemented("Empty list of composed codecs".to_owned())
        }))
    }
}

impl PhysicalExtensionCodec for ComposedPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.try_any(|codec| codec.try_decode(buf, inputs, registry))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        self.try_any(|codec| codec.try_encode(node.clone(), buf))
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>, DataFusionError> {
        self.try_any(|codec| codec.try_decode_udf(name, buf))
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<(), DataFusionError> {
        self.try_any(|codec| codec.try_encode_udf(node, buf))
    }

    fn try_decode_udaf(
        &self,
        name: &str,
        buf: &[u8],
    ) -> Result<Arc<AggregateUDF>, DataFusionError> {
        self.try_any(|codec| codec.try_decode_udaf(name, buf))
    }

    fn try_encode_udaf(
        &self,
        node: &AggregateUDF,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        self.try_any(|codec| codec.try_encode_udaf(node, buf))
    }
}

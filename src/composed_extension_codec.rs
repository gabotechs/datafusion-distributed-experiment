use datafusion::common::not_impl_err;
use datafusion::error::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::logical_expr::{AggregateUDF, ScalarUDF, WindowUDF};
use datafusion::physical_expr::PhysicalExpr;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionConfig;
use datafusion_proto::physical_plan::PhysicalExtensionCodec;
use std::fmt::Debug;
use std::sync::Arc;


// Idea taken from
// https://github.com/apache/datafusion/blob/0eebc0c7c0ffcd1514f5c6d0f8e2b6d0c69a07f5/datafusion-examples/examples/composed_extension_codec.rs#L236-L291

pub trait PhysicalExtensionCodecExt: Debug + Send + Sync {
    fn try_decode(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn ExecutionPlan>],
        _registry: &dyn FunctionRegistry,
        _codec: &dyn PhysicalExtensionCodec
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        not_impl_err!("try_decode not implemented for {self:?}")
    }

    fn try_encode(
        &self,
        _node: Arc<dyn ExecutionPlan>,
        _buf: &mut Vec<u8>,
        _codec: &dyn PhysicalExtensionCodec
    ) -> datafusion::common::Result<()> {
        not_impl_err!("try_encode not implemented for {self:?}")
    }

    fn try_decode_udf(
        &self,
        _name: &str,
        _buf: &[u8],
        _codec: &dyn PhysicalExtensionCodec
    ) -> datafusion::common::Result<Arc<ScalarUDF>> {
        not_impl_err!("try_decode_udf not implemented for {self:?}")
    }

    fn try_encode_udf(
        &self,
        _node: &ScalarUDF,
        _buf: &mut Vec<u8>,
        _codec: &dyn PhysicalExtensionCodec
    ) -> datafusion::common::Result<()> {
        Ok(())
    }

    fn try_decode_expr(
        &self,
        _buf: &[u8],
        _inputs: &[Arc<dyn PhysicalExpr>],
        _codec: &dyn PhysicalExtensionCodec
    ) -> datafusion::common::Result<Arc<dyn PhysicalExpr>> {
        not_impl_err!("try_decode_expr not implemented for {self:?}")
    }

    fn try_encode_expr(
        &self,
        _node: &Arc<dyn PhysicalExpr>,
        _buf: &mut Vec<u8>,
        _codec: &dyn PhysicalExtensionCodec
    ) -> datafusion::common::Result<()> {
        not_impl_err!("try_encode_expr not implemented for {self:?}")
    }

    fn try_decode_udaf(
        &self,
        _name: &str,
        _buf: &[u8],
        _codec: &dyn PhysicalExtensionCodec
    ) -> datafusion::common::Result<Arc<AggregateUDF>> {
        not_impl_err!("try_decode_udaf not implemented for {self:?}")
    }

    fn try_encode_udaf(
        &self,
        _node: &AggregateUDF,
        _buf: &mut Vec<u8>,
        _codec: &dyn PhysicalExtensionCodec
    ) -> datafusion::common::Result<()> {
        Ok(())
    }

    fn try_decode_udwf(
        &self,
        _name: &str,
        _buf: &[u8],
        _codec: &dyn PhysicalExtensionCodec
    ) -> datafusion::common::Result<Arc<WindowUDF>> {
        not_impl_err!("try_decode_udwf not implemented for {self:?}")
    }

    fn try_encode_udwf(
        &self,
        _node: &WindowUDF,
        _buf: &mut Vec<u8>,
        _codec: &dyn PhysicalExtensionCodec
    ) -> datafusion::common::Result<()> {
        Ok(())
    }
}

/// A PhysicalExtensionCodec that tries one of multiple inner codecs
/// until one works
#[derive(Debug, Clone, Default)]
pub(crate) struct ComposedPhysicalExtensionCodec {
    codecs: Vec<Arc<dyn PhysicalExtensionCodecExt>>,
}

impl ComposedPhysicalExtensionCodec {
    pub(crate) fn push(&mut self, codec: impl PhysicalExtensionCodecExt + 'static) {
        self.codecs.push(Arc::new(codec));
    }

    pub(crate) fn push_from_config(&mut self, config: &SessionConfig) {
        if let Some(user_codec) = config.get_extension::<Arc<dyn PhysicalExtensionCodecExt>>() {
            self.codecs.push(user_codec.as_ref().clone());
        }
    }

    fn try_any<T>(
        &self,
        mut f: impl FnMut(&dyn PhysicalExtensionCodecExt) -> Result<T, DataFusionError>,
    ) -> Result<T, DataFusionError> {
        let mut errs = vec![];
        for codec in &self.codecs {
            match f(codec.as_ref()) {
                Ok(node) => return Ok(node),
                Err(err) => errs.push(err),
            }
        }

        if errs.is_empty() {
            return not_impl_err!("Empty list of composed codecs");
        }

        let mut msg = "None of the provided PhysicalExtensionCodec worked:".to_string();
        for err in &errs {
            msg += &format!("\n    {err}");
        }
        not_impl_err!("{msg}")
    }
}

impl PhysicalExtensionCodec for ComposedPhysicalExtensionCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        self.try_any(|codec| codec.try_decode(buf, inputs, registry, self))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        self.try_any(|codec| codec.try_encode(node.clone(), buf, self))
    }

    fn try_decode_udf(&self, name: &str, buf: &[u8]) -> Result<Arc<ScalarUDF>, DataFusionError> {
        self.try_any(|codec| codec.try_decode_udf(name, buf, self))
    }

    fn try_encode_udf(&self, node: &ScalarUDF, buf: &mut Vec<u8>) -> Result<(), DataFusionError> {
        self.try_any(|codec| codec.try_encode_udf(node, buf, self))
    }

    fn try_decode_udaf(
        &self,
        name: &str,
        buf: &[u8],
    ) -> Result<Arc<AggregateUDF>, DataFusionError> {
        self.try_any(|codec| codec.try_decode_udaf(name, buf, self))
    }

    fn try_encode_udaf(
        &self,
        node: &AggregateUDF,
        buf: &mut Vec<u8>,
    ) -> Result<(), DataFusionError> {
        self.try_any(|codec| codec.try_encode_udaf(node, buf, self))
    }
}

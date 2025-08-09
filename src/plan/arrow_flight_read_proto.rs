use crate::context::{InputStage, StageContext};
use crate::plan::arrow_flight_read::ArrowFlightReadExec;
use datafusion::error::DataFusionError;
use datafusion::execution::FunctionRegistry;
use datafusion::physical_plan::ExecutionPlan;
use datafusion_proto::physical_plan::from_proto::parse_protobuf_partitioning;
use datafusion_proto::physical_plan::to_proto::serialize_partitioning;
use datafusion_proto::physical_plan::{DefaultPhysicalExtensionCodec, PhysicalExtensionCodec};
use datafusion_proto::protobuf;
use datafusion_proto::protobuf::proto_error;
use prost::Message;
use std::sync::Arc;
use url::Url;
use uuid::Uuid;

/// DataFusion [PhysicalExtensionCodec] implementation that allows sending and receiving
/// [ArrowFlightReadExecProto] over the wire.
#[derive(Debug)]
pub struct ArrowFlightReadExecProtoCodec;

impl PhysicalExtensionCodec for ArrowFlightReadExecProtoCodec {
    fn try_decode(
        &self,
        buf: &[u8],
        inputs: &[Arc<dyn ExecutionPlan>],
        registry: &dyn FunctionRegistry,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        let ArrowFlightReadExecProto {
            partitioning,
            stage_context,
        } = ArrowFlightReadExecProto::decode(buf).map_err(|err| proto_error(format!("{err}")))?;

        if inputs.len() != 1 {
            return Err(proto_error(format!(
                "Expected exactly 1 input, but got {}",
                inputs.len()
            )));
        }

        let Some(stage_context) = stage_context else {
            return Err(proto_error("Missing stage context"));
        };

        let Some(partitioning) = parse_protobuf_partitioning(
            partitioning.as_ref(),
            registry,
            &inputs[0].schema(),
            &DefaultPhysicalExtensionCodec {},
        )?
        else {
            return Err(proto_error("Partitioning not specified"));
        };
        let mut node = ArrowFlightReadExec::new(inputs[0].clone(), partitioning);

        node.stage_context = Some(StageContext {
            query_id: parse_uuid(&stage_context.query_id)?,
            idx: stage_context.idx as usize,
            n_tasks: stage_context.n_tasks as usize,
            input: match stage_context.input {
                None => None,
                Some(v) => Some(v.to_input_stage()?),
            },
        });

        Ok(Arc::new(node))
    }

    fn try_encode(
        &self,
        node: Arc<dyn ExecutionPlan>,
        buf: &mut Vec<u8>,
    ) -> datafusion::common::Result<()> {
        let Some(node) = node.as_any().downcast_ref::<ArrowFlightReadExec>() else {
            return Err(proto_error(format!(
                "Expected ArrowFlightReadExec, but got {}",
                node.name()
            )));
        };
        let Some(stage_context) = &node.stage_context else {
            return Err(proto_error(
                "Upon serializing the ArrowFlightReadExec, the stage context must be set.",
            ));
        };

        ArrowFlightReadExecProto {
            partitioning: Some(serialize_partitioning(
                &node.properties().partitioning,
                &DefaultPhysicalExtensionCodec {},
            )?),
            stage_context: Some(StageContextProto {
                query_id: stage_context.query_id.to_string(),
                idx: stage_context.idx as u64,
                n_tasks: stage_context.n_tasks as u64,
                input: match &stage_context.input {
                    None => None,
                    Some(v) => Some(InputStageProto::from_input_stage(v)?),
                },
            }),
        }
        .encode(buf)
        .map_err(|err| proto_error(format!("{err}")))
    }
}

/// Protobuf representation of the [ArrowFlightReadExec] physical node. It serves as
/// an intermediate format for serializing/deserializing [ArrowFlightReadExec] nodes
/// to send them over the wire.
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct ArrowFlightReadExecProto {
    #[prost(message, optional, tag = "1")]
    partitioning: Option<protobuf::Partitioning>,
    #[prost(message, tag = "2")]
    stage_context: Option<StageContextProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct StageContextProto {
    #[prost(string, tag = "1")]
    pub query_id: String,
    #[prost(uint64, tag = "2")]
    pub idx: u64,
    #[prost(uint64, tag = "3")]
    pub n_tasks: u64,
    #[prost(message, tag = "4")]
    pub input: Option<InputStageProto>,
}

#[derive(Clone, PartialEq, ::prost::Message)]
pub struct InputStageProto {
    #[prost(uint64, tag = "1")]
    pub idx: u64,
    #[prost(string, repeated, tag = "2")]
    pub tasks: Vec<String>,
}

impl InputStageProto {
    fn to_input_stage(&self) -> Result<InputStage, DataFusionError> {
        let tasks = self
            .tasks
            .iter()
            .map(|v| Ok(Some(Url::parse(v)?)))
            .collect::<Result<Vec<_>, url::ParseError>>()
            .map_err(|err| proto_error(format!("{err}")))?;

        Ok(InputStage {
            idx: self.idx as usize,
            tasks,
        })
    }

    fn from_input_stage(stage: &InputStage) -> Result<Self, DataFusionError> {
        let tasks = stage
            .tasks
            .iter()
            .map(|v| {
                let Some(v) = v else {
                    return Err(proto_error(
                        "Cannot serialize an input stage with non assigned URLs",
                    ));
                };
                Ok(v.to_string())
            })
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self {
            idx: stage.idx as u64,
            tasks,
        })
    }
}

fn parse_uuid(uuid: &String) -> Result<Uuid, DataFusionError> {
    uuid.parse::<Uuid>()
        .map_err(|err| proto_error(format!("{err}")))
}

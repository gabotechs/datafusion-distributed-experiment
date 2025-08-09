use crate::channel_manager::ChannelManager;
use crate::composed_extension_codec::ComposedPhysicalExtensionCodec;
use crate::context::{StageContext, StageTaskContext};
use crate::errors::tonic_status_to_datafusion_error;
use crate::flight_service::DoGet;
use crate::plan::arrow_flight_read_proto::ArrowFlightReadExecProtoCodec;
use crate::user_provided_codec::get_user_codec;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use datafusion::common::{exec_err, internal_err, plan_err};
use datafusion::error::DataFusionError;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::{TryFutureExt, TryStreamExt};
use std::any::Any;
use std::fmt::Formatter;
use std::sync::Arc;
use tonic::IntoRequest;

#[derive(Debug, Clone)]
pub struct ArrowFlightReadExec {
    properties: PlanProperties,
    child: Arc<dyn ExecutionPlan>,
    pub(crate) stage_context: Option<StageContext>,
}

impl ArrowFlightReadExec {
    pub fn new(child: Arc<dyn ExecutionPlan>, partitioning: Partitioning) -> Self {
        Self {
            properties: PlanProperties::new(
                EquivalenceProperties::new(child.schema()),
                partitioning,
                EmissionType::Incremental,
                Boundedness::Bounded,
            ),
            child,
            stage_context: None,
        }
    }
}

impl DisplayAs for ArrowFlightReadExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> std::fmt::Result {
        let (hash_expr, size) = match &self.properties.partitioning {
            Partitioning::RoundRobinBatch(size) => (vec![], size),
            Partitioning::Hash(hash_expr, size) => (hash_expr.clone(), size),
            Partitioning::UnknownPartitioning(size) => (vec![], size),
        };

        write!(f, "ArrowFlightReadExec:")?;

        match &self.stage_context {
            None => write!(f, " (Unassigned stage)")?,
            Some(stage) => write!(
                f,
                " stage_idx={} input_stage_idx={}",
                stage.idx,
                stage
                    .input
                    .as_ref()
                    .map(|v| v.idx.to_string())
                    .unwrap_or("None".to_string()),
            )?,
        };

        write!(f, " input_tasks={size}")?;

        if !hash_expr.is_empty() {
            write!(f, " hash_expr=[")?;
            for (i, expr) in hash_expr.iter().enumerate() {
                if i > 0 {
                    write!(f, ", ")?;
                }
                write!(f, "{expr}")?;
            }
            write!(f, "]")?;
        }
        Ok(())
    }
}

impl ExecutionPlan for ArrowFlightReadExec {
    fn name(&self) -> &str {
        "ArrowFlightReadExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.child]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return plan_err!(
                "ArrowFlightReadExec: wrong number of children, expected 1, got {}",
                children.len()
            );
        }
        Ok(Arc::new(Self {
            properties: self.properties.clone(),
            child: Arc::clone(&children[0]),
            stage_context: self.stage_context.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let plan = Arc::clone(&self.child);
        let channel_manager: ChannelManager = context.as_ref().try_into()?;

        let Some(stage) = self.stage_context.clone() else {
            return plan_err!("No stage assigned to this ArrowFlightReadExec");
        };
        let task_context = context.session_config().get_extension::<StageTaskContext>();

        let hash_expr = match &self.properties.partitioning {
            Partitioning::Hash(hash_expr, _) => hash_expr.clone(),
            _ => vec![],
        };
        let Some(input_stage) = stage.input else {
            return plan_err!("No input stage assigned to this ArrowFlightReadExec");
        };

        let stream = async move {
            if partition >= input_stage.tasks.len() {
                return internal_err!(
                    "Invalid partition {partition} for a stage with only {} inputs",
                    input_stage.tasks.len()
                );
            }

            let Some(ref url) = input_stage.tasks[partition] else {
                return exec_err!(
                    "Task {partition} from input stage {} does not have a URL assigned",
                    input_stage.idx
                );
            };

            let channel = channel_manager.get_channel_for_url(url).await?;

            let mut codec = ComposedPhysicalExtensionCodec::default();
            codec.push(ArrowFlightReadExecProtoCodec);
            if let Some(user_codec) = get_user_codec(context.session_config()) {
                codec.push_arc(user_codec);
            }

            let ticket = DoGet::new_remote_plan_exec_ticket(
                plan,
                stage.query_id,
                input_stage.idx,
                partition,
                task_context.as_ref().map(|v| v.task_idx).unwrap_or(0),
                stage.n_tasks,
                &hash_expr,
                &codec,
            )?;

            let mut client = FlightServiceClient::new(channel);
            let stream = client
                .do_get(ticket.into_request())
                .await
                .map_err(|err| {
                    tonic_status_to_datafusion_error(&err)
                        .unwrap_or_else(|| DataFusionError::External(Box::new(err)))
                })?
                .into_inner()
                .map_err(|err| FlightError::Tonic(Box::new(err)));

            Ok(
                FlightRecordBatchStream::new_from_flight_data(stream).map_err(|err| match err {
                    FlightError::Tonic(status) => tonic_status_to_datafusion_error(&status)
                        .unwrap_or_else(|| DataFusionError::External(Box::new(status))),
                    err => DataFusionError::External(Box::new(err)),
                }),
            )
        }
        .try_flatten_stream();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

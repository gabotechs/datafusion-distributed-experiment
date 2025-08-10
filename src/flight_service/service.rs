use crate::channel_manager::ChannelManager;
use crate::flight_service::session_builder::NoopSessionBuilder;
use crate::flight_service::stream_partitioner_registry::StreamPartitionerRegistry;
use crate::flight_service::SessionBuilder;
use crate::{BoxCloneSyncChannel, ChannelResolver};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use async_trait::async_trait;
use datafusion::execution::runtime_env::RuntimeEnv;
use futures::stream::BoxStream;
use hyper_util::rt::TokioIo;
use std::cell::RefCell;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::Arc;
use tonic::codegen::tokio_stream;
use tonic::transport::{Endpoint, Server};
use tonic::{Request, Response, Status, Streaming};
use uuid::Uuid;

thread_local! {
    static LOOPBACK_CHANNEL: RefCell<HashMap<Uuid, BoxCloneSyncChannel>> = RefCell::new(HashMap::new());
}

#[derive(Clone)]
pub struct ArrowFlightEndpoint {
    pub(super) channel_manager: Arc<ChannelManager>,
    pub(super) runtime: Arc<RuntimeEnv>,
    pub(super) partitioner_registry: Arc<StreamPartitionerRegistry>,
    pub(super) session_builder: Arc<dyn SessionBuilder + Send + Sync>,
    pub(super) loopback_channel_id: Uuid,
}

const DUMMY_URL: &str = "http://[::]:50051";

impl ArrowFlightEndpoint {
    pub fn new(channel_resolver: impl ChannelResolver + Send + Sync + 'static) -> Self {
        Self {
            channel_manager: Arc::new(ChannelManager::new(channel_resolver)),
            runtime: Arc::new(RuntimeEnv::default()),
            partitioner_registry: Arc::new(StreamPartitionerRegistry::default()),
            session_builder: Arc::new(NoopSessionBuilder),
            // dummy placeholder that we are about to populate.
            loopback_channel_id: Uuid::new_v4(),
        }
    }

    pub fn with_session_builder(
        &mut self,
        session_builder: impl SessionBuilder + Send + Sync + 'static,
    ) {
        self.session_builder = Arc::new(session_builder);
    }

    pub fn loopback_channel(&self) -> BoxCloneSyncChannel {
        LOOPBACK_CHANNEL.with_borrow_mut(|chs| match chs.entry(self.loopback_channel_id) {
            Entry::Occupied(v) => v.get().clone(),
            Entry::Vacant(empty) => {
                let new_ch = self.clone().in_memory();
                empty.insert(new_ch.clone());
                new_ch
            }
        })
    }

    fn in_memory(self) -> BoxCloneSyncChannel {
        let (client, server) = tokio::io::duplex(1024 * 1024);

        tokio::spawn(async move {
            Server::builder()
                .add_service(FlightServiceServer::new(self))
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server)))
                .await
        });

        let mut client = Some(client);
        let channel = Endpoint::try_from(DUMMY_URL)
            .expect("Invalid dummy URL for building an endpoint. This should never happen")
            .connect_with_connector_lazy(tower::service_fn(move |_| {
                let client = client
                    .take()
                    .expect("Client taken twice. This should never happen");
                async move { Ok::<_, std::io::Error>(TokioIo::new(client)) }
            }));
        BoxCloneSyncChannel::new(channel)
    }
}

#[async_trait]
impl FlightService for ArrowFlightEndpoint {
    type HandshakeStream = BoxStream<'static, Result<HandshakeResponse, Status>>;

    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type ListFlightsStream = BoxStream<'static, Result<FlightInfo, Status>>;

    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn poll_flight_info(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    async fn get_schema(
        &self,
        _: Request<FlightDescriptor>,
    ) -> Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type DoGetStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> Result<Response<Self::DoGetStream>, Status> {
        self.get(request).await
    }

    type DoPutStream = BoxStream<'static, Result<PutResult, Status>>;

    async fn do_put(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type DoExchangeStream = BoxStream<'static, Result<FlightData, Status>>;

    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type DoActionStream = BoxStream<'static, Result<arrow_flight::Result, Status>>;

    async fn do_action(
        &self,
        _: Request<Action>,
    ) -> Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }

    type ListActionsStream = BoxStream<'static, Result<ActionType, Status>>;

    async fn list_actions(
        &self,
        _: Request<Empty>,
    ) -> Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("Not yet implemented"))
    }
}

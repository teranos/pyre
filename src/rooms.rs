//! 「[Opus 5.5] RoomsService for a plugin whose Python declares @rooms (ADR-057): the rooms」
//! 「[Opus 5.5] a plugin provides.」
//!
//! 「[Opus 5.5] The @rooms function is called with one dict, holding call (the RPC's name)」
//! 「[Opus 5.5] and request (its request), and returns the response as a dict. A dict that」
//! 「[Opus 5.5] carries error refuses the call with it.」

use crate::proto::{
    rooms_service_server::RoomsService, CreateRoomRequest, CreateRoomResponse, GetRoomRequest,
    GetRoomResponse, InvolveInRoomRequest, InvolveInRoomResponse, ListRoomsRequest,
    ListRoomsResponse, PostToRoomRequest, PostToRoomResponse, ReadRoomRequest, ReadRoomResponse,
    RegisterRoomAgentRequest, RegisterRoomAgentResponse, RoomArrival, StreamRoomArrivalsRequest,
    WakeInRoomRequest, WakeInRoomResponse,
};
use crate::service::PythonPluginService;
use serde::{de::DeserializeOwned, Serialize};
use std::pin::Pin;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

impl PythonPluginService {
    /// 「[Opus 5.5] One room call, answered by the @rooms function.」
    async fn answer_rooms<Q: Serialize, A: DeserializeOwned>(
        &self,
        call: &str,
        request: Q,
    ) -> Result<A, Status> {
        let declared = self.handlers.state.read().rooms_handler.clone();
        let (handler, function) = declared.ok_or_else(|| {
            Status::unimplemented(format!("{} declares no @rooms function", self.name))
        })?;
        let request = serde_json::to_value(&request)
            .map_err(|e| Status::internal(format!("the {call} request did not serialize: {e}")))?;
        let argument = serde_json::json!({ "call": call, "request": request });
        let answer = self.call_declared(handler, function, argument, false).await?;
        if !answer["error"].is_null() {
            return Err(crate::service::refused(&answer["error"]));
        }
        serde_json::from_value(answer).map_err(|e| {
            Status::internal(format!("the @rooms function's answer to {call} did not read: {e}"))
        })
    }
}

#[tonic::async_trait]
impl RoomsService for PythonPluginService {
    async fn register_room_agent(
        &self,
        request: Request<RegisterRoomAgentRequest>,
    ) -> Result<Response<RegisterRoomAgentResponse>, Status> {
        Ok(Response::new(self.answer_rooms("RegisterRoomAgent", request.into_inner()).await?))
    }

    async fn create_room(
        &self,
        request: Request<CreateRoomRequest>,
    ) -> Result<Response<CreateRoomResponse>, Status> {
        Ok(Response::new(self.answer_rooms("CreateRoom", request.into_inner()).await?))
    }

    async fn list_rooms(
        &self,
        request: Request<ListRoomsRequest>,
    ) -> Result<Response<ListRoomsResponse>, Status> {
        Ok(Response::new(self.answer_rooms("ListRooms", request.into_inner()).await?))
    }

    async fn get_room(
        &self,
        request: Request<GetRoomRequest>,
    ) -> Result<Response<GetRoomResponse>, Status> {
        Ok(Response::new(self.answer_rooms("GetRoom", request.into_inner()).await?))
    }

    async fn involve_in_room(
        &self,
        request: Request<InvolveInRoomRequest>,
    ) -> Result<Response<InvolveInRoomResponse>, Status> {
        Ok(Response::new(self.answer_rooms("InvolveInRoom", request.into_inner()).await?))
    }

    async fn post_to_room(
        &self,
        request: Request<PostToRoomRequest>,
    ) -> Result<Response<PostToRoomResponse>, Status> {
        Ok(Response::new(self.answer_rooms("PostToRoom", request.into_inner()).await?))
    }

    async fn wake_in_room(
        &self,
        request: Request<WakeInRoomRequest>,
    ) -> Result<Response<WakeInRoomResponse>, Status> {
        Ok(Response::new(self.answer_rooms("WakeInRoom", request.into_inner()).await?))
    }

    async fn read_room(
        &self,
        request: Request<ReadRoomRequest>,
    ) -> Result<Response<ReadRoomResponse>, Status> {
        Ok(Response::new(self.answer_rooms("ReadRoom", request.into_inner()).await?))
    }

    type StreamRoomArrivalsStream = Pin<Box<dyn Stream<Item = Result<RoomArrival, Status>> + Send>>;

    /// 「[Opus 5.5] A @rooms function returns once, so it carries no stream: what arrives is」
    /// 「[Opus 5.5] read from the room (ReadRoom).」
    async fn stream_room_arrivals(
        &self,
        _request: Request<StreamRoomArrivalsRequest>,
    ) -> Result<Response<Self::StreamRoomArrivalsStream>, Status> {
        Err(Status::unimplemented(format!(
            "{}'s @rooms function streams no arrivals: read the room",
            self.name
        )))
    }
}

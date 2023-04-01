mod generated;

use crate::bytes::Bytes;
use crate::ops::{
    CompactReq, DeadlineReq, DeleteReq, DequeueReq, EnqueueReq, EnqueueResp, IdPattern, InfoReq,
    PeekReq, ReleaseReq, RequeueReq, ValueReq,
};
use crate::promise::QronoFuture;
use crate::result::QronoResult;
use std::sync::Arc;

use generated::qrono::qrono_server::{Qrono, QronoServer};
use generated::qrono::{
    CompactRequest, CompactResponse, DeleteRequest, DeleteResponse, DequeueRequest,
    DequeueResponse, EnqueueRequest, EnqueueResponse, InfoRequest, InfoResponse, PeekRequest,
    PeekResponse, ReleaseRequest, ReleaseResponse, RequeueRequest, RequeueResponse,
};
use prost_types::Timestamp;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;

use crate::error::QronoError;
use crate::grpc::generated::qrono::{Item, Stats};
use tonic::{Request, Response, Status, Streaming};

pub mod messages {
    pub use super::generated::qrono::{
        CompactRequest, CompactResponse, DeleteRequest, DeleteResponse, DequeueRequest,
        DequeueResponse, EnqueueRequest, EnqueueResponse, InfoRequest, InfoResponse, PeekRequest,
        PeekResponse, ReleaseRequest, ReleaseResponse, RequeueRequest, RequeueResponse,
    };
}

pub use generated::qrono::qrono_client::QronoClient;

struct QronoService {
    qrono: Arc<crate::service::Qrono>,
}

#[tonic::async_trait]
impl Qrono for QronoService {
    async fn enqueue(
        &self,
        request: Request<EnqueueRequest>,
    ) -> Result<Response<EnqueueResponse>, Status> {
        let request = request.into_inner();
        let queue = request.queue.as_str();
        let req = EnqueueReq {
            value: ValueReq::Bytes(Bytes::from(request.value)),
            deadline: request.deadline.try_into()?,
        };

        let (promise, resp) = QronoFuture::new_std();
        self.qrono.enqueue(queue, req, promise);
        let resp = resp.await?;
        Ok(Response::new(EnqueueResponse {
            deadline: Some(resp.deadline.into()),
        }))
    }

    type EnqueueManyStream = UnboundedReceiverStream<Result<EnqueueResponse, tonic::Status>>;

    async fn enqueue_many(
        &self,
        request: Request<Streaming<EnqueueRequest>>,
    ) -> Result<Response<Self::EnqueueManyStream>, tonic::Status> {
        let mut req_stream = request.into_inner();
        let qrono = self.qrono.clone();
        let (resp_tx, resp_rx) = mpsc::unbounded_channel();
        let (resp_adapter_tx, mut resp_adapter_rx) = mpsc::channel(500);

        // Wait on Qrono futures and send gRPC responses
        tokio::spawn(async move {
            loop {
                match resp_adapter_rx.recv().await {
                    Some(Ok(resp)) => {
                        let resp: QronoResult<EnqueueResp> = resp.await;
                        let resp = match resp {
                            Ok(resp) => Ok(EnqueueResponse {
                                deadline: Some(resp.deadline.into()),
                            }),
                            Err(err) => Err(err.into()),
                        };
                        resp_tx.send(resp).expect("response channel closed?");
                    }
                    Some(Err(err)) => {
                        resp_tx.send(Err(err)).expect("response channel closed?");
                    }
                    None => break,
                }
            }
        });

        tokio::spawn(async move {
            let mut queue_name = String::new();
            loop {
                let req = match req_stream.message().await {
                    Ok(Some(req)) => req,
                    Ok(None) => break,
                    Err(err) => {
                        resp_adapter_tx
                            .send(Err(err))
                            .await
                            .map_err(|_| "response channel closed?")
                            .unwrap();
                        break;
                    }
                };
                if !req.queue.is_empty() {
                    queue_name = req.queue;
                }
                if queue_name.is_empty() {
                    resp_adapter_tx
                        .send(Err(Status::invalid_argument("queue name required")))
                        .await
                        .map_err(|_| "response channel closed?")
                        .unwrap();
                    continue;
                }
                let queue = &queue_name;
                let deadline = match req.deadline.try_into() {
                    Ok(deadline) => deadline,
                    Err(err) => {
                        resp_adapter_tx
                            .send(Err(err))
                            .await
                            .map_err(|_| "response channel closed?")
                            .unwrap();
                        continue;
                    }
                };
                let value = ValueReq::Bytes(Bytes::from(req.value));
                let req = EnqueueReq { value, deadline };
                let (promise, resp) = QronoFuture::new_std();
                resp_adapter_tx
                    .send(Ok(resp))
                    .await
                    .map_err(|_| "response channel closed?")
                    .unwrap();
                qrono.enqueue(queue, req, promise);
            }
        });
        Ok(Response::new(UnboundedReceiverStream::new(resp_rx)))
    }

    async fn dequeue(
        &self,
        request: Request<DequeueRequest>,
    ) -> Result<Response<DequeueResponse>, Status> {
        let request = request.into_inner();
        let queue = request.queue.as_str();
        let req = DequeueReq {
            timeout: Duration::from_millis(request.timeout_millis),
            count: request.count.max(1),
        };
        let (promise, resp) = QronoFuture::new_std();
        self.qrono.dequeue(queue, req, promise);
        let resp = resp.await?;
        let items = resp.into_iter().map(|item| item.into()).collect();
        Ok(Response::new(DequeueResponse { item: items }))
    }

    async fn requeue(
        &self,
        request: Request<RequeueRequest>,
    ) -> Result<Response<RequeueResponse>, Status> {
        let request = request.into_inner();
        let queue = request.queue.as_str();
        let req = RequeueReq {
            id: request.id_pattern.try_into()?,
            deadline: request.deadline.try_into()?,
        };
        let (promise, resp) = QronoFuture::new_std();
        self.qrono.requeue(queue, req, promise);
        let resp = resp.await?;
        Ok(Response::new(RequeueResponse {
            deadline: Some(resp.deadline.into()),
        }))
    }

    async fn release(
        &self,
        request: Request<ReleaseRequest>,
    ) -> Result<Response<ReleaseResponse>, Status> {
        let request = request.into_inner();
        let queue = request.queue.as_str();
        let req = ReleaseReq {
            id: request.id_pattern.try_into()?,
        };
        let (promise, resp) = QronoFuture::new_std();
        self.qrono.release(queue, req, promise);
        resp.await?;
        Ok(Response::new(ReleaseResponse {}))
    }

    async fn peek(&self, request: Request<PeekRequest>) -> Result<Response<PeekResponse>, Status> {
        let request = request.into_inner();
        let queue = request.queue.as_str();
        let req = PeekReq {};
        let (promise, resp) = QronoFuture::new_std();
        self.qrono.peek(queue, req, promise);
        let item = Some(resp.await?.into());
        Ok(Response::new(PeekResponse { item }))
    }

    async fn compact(
        &self,
        request: Request<CompactRequest>,
    ) -> Result<Response<CompactResponse>, Status> {
        let request = request.into_inner();
        let queue = request.queue.as_str();
        let req = CompactReq {};
        let (promise, resp) = QronoFuture::new_std();
        self.qrono.compact(queue, req, promise);
        resp.await?;
        Ok(Response::new(CompactResponse {}))
    }

    async fn info(&self, request: Request<InfoRequest>) -> Result<Response<InfoResponse>, Status> {
        let request = request.into_inner();
        let queue = request.queue.as_str();
        let req = InfoReq;
        let (promise, resp) = QronoFuture::new_std();
        self.qrono.info(queue, req, promise);
        let info = resp.await?;
        Ok(Response::new(InfoResponse {
            pending: info.pending,
            dequeued: info.dequeued,
        }))
    }

    async fn delete(
        &self,
        request: Request<DeleteRequest>,
    ) -> Result<Response<DeleteResponse>, Status> {
        let request = request.into_inner();
        let queue = request.queue.as_str();
        let req = DeleteReq;
        let (promise, resp) = QronoFuture::new_std();
        self.qrono.delete(queue, req, promise);
        resp.await?;
        Ok(Response::new(DeleteResponse {}))
    }
}

pub fn service(qrono: Arc<crate::service::Qrono>) -> QronoServer<impl Qrono> {
    QronoServer::new(QronoService { qrono })
}

impl From<Timestamp> for crate::data::Timestamp {
    fn from(t: Timestamp) -> Self {
        let millis = 1_000 * t.seconds + (t.nanos as i64 / 1_000_000);
        Self::from_millis(millis)
    }
}

impl From<crate::data::Timestamp> for Timestamp {
    fn from(t: crate::data::Timestamp) -> Self {
        let ms = t.millis();
        Self {
            seconds: ms / 1000,
            nanos: 1000000 * (ms % 1000) as i32,
        }
    }
}

impl From<QronoError> for Status {
    fn from(err: QronoError) -> Self {
        match err {
            QronoError::NoSuchQueue => Status::not_found("no such queue"),
            QronoError::NoItemReady => Status::unavailable("no item ready"),
            QronoError::ItemNotDequeued => Status::failed_precondition("item not dequeued"),
            QronoError::Internal => Status::internal("internal error"),
            QronoError::Canceled => Status::aborted("canceled"),
        }
    }
}

impl From<crate::data::Item> for Item {
    fn from(item: crate::data::Item) -> Self {
        Item {
            id: item.id,
            deadline: Some(item.deadline.into()),
            stats: Some(item.stats.into()),
            value: item.value.to_vec(),
        }
    }
}

impl From<crate::data::Stats> for Stats {
    fn from(stats: crate::data::Stats) -> Self {
        Stats {
            enqueue_time: Some(stats.enqueue_time.into()),
            requeue_time: Some(stats.requeue_time.into()),
            dequeue_count: stats.dequeue_count,
        }
    }
}

macro_rules! deadline_req_impl {
    ($deadline:ty) => {
        impl TryInto<DeadlineReq> for $deadline {
            type Error = Status;

            fn try_into(self) -> Result<DeadlineReq, Status> {
                Ok(match self {
                    Self::Relative(relative) => DeadlineReq::Relative(match relative.try_into() {
                        Ok(duration) => duration,
                        Err(_) => return Err(Status::invalid_argument("invalid deadline")),
                    }),
                    Self::Absolute(absolute) => DeadlineReq::Absolute(absolute.into()),
                })
            }
        }
    };
}

impl<D: TryInto<DeadlineReq, Error = Status>> TryInto<DeadlineReq> for Option<D> {
    type Error = Status;

    fn try_into(self) -> Result<DeadlineReq, Self::Error> {
        match self {
            Some(req) => req.try_into(),
            None => Ok(DeadlineReq::Now),
        }
    }
}

deadline_req_impl!(generated::qrono::enqueue_request::Deadline);
deadline_req_impl!(generated::qrono::requeue_request::Deadline);

macro_rules! id_pattern_impl {
    ($id_pattern:ty) => {
        impl TryInto<IdPattern> for $id_pattern {
            type Error = Status;

            fn try_into(self) -> Result<IdPattern, Status> {
                Ok(match self {
                    Self::Any(_) => IdPattern::Any,
                    Self::Id(id) => IdPattern::Id(id),
                })
            }
        }
    };
}

impl<I: TryInto<IdPattern, Error = Status>> TryInto<IdPattern> for Option<I> {
    type Error = Status;

    fn try_into(self) -> Result<IdPattern, Self::Error> {
        match self {
            Some(id) => id.try_into(),
            None => Err(Status::invalid_argument("id required")),
        }
    }
}

id_pattern_impl!(generated::qrono::requeue_request::IdPattern);
id_pattern_impl!(generated::qrono::release_request::IdPattern);

use crate::error::QronoError;
use crate::ops::{
    CompactReq, CompactResp, DeleteReq, DeleteResp, DequeueReq, DequeueResp, EnqueueReq,
    EnqueueResp, InfoReq, InfoResp, PeekReq, PeekResp, ReleaseReq, ReleaseResp, RequeueReq,
    RequeueResp,
};
use crate::promise::QronoFuture;
use crate::service::Qrono;
use async_trait::async_trait;
use axum::body::{Body, HttpBody};
use axum::extract::rejection::JsonRejection;
use axum::extract::{Extension, FromRequest, Path, Query, RequestParts};
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, get, post};
use axum::{BoxError, Json, Router};
use serde::de::DeserializeOwned;
use std::sync::Arc;

mod error;

type QronoExt = Extension<Arc<Qrono>>;
type QronoResponse<T> = Result<Json<T>, QronoError>;

pub fn router(qrono: Arc<Qrono>) -> Router<Body> {
    let qrono: QronoExt = Extension(qrono);
    Router::<Body>::new()
        .route("/queues/:queue/enqueue", post(enqueue))
        .route("/queues/:queue/dequeue", post(dequeue))
        .route("/queues/:queue/requeue", post(requeue))
        .route("/queues/:queue/release", post(release))
        .route("/queues/:queue/peek", get(peek))
        .route("/queues/:queue/compact", post(compact))
        .route("/queues/:queue/poke", get(poke))
        .route("/queues/:queue", get(info))
        .route("/queues/:queue", delete(delete_queue))
        .layer(qrono)
}

async fn enqueue(
    Extension(qrono): QronoExt,
    Path(queue_name): Path<String>,
    Req(req): Req<EnqueueReq>,
) -> QronoResponse<EnqueueResp> {
    let (promise, future) = QronoFuture::new_std();
    qrono.enqueue(&queue_name, req, promise);
    future.await.map(Json)
}

async fn dequeue(
    Extension(qrono): QronoExt,
    Path(queue_name): Path<String>,
    Req(req): Req<DequeueReq>,
) -> QronoResponse<DequeueResp> {
    let (promise, future) = QronoFuture::new_std();
    qrono.dequeue(&queue_name, req, promise);
    future.await.map(Json)
}

async fn release(
    Extension(qrono): QronoExt,
    Path(queue_name): Path<String>,
    Req(req): Req<ReleaseReq>,
) -> QronoResponse<ReleaseResp> {
    let (promise, future) = QronoFuture::new_std();
    qrono.release(&queue_name, req, promise);
    future.await.map(Json)
}

async fn requeue(
    Extension(qrono): QronoExt,
    Path(queue_name): Path<String>,
    Req(req): Req<RequeueReq>,
) -> QronoResponse<RequeueResp> {
    let (promise, future) = QronoFuture::new_std();
    qrono.requeue(&queue_name, req, promise);
    future.await.map(Json)
}

async fn peek(
    Extension(qrono): QronoExt,
    Path(queue_name): Path<String>,
) -> QronoResponse<PeekResp> {
    let (promise, future) = QronoFuture::new_std();
    qrono.peek(&queue_name, PeekReq, promise);
    future.await.map(Json)
}

async fn info(
    Extension(qrono): QronoExt,
    Path(queue_name): Path<String>,
) -> QronoResponse<InfoResp> {
    let (promise, future) = QronoFuture::new_std();
    qrono.info(&queue_name, InfoReq, promise);
    future.await.map(Json)
}

async fn delete_queue(
    Extension(qrono): QronoExt,
    Path(queue_name): Path<String>,
) -> QronoResponse<DeleteResp> {
    let (promise, future) = QronoFuture::new_std();
    qrono.delete(&queue_name, DeleteReq, promise);
    future.await.map(Json)
}

async fn compact(
    Extension(qrono): QronoExt,
    Path(queue_name): Path<String>,
) -> QronoResponse<CompactResp> {
    let (promise, future) = QronoFuture::new_std();
    qrono.compact(&queue_name, CompactReq, promise);
    future.await.map(Json)
}

async fn poke(
    Extension(qrono): QronoExt,
    Path(queue_name): Path<String>,
) -> QronoResponse<CompactResp> {
    qrono.poke(&queue_name);
    Ok(Json(()))
}

struct Req<T>(T);

#[async_trait]
impl<T, B> FromRequest<B> for Req<T>
where
    T: DeserializeOwned + Send,
    B: HttpBody + Send,
    B::Data: Send,
    B::Error: Into<BoxError>,
{
    type Rejection = Response;

    async fn from_request(req: &mut RequestParts<B>) -> Result<Self, Self::Rejection> {
        match Json::from_request(req).await {
            Ok(Json(val)) => Ok(Self(val)),
            Err(err) => match err {
                JsonRejection::MissingJsonContentType(_) => match Query::from_request(req).await {
                    Ok(Query(val)) => Ok(Self(val)),
                    Err(err) => Err(err.into_response()),
                },
                err => Err(err.into_response()),
            },
        }
    }
}

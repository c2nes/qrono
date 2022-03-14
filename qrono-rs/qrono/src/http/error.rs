use crate::error::QronoError;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde::Serialize;

#[derive(Serialize)]
struct ErrorMessage {
    error: &'static str,
}

impl ErrorMessage {
    fn new(error: &'static str) -> Json<ErrorMessage> {
        Json(ErrorMessage { error })
    }
}

impl IntoResponse for QronoError {
    fn into_response(self) -> Response {
        fn make_response(code: StatusCode, reason: &'static str) -> Response {
            let mut resp = ErrorMessage::new(reason).into_response();
            *resp.status_mut() = code;
            resp.headers_mut()
                .insert("X-Qrono-Reason", reason.parse().unwrap());
            resp
        }

        match self {
            QronoError::NoSuchQueue => make_response(StatusCode::NOT_FOUND, "no such queue"),
            QronoError::NoItemReady => make_response(StatusCode::NO_CONTENT, "no item ready"),
            QronoError::ItemNotDequeued => make_response(StatusCode::CONFLICT, "item not dequeued"),
            QronoError::Internal => {
                make_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
            }
        }
    }
}

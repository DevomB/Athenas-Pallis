//! Localhost HTTP control plane (`control-server` feature).

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::post;
use axum::Router;
use tracing::info;

use crate::engine::EngineHandle;
use crate::error::Result;
use crate::events::{ControlEvent, Event};

/// Header checked when a secret is configured (`x-pallas-secret`).
pub const HEADER_SECRET: &str = "x-pallas-secret";

/// HTTP bind target and shared secret.
#[derive(Clone, Debug)]
pub struct ControlServerConfig {
    /// e.g. `127.0.0.1:9847`
    pub bind: String,
    /// Required header value for `HEADER_SECRET`.
    pub secret: String,
}

struct Ctx {
    handle: EngineHandle,
    secret: String,
}

fn authorize(headers: &HeaderMap, secret: &str) -> bool {
    headers
        .get(HEADER_SECRET)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| v == secret)
}

/// Serve until the process stops or the socket errors.
pub async fn serve(handle: EngineHandle, cfg: ControlServerConfig) -> Result<()> {
    let ctx = Arc::new(Ctx {
        handle,
        secret: cfg.secret,
    });
    let app = Router::new()
        .route("/pause", post(pause_handler))
        .route("/resume", post(resume_handler))
        .route("/cancel-all", post(cancel_all_handler))
        .route("/flatten", post(flatten_handler))
        .with_state(ctx);

    let listener = tokio::net::TcpListener::bind(&cfg.bind).await?;
    info!(target: "athenas_pallas::control", "listening on {}", cfg.bind);
    axum::serve(listener, app)
        .await
        .map_err(|e| crate::error::Error::Invalid(e.to_string()))?;
    Ok(())
}

async fn pause_handler(State(ctx): State<Arc<Ctx>>, headers: HeaderMap) -> StatusCode {
    if !authorize(&headers, &ctx.secret) {
        return StatusCode::UNAUTHORIZED;
    }
    match ctx
        .handle
        .send(Event::Control(ControlEvent::Pause))
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn resume_handler(State(ctx): State<Arc<Ctx>>, headers: HeaderMap) -> StatusCode {
    if !authorize(&headers, &ctx.secret) {
        return StatusCode::UNAUTHORIZED;
    }
    match ctx
        .handle
        .send(Event::Control(ControlEvent::Resume))
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn cancel_all_handler(State(ctx): State<Arc<Ctx>>, headers: HeaderMap) -> StatusCode {
    if !authorize(&headers, &ctx.secret) {
        return StatusCode::UNAUTHORIZED;
    }
    match ctx
        .handle
        .send(Event::Control(ControlEvent::CancelAll))
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

async fn flatten_handler(State(ctx): State<Arc<Ctx>>, headers: HeaderMap) -> StatusCode {
    if !authorize(&headers, &ctx.secret) {
        return StatusCode::UNAUTHORIZED;
    }
    match ctx
        .handle
        .send(Event::Control(ControlEvent::Flatten))
        .await
    {
        Ok(()) => StatusCode::NO_CONTENT,
        Err(_) => StatusCode::SERVICE_UNAVAILABLE,
    }
}

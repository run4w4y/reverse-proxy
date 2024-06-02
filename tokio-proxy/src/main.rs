mod application;
mod proxy;
mod routing;
mod server;

use std::sync::Arc;

use application::Application;
use axum::{
    extract::State,
    routing::{delete, put},
    Json, Router,
};
use http::StatusCode;
use proxy::http::HttpProxy;
use routing::RouteAll;
use serde::{Deserialize, Serialize};
use server::{axum::AxumServerBuilder, h1::HttpServerBuilder, UpstreamChange};
use tokio::sync::{mpsc::UnboundedSender, watch};
use tower::load::{CompleteOnResponse, PendingRequests};

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
enum UpstreamId {
    DefaultUpstream,
}

#[derive(Clone)]
struct AppState {
    upstream_tx: UnboundedSender<UpstreamChange<UpstreamId, String, PendingRequests<HttpProxy>>>,
}

#[derive(Serialize, Deserialize)]
struct AddUpstream {
    target: String,
    key: String,
}

#[derive(Serialize, Deserialize)]
struct RemoveUpstream {
    key: String,
}

async fn add_upstream_route(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<AddUpstream>,
) -> StatusCode {
    let proxy = PendingRequests::new(
        HttpProxy::new(payload.target),
        CompleteOnResponse::default(),
    );
    let res = state.upstream_tx.send(UpstreamChange::Add(
        UpstreamId::DefaultUpstream,
        payload.key,
        proxy,
    ));

    match res {
        Ok(_) => StatusCode::CREATED,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

async fn remove_upstream_route(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<RemoveUpstream>,
) -> StatusCode {
    let res = state.upstream_tx.send(UpstreamChange::Remove(
        UpstreamId::DefaultUpstream,
        payload.key,
    ));

    match res {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let router_svc = RouteAll::new(UpstreamId::DefaultUpstream);
    let (router_tx, router_rx) = watch::channel(router_svc);
    let srv = HttpServerBuilder::default()
        .bind("0.0.0.0:8080")
        .router_channel(router_rx)
        .upstreams_channel(rx)
        .build()
        .unwrap();

    let axum_state = Arc::new(AppState { upstream_tx: tx });
    let axum_app = Router::new()
        .route("/upstreams", put(add_upstream_route))
        .route("/upstreams", delete(remove_upstream_route))
        .with_state(axum_state);
    let axum_srv = AxumServerBuilder::default()
        .bind("0.0.0.0:8999")
        .router(axum_app)
        .build()
        .unwrap();

    Application::new()
        .server(axum_srv)
        .server(srv)
        .serve_all()
        .await;
}

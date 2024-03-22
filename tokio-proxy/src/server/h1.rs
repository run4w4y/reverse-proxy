use std::{
    convert::Infallible,
    error::Error,
    fmt::{Debug, Display},
    hash::Hash,
    net::SocketAddr,
    ops::Add,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use derive_builder::Builder;
use futures::{future, Future, FutureExt, TryFutureExt};
use http::{request::Parts, Request, Response, StatusCode};
use http_body_util::{combinators::BoxBody, BodyExt, Empty};
use hyper::{
    body::{Body, Incoming},
    server::conn::http1,
    service::service_fn,
};
use scc::hash_map::Entry;
use tokio::{
    net::ToSocketAddrs,
    sync::{mpsc, watch, RwLock},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tower::{load::Load, BoxError, Layer, Service, ServiceExt};

use crate::server::util::{poll_router_changes_rw_lock, poll_upstream_changes};

use super::{util::UpstreamStore, Server, UpstreamChange};

#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct HttpServer<Address, UpstreamId, UpstreamKey, Proxy, Router> {
    #[builder(setter(name = "bind"))]
    pub listen_addr: Address,

    #[builder(setter(name = "router_channel"))]
    pub router_rx: watch::Receiver<Router>,

    #[builder(setter(name = "upstreams_channel"))]
    pub upstream_rx: mpsc::UnboundedReceiver<UpstreamChange<UpstreamId, UpstreamKey, Proxy>>,

    #[builder(default, setter(skip))]
    pub cancel_token: CancellationToken,

    #[builder(default = "http1::Builder::new()")]
    pub hyper_conn_builder: http1::Builder,

    #[builder(default = "default_502")]
    pub bad_gateway_resp: fn(Parts) -> Response<BoxBody<Bytes, hyper::Error>>,

    #[builder(default = "true")]
    pub x_forwarded_for: bool,
}

#[async_trait::async_trait]
impl<
        Address: ToSocketAddrs + Display + Send + Sync,
        UpstreamId: Send + Sync + Hash + Eq + Debug + 'static,
        UpstreamKey: Eq + Hash + Debug + Clone + Send + Sync + 'static,
        Router: Service<Parts> + Send + Sync + Clone + 'static,
        Proxy: Service<Request<Incoming>, Response = Response<BoxBody<Bytes, hyper::Error>>>
            + Load
            + Send
            + Sync
            + 'static,
    > Server for HttpServer<Address, UpstreamId, UpstreamKey, Proxy, Router>
where
    Proxy::Error: Send + Sync + Error + Into<BoxError> + Debug,
    Proxy::Response: Send + Sync,
    Proxy::Future: Send + Sync,
    <Proxy as Load>::Metric: Debug,
    Router::Error: Send + Sync + Error,
    Router::Future: Send + Sync,
    Router::Response: Into<UpstreamId>,
{
    type Error = anyhow::Error;

    fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    async fn serve(&mut self) -> Result<(), Self::Error> {
        let init_router_svc = self.router_rx.borrow().clone();
        let router = Arc::new(RwLock::new(init_router_svc));

        let upstreams: Arc<UpstreamStore<Request<Incoming>, UpstreamId, UpstreamKey, Proxy>> =
            Arc::new(scc::HashMap::new());

        let connection_tracker = Arc::new(TaskTracker::new());

        let listener = tokio::net::TcpListener::bind(&self.listen_addr).await?;
        log::info!("Listener started on {}", self.listen_addr);

        let conn_builder = self.hyper_conn_builder.clone();
        let upstreams_clone = upstreams.clone();
        let connection_tracker_clone = connection_tracker.clone();
        let child_token = self.cancel_token.child_token();
        let router_clone = router.clone();
        let resp502 = self.bad_gateway_resp.clone();
        let x_forwarded_for = self.x_forwarded_for;

        let accept_task_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    _ = child_token.cancelled() => {
                        connection_tracker_clone.close();
                        log::info!("Received a shutdown signal");
                        break;
                    },
                    res = listener.accept() => match res {
                        Ok((inbound, peer_addr)) => {
                            let io = hyper_util::rt::TokioIo::new(inbound);

                            let upstreams_clone = upstreams_clone.clone();
                            let router_clone = router_clone.clone();
                            let resp502 = resp502.clone();

                            let handle_request = service_fn(move |mut req: Request<Incoming>| {
                                let upstreams_clone = upstreams_clone.clone();
                                let router_clone = router_clone.clone();

                                async move {
                                    if x_forwarded_for {
                                        req.headers_mut().insert("X-Forwarded-For", peer_addr.to_string().parse().unwrap());
                                    }
                                    let (req_info, body) = req.into_parts();

                                    let router_lock = router_clone.read().await;
                                    let upstream_fut = router_lock.clone().ready().await.unwrap().call(req_info.clone());
                                    drop(router_lock);

                                    let upstream = upstream_fut.await.unwrap().into();
                                    let entry = upstreams_clone.entry(upstream);

                                    match entry {
                                        Entry::Vacant(vacant_entry) => {
                                            log::error!("no upstream found for: {:?}:", vacant_entry.key());
                                            drop(vacant_entry);
                                            Ok::<_, Infallible>(resp502(req_info))
                                        }
                                        Entry::Occupied(mut proxy_entry) => {
                                            let (_, proxy) = proxy_entry.get_mut();
                                            let req = Request::from_parts(req_info.clone(), body);
                                            let proxy_fut = proxy.ready().await.unwrap().call(req);
                                            drop(proxy_entry);

                                            let resp = proxy_fut.await;
                                            match resp {
                                                Ok(res) => {
                                                    Ok(res)
                                                },
                                                Err(e) => {
                                                    log::error!("error when proxying: {:?}", e);
                                                    Ok(resp502(req_info))
                                                }
                                            }
                                        }
                                    }
                                }
                            });

                            // TODO: add connection's graceful shutdown call
                            let connection = conn_builder.serve_connection(io, handle_request);
                            connection_tracker_clone.spawn(connection);
                        },
                        Err(err) => {
                            log::error!("error accepting connection: {}", err);
                        }
                    }
                }
            }
        });

        tokio::select! {
            biased;

            _ = self.cancel_token.cancelled() => {},
            _ = poll_upstream_changes(&mut self.upstream_rx, upstreams.clone()) => {
                log::warn!("Upstream changes polling task exited unexpectedly");
            },
            _ = poll_router_changes_rw_lock(&mut self.router_rx, router.clone()) => {
                log::warn!("Router changes polling task exited unexpectedly");
            }
        };
        accept_task_handle.await?;
        log::info!("Waiting for the active connections to close");
        connection_tracker.wait().await; // wait for all the pending connections to complete

        Ok(())
    }
}

fn default_502(_: Parts) -> Response<BoxBody<Bytes, hyper::Error>> {
    Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .body(empty_body())
        .unwrap()
}

fn empty_body() -> BoxBody<Bytes, hyper::Error> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

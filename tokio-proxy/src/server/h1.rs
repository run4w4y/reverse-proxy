use std::{
    error::Error,
    fmt::{Debug, Display},
    hash::Hash,
    sync::Arc,
};

use bytes::Bytes;
use derive_builder::Builder;
use http::{Request, Response, StatusCode};
use http_body_util::{combinators::BoxBody, BodyExt, Empty};
use hyper::{
    body::Incoming,
    server::conn::http1,
    service::service_fn,
    HeaderMap,
};
use scc::hash_map::Entry;
use tokio::{
    net::ToSocketAddrs,
    sync::{mpsc, watch, Mutex},
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tower::{load::Load, BoxError, Service, ServiceExt};

use crate::server::util::{poll_router_changes, poll_upstream_changes};

use super::{util::UpstreamStore, Server, UpstreamChange};

#[derive(Debug, Builder)]
#[builder(pattern = "owned")]
pub struct HttpServer<Address, UpstreamId, UpstreamKey, Proxy, MakeRouter> {
    #[builder(setter(name = "bind"))]
    pub listen_addr: Address,

    #[builder(setter(name = "router_channel"))]
    pub router_rx: watch::Receiver<MakeRouter>,

    #[builder(setter(name = "upstreams_channel"))]
    pub upstream_rx: mpsc::UnboundedReceiver<UpstreamChange<UpstreamId, UpstreamKey, Proxy>>,

    #[builder(default, setter(skip))]
    pub cancel_token: CancellationToken,

    #[builder(default = "http1::Builder::new()")]
    pub hyper_conn_builder: http1::Builder,
}

#[async_trait::async_trait]
impl<
        Address: ToSocketAddrs + Display + Send + Sync,
        UpstreamId: Send + Sync + Hash + Eq + Debug + 'static,
        UpstreamKey: Eq + Hash + Debug + Clone + Send + Sync + 'static,
        Router: Service<HeaderMap, Response = UpstreamId> + Send + Sync,
        MakeRouter: Service<(), Response = Router> + Send + Sync + Clone + 'static,
        Proxy: Service<Request<Incoming>, Response = Response<BoxBody<Bytes, hyper::Error>>> + Load + Send + Sync + 'static,
    > Server for HttpServer<Address, UpstreamId, UpstreamKey, Proxy, MakeRouter>
where
    Proxy::Error: Send + Sync + Error + Into<BoxError> + Debug,
    Proxy::Response: Send + Sync,
    Proxy::Future: Send + Sync,
    <Proxy as Load>::Metric: Debug,
    Router::Error: Send + Sync + Error,
    Router::Future: Send + Sync,
    MakeRouter::Error: Debug,
    MakeRouter::Future: Send + Sync,
{
    type Error = anyhow::Error;

    fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    async fn serve(&mut self) -> Result<(), Self::Error> {
        let init_router_svc = self.router_rx.borrow().clone();
        let make_router = Arc::new(Mutex::new(init_router_svc));

        let upstreams: Arc<UpstreamStore<Request<Incoming>, UpstreamId, UpstreamKey, Proxy>> =
            Arc::new(scc::HashMap::new());

        let connection_tracker = Arc::new(TaskTracker::new());

        let listener = tokio::net::TcpListener::bind(&self.listen_addr).await?;
        log::info!("Listener started on {}", self.listen_addr);

        let conn_builder = self.hyper_conn_builder.clone();
        let upstreams_clone = upstreams.clone();
        let connection_tracker_clone = connection_tracker.clone();
        let child_token = self.cancel_token.child_token();
        let make_router_clone = make_router.clone();
        let accept_task_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;

                    _ = child_token.cancelled() => {
                        connection_tracker_clone.close();
                        break;
                    },
                    res = listener.accept() => match res {
                        Ok((inbound, peer_addr)) => {
                            let io = hyper_util::rt::TokioIo::new(inbound);

                            let upstreams_clone = upstreams_clone.clone();
                            let make_router_clone = make_router_clone.clone();
                            // TODO: add connection's graceful shutdown call
                            connection_tracker_clone.spawn(conn_builder.serve_connection(io, service_fn(move |mut req: Request<Incoming>| {
                                let upstreams_clone = upstreams_clone.clone();
                                let make_router_clone = make_router_clone.clone();

                                async move {
                                    let mut make_router_lock = make_router_clone.lock().await;
                                    let router_fut = make_router_lock.call(());
                                    drop(make_router_lock);

                                    let headers = req.headers_mut();
                                    headers.insert("X-Forwarded-For", peer_addr.to_string().parse().unwrap());

                                    let mut router = router_fut.await.unwrap();
                                    let upstream = router.call(headers.clone()).await.unwrap();
                                    let entry = upstreams_clone.entry(upstream);

                                    match entry {
                                        Entry::Vacant(entry) => {
                                            log::error!("no upstream found for: {:?}:", entry.key());
                                            Response::builder().status(StatusCode::BAD_GATEWAY).body(empty())
                                        }
                                        Entry::Occupied(mut proxy_entry) => {
                                            let (_, proxy) = proxy_entry.get_mut();
                                            let proxy_fut = proxy.ready().await.unwrap().call(req);
                                            drop(proxy_entry);

                                            let resp = proxy_fut.await;
                                            match resp {
                                                Ok(res) => {
                                                    Ok(res)
                                                },
                                                Err(e) => {
                                                    log::error!("error when proxying: {:?}", e);
                                                    Ok(Response::builder().status(StatusCode::BAD_GATEWAY).body(empty()).unwrap())
                                                }
                                            }
                                        }
                                    }
                                }
                            })));
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
            _ = poll_router_changes(&mut self.router_rx, make_router.clone()) => {
                log::warn!("Router changes polling task exited unexpectedly");
            }
        };
        accept_task_handle.await?;
        connection_tracker.wait().await; // wait for all the pending connections to complete

        Ok(())
    }
}

fn empty() -> BoxBody<Bytes, hyper::Error> {
    Empty::<Bytes>::new()
        .map_err(|never| match never {})
        .boxed()
}

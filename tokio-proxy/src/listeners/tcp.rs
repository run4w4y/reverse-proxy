use std::{
    collections::HashMap,
    convert::Infallible,
    error::Error,
    fmt::{Debug, Display},
    hash::Hash,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use futures::{
    channel::mpsc::UnboundedReceiver,
    future::{self, poll_fn},
    pin_mut, Future, Stream, StreamExt, TryStream,
};
use tokio::{
    io::copy_bidirectional,
    net::{TcpStream, ToSocketAddrs},
    sync::{
        mpsc::{self, error::SendError},
        watch, RwLock,
    },
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tower::{
    balance::p2c,
    discover::{Change, Discover, ServiceList},
    load::Load,
    BoxError, Service,
};

#[derive(Debug, Clone)]
pub enum SourceConfigProto<UpstreamId, UpstreamKey, UpstreamService> {
    Shutdown,
    UpstreamAdd(UpstreamId, UpstreamKey, UpstreamService),
    UpstreamRemove(UpstreamId, UpstreamKey),
}

pub type TcpAccept = (TcpStream, SocketAddr);

#[async_trait::async_trait]
pub trait Source<Req, S: Service<Req> + Send, RoutingReq> {
    type Error;

    async fn io_loop<
        UpstreamId: Eq + Hash + Debug + Clone + Send + Sync + 'static,
        UpstreamKey: Eq + Hash + Debug + Clone + Send + Sync + 'static,
        RoutingS: Service<RoutingReq, Response = UpstreamId> + Clone + Send + Sync + 'static,
    >(
        self,
        proto_rx: mpsc::Receiver<SourceConfigProto<UpstreamId, UpstreamKey, S>>,
        routing_rx: watch::Receiver<RoutingS>,
    ) -> Result<(), Self::Error>
    where
        RoutingS::Error: Send + Display,
        RoutingS::Future: Send;
}

#[derive(Debug, Clone, Copy)]
pub struct TcpSource<A> {
    pub listen_addr: A,
}

impl<A: ToSocketAddrs> TcpSource<A> {
    pub fn new(listen_addr: A) -> Self {
        TcpSource { listen_addr }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TcpProxy<A> {
    pub target_addr: A,
}

impl<A> TcpProxy<A> {
    pub fn new(target_addr: A) -> Self {
        TcpProxy { target_addr }
    }
}

impl<A: ToSocketAddrs + Copy + Send + Debug + 'static> Service<TcpAccept> for TcpProxy<A> {
    type Error = std::io::Error;
    type Response = (u64, u64);
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, (mut inbound, peer_addr): TcpAccept) -> Self::Future {
        let addr = self.target_addr.clone();
        Box::pin(async move {
            log::debug!("proxying tcp traffic from {:?} to {:?}", peer_addr, addr);
            let mut outbound = TcpStream::connect(addr).await?;
            copy_bidirectional(&mut inbound, &mut outbound).await
        })
    }
}

#[derive(Debug, Clone)]
pub struct RouteAll<T> {
    pub upstream: T,
}

impl<T> RouteAll<T> {
    pub fn new(upstream: T) -> Self {
        RouteAll { upstream }
    }
}

impl<Req, T: Copy + Send + 'static> Service<Req> for RouteAll<T> {
    type Response = T;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Req) -> Self::Future {
        Box::pin(future::ready(Ok(self.upstream)))
    }
}

#[derive(Debug)]
pub struct ServiceChannelTransport<S, R> {
    tx: mpsc::UnboundedSender<S>,
    rx: mpsc::UnboundedReceiver<R>,
}

impl<S, R> ServiceChannelTransport<S, R> {
    pub fn new_transport_pair() -> (ServiceChannelTransport<S, R>, ServiceChannelTransport<R, S>) {
        let (tx1, rx1) = mpsc::unbounded_channel();
        let (tx2, rx2) = mpsc::unbounded_channel();
        let pair1 = ServiceChannelTransport { tx: tx1, rx: rx2 };
        let pair2 = ServiceChannelTransport { tx: tx2, rx: rx1 };
        (pair1, pair2)
    }
}

impl<S, R> futures::Sink<S> for ServiceChannelTransport<S, R> {
    type Error = SendError<S>;

    fn poll_ready(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, item: S) -> Result<(), Self::Error> {
        self.tx.send(item)?;
        Ok(())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
}

impl<S, R> futures::Stream for ServiceChannelTransport<S, R> {
    type Item = Result<R, anyhow::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx).map(|i| i.map(Ok))
    }
}

#[async_trait::async_trait]
impl<
        A: ToSocketAddrs + Send + Sync + Clone + Display,
        S: Service<TcpAccept> + Load + Send + Sync + 'static,
    > Source<TcpAccept, S, SocketAddr> for TcpSource<A>
where
    S::Error: Send + Sync + Error + Into<BoxError>,
    S::Response: Send,
    S::Future: Send,
    <S as Load>::Metric: Debug,
{
    type Error = anyhow::Error;

    async fn io_loop<
        UpstreamId: Eq + Hash + Debug + Clone + Send + Send + Sync + 'static,
        UpstreamKey: Eq + Hash + Debug + Clone + Send + Sync + 'static,
        RoutingS: Service<SocketAddr, Response = UpstreamId> + Clone + Send + Sync + 'static,
    >(
        self,
        mut proto_rx: mpsc::Receiver<SourceConfigProto<UpstreamId, UpstreamKey, S>>,
        mut routing_rx: watch::Receiver<RoutingS>,
    ) -> Result<(), Self::Error>
    where
        RoutingS::Error: Send + Display,
        RoutingS::Future: Send,
    {
        let init_router_svc = routing_rx.borrow().clone();
        // NOTE: locks can be expensive, but for now i dont really see any workaround
        let router: Arc<RwLock<RoutingS>> = Arc::new(RwLock::new(init_router_svc));

        // NOTE: i dont really like how this can throw if not polled for readiness
        //       - although it makes complete sense as for why it does that it would be better to be in control of said readiness
        // NOTE: a completely different approach is possible with a multitude of tcp listener polling tasks 
        //       that await the proxy service calls internally
        // NOTE: another approach similar to the one above would be to have a number of proxy service tasks
        //       that would poll an mspc receiver and handle the incoming requests 
        //       - i feel like thats the same thing as above but worse
        //       - maybe do this but with tower::balance::pool somehow? worth looking into if out of other options
        // TODO: try to implement this as a tokio-tower server and get rid of the nasty write locks in the listener polling task
        //       - does it poll for readiness by itself? 
        //       - can it be used in parallel? (how are the calls hanlded by the server)
        //       - how expensive is creating a client for it?
        //       - a consideration: a number of servers per service that all poll the same mspc receiver
        let upstreams: Arc<
            RwLock<
                HashMap<
                    UpstreamId,
                    (
                        mpsc::UnboundedSender<Result<Change<UpstreamKey, S>, Infallible>>,
                        p2c::Balance<
                            Pin<
                                Box<
                                    UnboundedReceiverStream<
                                        Result<Change<UpstreamKey, S>, Infallible>,
                                    >,
                                >,
                            >,
                            TcpAccept,
                        >,
                    ),
                >,
            >,
        > = Arc::new(RwLock::new(HashMap::new()));
        let listener = tokio::net::TcpListener::bind(&self.listen_addr).await?;
        log::info!("Listener started on {}", self.listen_addr);

        let router_ = router.clone();
        let upstreams_ = upstreams.clone();
        // NOTE: might want to make it unbounded
        let proxy_handle = tokio::task::spawn(async move {
            let router = router_;
            let upstreams = upstreams_;
            while let Ok((inbound, peer_addr)) = listener.accept().await {
                log::debug!("Accepted TCP connection from: {}", peer_addr);

                let router_ = router.clone();
                let upstreams_ = upstreams.clone();
                tokio::spawn(async move {
                    let mut router = router_.write().await; // TODO: i really dont like this write borrow here

                    match router.call(peer_addr).await {
                        Ok(upstream_id) => {
                            log::debug!("resolved the upstream id to: {:?}", upstream_id);
                            let mut upstreams = upstreams_.write().await;
                            log::debug!("got the write lock");
                            let upstream = upstreams.get_mut(&upstream_id);
                            match upstream {
                                Some((_, ref mut svc)) => {
                                    // the readiness polling below requires svc to be mutable as well
                                    poll_fn(|cx| svc.poll_ready(cx)).await.unwrap();
                                    let res = svc.call((inbound, peer_addr)).await;
                                    if let Err(e) = res {
                                        log::error!("error when proxying: {}", e);
                                    }
                                }
                                None => {
                                    log::error!(
                                        "no active services found for upstream {:?}",
                                        upstream_id
                                    );
                                }
                            }
                        }
                        Err(e) => {
                            log::error!("failed resolving an upstream: {}", e);
                        }
                    }
                });
            }
        });

        while let Some(msg) = proto_rx.recv().await {
            match msg {
                SourceConfigProto::Shutdown => {
                    log::error!("graceful shutdown not yet implemented");
                }
                SourceConfigProto::UpstreamAdd(id, key, svc) => {
                    // NOTE: be careful to not deadlock here
                    let upstreams_rl = upstreams.read().await;
                    match upstreams_rl.get(&id) {
                        Some((tx, balanced_svc)) => {
                            log::debug!("received a new upstream id={:?} key={:?}", id, key);
                            // TODO: would be nice to poll for readiness right here
                            tx.send(Ok(Change::Insert(key, svc))).unwrap();
                        }
                        None => {
                            drop(upstreams_rl); // free the read lock before trying to acquire the write lock
                            log::debug!("creating a new load balanced service for the upstream id={:?} key={:?}", id, key);
                            let mut upstreams = upstreams.write().await;
                            log::debug!("got an upstreams write lock");
                            let (tx, rx) = mpsc::unbounded_channel::<
                                Result<Change<UpstreamKey, S>, Infallible>,
                            >();
                            tx.send(Ok(Change::Insert(key, svc))).unwrap();
                            let mut balanced_svc = p2c::Balance::new(Box::pin(UnboundedReceiverStream::new(rx)));
                            poll_fn(|cx| balanced_svc.poll_ready(cx)).await.unwrap();
                            upstreams.insert(
                                id,
                                (
                                    tx,
                                    balanced_svc,
                                ),
                            );
                        }
                    }
                }
                SourceConfigProto::UpstreamRemove(id, key) => {
                    match upstreams.read().await.get(&id) {
                        Some((tx, _)) => {
                            tx.send(Ok(Change::Remove(key)));
                        }
                        None => {
                            log::warn!("No upstream found with the id: {:?}", id);
                        }
                    }
                }
            }
        }

        proxy_handle.await?;

        Ok(())
    }
}

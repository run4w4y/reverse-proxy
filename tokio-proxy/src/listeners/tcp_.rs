use std::{
    convert::Infallible,
    error::Error,
    fmt::{Debug, Display},
    hash::Hash,
    net::{Ipv4Addr, SocketAddr, SocketAddrV4},
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use futures::{
    channel::mpsc::UnboundedReceiver,
    future::{self, poll_fn},
    pin_mut, Future, Stream, StreamExt, TryFutureExt, TryStream,
};
use slab::Slab;
use tokio::{
    io::{copy_bidirectional, AsyncWriteExt},
    net::{TcpStream, ToSocketAddrs},
    sync::{
        mpsc::{self, error::SendError},
        watch, RwLock,
    },
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_tower::multiplex::{self, TagStore};
use tower::{
    balance::{self, p2c},
    discover::{Change, Discover, ServiceList},
    load::Load,
    BoxError, Service, ServiceExt,
};

#[derive(Debug, Clone)]
pub enum SourceConfigProto<UpstreamId, UpstreamKey, UpstreamService> {
    Shutdown,
    UpstreamAdd(UpstreamId, UpstreamKey, UpstreamService),
    UpstreamRemove(UpstreamId, UpstreamKey),
}

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
        RoutingS::Error: Send + Display + Debug,
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

impl<A: ToSocketAddrs + Copy + Send + Debug + 'static, Tag: Send + 'static> Service<TcpAccept<Tag>>
    for TcpProxy<A>
{
    type Error = std::io::Error;
    type Response = TcpTransferReponse<Tag>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, accept: TcpAccept<Tag>) -> Self::Future {
        let addr = self.target_addr.clone();
        Box::pin(async move {
            let mut inbound = accept.stream;
            log::debug!("proxying tcp traffic from {:?} to {:?}", accept.addr, addr);
            let mut outbound = TcpStream::connect(addr).await?;
            let res = copy_bidirectional(&mut inbound, &mut outbound).await?;
            inbound.shutdown().await?;
            outbound.shutdown().await?;
            Ok(TcpTransferReponse::new(res, accept.tag))
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

// async fn serve_over_mpsc<Tag: Eq, Req: Send + Tagged<Tag>, S: Service<Req>>(
//     mut svc: S,
//     rx: async_channel::Receiver<Req>,
//     tx: async_channel::Sender<Result<(Tag, S::Response), (Tag, S::Error)>>,
// ) where
//     S::Error: Send + Debug,
//     S::Future: Send,
//     S::Response: Send,
// {
//     while let Ok(req) = rx.recv().await {
//         let tag = req.get_tag();
//         match svc.ready().await {
//             Ok(ref mut svc) => {
//                 let fut = svc.call(req);
//                 tokio::spawn(fut);
//             }
//             Err(e) => {
//                 tx.send(Err((tag, e))).await;
//             }
//         }
//     }
// }

pub trait Tagged<T> {
    fn get_tag(&self) -> T;
}

pub struct TcpAccept<Tag> {
    stream: TcpStream,
    addr: SocketAddr,
    tag: Tag,
}

pub struct TcpTransferReponse<Tag> {
    res: (u64, u64),
    tag: Tag,
}

impl<Tag> TcpTransferReponse<Tag> {
    fn new(res: (u64, u64), tag: Tag) -> Self {
        TcpTransferReponse { res, tag }
    }
}

impl<Tag: Copy> Tagged<Tag> for TcpTransferReponse<Tag> {
    fn get_tag(&self) -> Tag {
        self.tag
    }
}

#[async_trait::async_trait]
impl<
        A: ToSocketAddrs + Send + Sync + Clone + Display,
        S: Service<TcpAccept<usize>> + Load + Send + Sync + 'static,
    > Source<TcpAccept<usize>, S, SocketAddr> for TcpSource<A>
where
    S::Error: Send + Sync + Error + Into<BoxError> + Debug,
    S::Response: Send + Tagged<usize>,
    S::Future: Send,
    <S as Load>::Metric: Debug,
{
    type Error = anyhow::Error;

    async fn io_loop<
        UpstreamId: Eq + Hash + Debug + Clone + Send + Sync + 'static,
        UpstreamKey: Eq + Hash + Debug + Clone + Send + Sync + 'static,
        RoutingS: Service<SocketAddr, Response = UpstreamId> + Clone + Send + Sync + 'static,
    >(
        self,
        mut proto_rx: mpsc::Receiver<SourceConfigProto<UpstreamId, UpstreamKey, S>>,
        routing_rx: watch::Receiver<RoutingS>,
    ) -> Result<(), Self::Error>
    where
        RoutingS::Error: Send + Display + Debug,
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
            scc::HashMap<
                UpstreamId,
                (
                    mpsc::UnboundedSender<Result<Change<UpstreamKey, S>, Infallible>>,
                    p2c::Balance<
                        Pin<
                            Box<
                                UnboundedReceiverStream<Result<Change<UpstreamKey, S>, Infallible>>,
                            >,
                        >,
                        TcpAccept<usize>,
                    >,
                ),
            >,
        > = Arc::new(scc::HashMap::new());

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
                    let upstream_resolve_fut = router.ready().await.unwrap().call(peer_addr);
                    drop(router);

                    match upstream_resolve_fut.await {
                        Ok(upstream_id) => {
                            log::debug!("resolved the upstream id to: {:?}", upstream_id);
                            let upstream = upstreams_.get_async(&upstream_id).await;
                            match upstream {
                                Some(mut entry) => {
                                    let (_, ref mut svc) = *entry.get_mut();
                                    // the readiness polling below requires svc to be mutable as well
                                    svc.ready().await.unwrap();
                                    let res = svc
                                        .call(TcpAccept {
                                            stream: inbound,
                                            addr: peer_addr,
                                            tag: 0,
                                        })
                                        .await;
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
                    match upstreams.get_async(&id).await {
                        Some(entry) => {
                            let (tx, _client) = entry.get();
                            log::debug!("received a new upstream id={:?} key={:?}", id, key);
                            tx.send(Ok(Change::Insert(key, svc))).unwrap();
                        }
                        None => {
                            log::debug!("creating a new load balanced service for the upstream id={:?} key={:?}", id, key);
                            let (tx, rx) = mpsc::unbounded_channel::<
                                Result<Change<UpstreamKey, S>, Infallible>,
                            >();
                            tx.send(Ok(Change::Insert(key, svc))).unwrap();

                            let balanced_svc =
                                p2c::Balance::new(Box::pin(UnboundedReceiverStream::new(rx)));

                            upstreams.insert_async(id, (tx, balanced_svc)).await;
                        }
                    }
                }
                SourceConfigProto::UpstreamRemove(id, key) => {
                    match upstreams.get_async(&id).await {
                        Some(entry) => {
                            let (tx, _client) = entry.get();
                            tx.send(Ok(Change::Remove(key))).unwrap();
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

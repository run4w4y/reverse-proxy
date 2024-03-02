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
    pin_mut, Future, FutureExt, Stream, StreamExt, TryFutureExt, TryStream,
};
use scc::hash_map::{Entry, OccupiedEntry};
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
    load::{CompleteOnResponse, Load, PendingRequests},
    make::Shared,
    BoxError, Layer, MakeService, Service, ServiceBuilder, ServiceExt,
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

#[derive(Debug, Clone, Copy)]
pub struct IntoMakeTcpProxy<A> {
    inner: TcpProxy<A>,
}

impl<A> TcpProxy<A> {
    pub fn new(target_addr: A) -> Self {
        TcpProxy { target_addr }
    }

    pub fn into_make_service(self) -> IntoMakeTcpProxy<A> {
        IntoMakeTcpProxy { inner: self }
    }
}

impl<A: ToSocketAddrs + Copy + Send + Sync + 'static> Service<()> for IntoMakeTcpProxy<A> {
    type Response = TcpProxy<A>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + Sync>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, target: ()) -> Self::Future {
        Box::pin(future::ready(Ok(TcpProxy {
            target_addr: self.inner.target_addr,
        })))
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
            let _ = inbound.shutdown().await; // its probably fine if i dont do that
            let _ = outbound.shutdown().await;
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

    pub fn into_make_service(self) -> IntoMakeRouteAll<T> {
        IntoMakeRouteAll { inner: self }
    }
}

#[derive(Debug, Clone)]
pub struct IntoMakeRouteAll<T> {
    inner: RouteAll<T>,
}

impl<T: Copy + Send + 'static> Service<()> for IntoMakeRouteAll<T> {
    type Response = RouteAll<T>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, target: ()) -> Self::Future {
        Box::pin(future::ready(Ok(RouteAll {
            upstream: self.inner.upstream,
        })))
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

struct SelectUpstreamService<'a, S, K: Eq + Hash, V> {
    upstreams: &'a scc::HashMap<K, V>,
    service: S,
}

impl<'a, Req, S: Service<Req, Response = K>, K: Eq + Hash + Send + Sync, V: Sync> Service<Req>
    for SelectUpstreamService<'a, S, K, V>
where
    S::Error: Send + Sync + Error + 'a,
    S::Future: Send + 'a,
{
    type Response = Entry<'a, K, V>;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + 'a>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.service.poll_ready(cx)
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let fut = self
            .service
            .call(req)
            .and_then(|res| self.upstreams.entry_async(res).map(Ok));
        Box::pin(fut)
    }
}

struct SelectUpstreamLayer<'a, K: Eq + Hash, V> {
    upstreams: &'a scc::HashMap<K, V>,
}

impl<'a, K: Eq + Hash, V> SelectUpstreamLayer<'a, K, V> {
    pub fn new(upstreams: &'a scc::HashMap<K, V>) -> Self {
        SelectUpstreamLayer { upstreams }
    }
}

impl<'a, S, K: Eq + Hash, V> Layer<S> for SelectUpstreamLayer<'a, K, V> {
    type Service = SelectUpstreamService<'a, S, K, V>;

    fn layer(&self, inner: S) -> Self::Service {
        SelectUpstreamService {
            service: inner,
            upstreams: self.upstreams,
        }
    }
}

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
enum UpstreamId {
    DefaultUpstream,
}

impl<A: ToSocketAddrs + Display> TcpSource<A> {
    pub async fn io_loop<
        MakeProxy: Service<()> + MakeService<(), TcpAccept<usize>> + Send + Sync + 'static,
    >(
        self,
        mut make_proxy: MakeProxy,
    ) -> Result<(), anyhow::Error>
    where
        <MakeProxy as Service<()>>::Error: Send + Sync + Error,
        <MakeProxy as Service<()>>::Response: Service<TcpAccept<usize>> + Send,
        <<MakeProxy as Service<()>>::Response as Service<TcpAccept<usize>>>::Error:
            Send + Sync + Debug,
        <MakeProxy as Service<()>>::Future: Send + Sync,
        <<MakeProxy as Service<()>>::Response as Service<TcpAccept<usize>>>::Future: Send,
    {
        let mut make_router = RouteAll::new(UpstreamId::DefaultUpstream).into_make_service();

        let upstreams = Box::new(scc::HashMap::new());
        let upstreams_ref = Box::leak::<'static>(upstreams);

        let mut balanced_make_proxy = p2c::Balance::new(ServiceList::new(vec![
            PendingRequests::new(make_proxy, CompleteOnResponse::default()), // this is pretty wrong, since the load handler (?) should be dropped only after the produced service responds
        ]));

        upstreams_ref.insert(UpstreamId::DefaultUpstream, balanced_make_proxy);

        let listener = tokio::net::TcpListener::bind(&self.listen_addr).await?;
        log::info!("Listener started on {}", self.listen_addr);

        let handle = tokio::task::spawn(async move {
            while let Ok((inbound, peer_addr)) = listener.accept().await {
                let accept = TcpAccept {
                    stream: inbound,
                    addr: peer_addr,
                    tag: 0,
                };

                let mut router = make_router.call(()).await.unwrap();
                let mut route_to_proxy = ServiceBuilder::new()
                    .layer(SelectUpstreamLayer::new(upstreams_ref))
                    .service(router);
                let entry_fut = route_to_proxy.call(peer_addr);

                tokio::spawn(async move {
                    let entry = entry_fut.await.unwrap();

                    match entry {
                        Entry::Vacant(entry) => {
                            log::error!("no upstream found for: {:?}:", entry.key());
                        }
                        Entry::Occupied(mut make_proxy_entry) => {
                            let make_proxy = make_proxy_entry.get_mut();
                            let proxy_fut = make_proxy.ready().await.unwrap().call(());
                            drop(make_proxy);
                            drop(make_proxy_entry);
                            let mut proxy = proxy_fut.await.unwrap();
                            if let Err(e) = proxy.ready().await.unwrap().call(accept).await {
                                log::error!("error when proxying: {:?}", e);
                            }
                        }
                    }
                });
            }
        });

        handle.await?;

        Ok(())
    }
}

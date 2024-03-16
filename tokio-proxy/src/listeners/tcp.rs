use std::{
    convert::Infallible,
    error::Error,
    fmt::{Debug, Display},
    hash::Hash,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::Poll,
};

use futures::{future, Future};
use scc::hash_map::Entry;
use tokio::{
    io::{copy_bidirectional, AsyncWriteExt},
    net::{TcpStream, ToSocketAddrs},
    sync::{mpsc, watch, Mutex},
};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tower::{balance::p2c, discover::Change, load::Load, BoxError, Service, ServiceExt};

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

#[derive(Debug)]
pub struct TcpSource<
    Address,
    UpstreamId: Default,
    UpstreamKey,
    Proxy,
    MakeRouter = IntoMakeRouteAll<UpstreamId>,
> {
    pub listen_addr: Address,
    pub router_rx: watch::Receiver<MakeRouter>,
    pub config_rx: mpsc::UnboundedReceiver<SourceConfigProto<UpstreamId, UpstreamKey, Proxy>>,
}

#[derive(Debug)]
pub struct TcpSourceBuilder<
    Address,
    UpstreamId: Default,
    UpstreamKey,
    Proxy,
    MakeRouter = IntoMakeRouteAll<UpstreamId>,
> {
    pub listen_addr: Address,
    pub router_rx: Option<watch::Receiver<MakeRouter>>,
    pub config_rx:
        Option<mpsc::UnboundedReceiver<SourceConfigProto<UpstreamId, UpstreamKey, Proxy>>>,
}

impl<Address: Clone, UpstreamId: Default, UpstreamKey, Proxy, MakeRouter: Default>
    TcpSourceBuilder<Address, UpstreamId, UpstreamKey, Proxy, MakeRouter>
{
    pub fn router_channel(mut self, router_rx: watch::Receiver<MakeRouter>) -> Self {
        self.router_rx = Some(router_rx);
        self
    }

    pub fn config_channel(
        mut self,
        config_rx: mpsc::UnboundedReceiver<SourceConfigProto<UpstreamId, UpstreamKey, Proxy>>,
    ) -> Self {
        self.config_rx = Some(config_rx);
        self
    }

    pub fn build(self) -> TcpSource<Address, UpstreamId, UpstreamKey, Proxy, MakeRouter> {
        TcpSource {
            listen_addr: self.listen_addr.clone(),
            router_rx: self.router_rx.unwrap_or({
                let (_, rx) = watch::channel(MakeRouter::default());
                rx
            }),
            config_rx: self.config_rx.unwrap(),
        }
    }
}

impl<Address, UpstreamId: Default, UpstreamKey, Proxy, MakeRouter: Default>
    TcpSource<Address, UpstreamId, UpstreamKey, Proxy, MakeRouter>
{
    pub fn new(
        listen_addr: Address,
    ) -> TcpSourceBuilder<Address, UpstreamId, UpstreamKey, Proxy, MakeRouter> {
        TcpSourceBuilder {
            listen_addr,
            router_rx: None,
            config_rx: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TcpProxy {
    pub target_addr: SocketAddr,
}

#[derive(Debug, Clone, Copy)]
pub struct IntoMakeTcpProxy {
    inner: TcpProxy,
}

impl TcpProxy {
    pub fn new(target_addr: SocketAddr) -> Self {
        TcpProxy { target_addr }
    }

    pub fn into_make_service(self) -> IntoMakeTcpProxy {
        IntoMakeTcpProxy { inner: self }
    }
}

impl Service<()> for IntoMakeTcpProxy {
    type Response = TcpProxy;
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

impl<Tag: Send + Sync + 'static> Service<TcpAccept<Tag>> for TcpProxy {
    type Error = std::io::Error;
    type Response = TcpTransferReponse<Tag>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + Sync>>;

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

impl<UpstreamId: Default> Default for IntoMakeRouteAll<UpstreamId> {
    fn default() -> Self {
        IntoMakeRouteAll {
            inner: RouteAll::new(UpstreamId::default()),
        }
    }
}

impl<T: Copy + Send + Sync + 'static> Service<()> for IntoMakeRouteAll<T> {
    type Response = RouteAll<T>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + Sync>>;

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

impl<Req, T: Copy + Send + Sync + 'static> Service<Req> for RouteAll<T> {
    type Response = T;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + Sync>>;

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

type UpstreamStore<Id, Key, Proxy> = scc::HashMap<
    Id,
    (
        mpsc::UnboundedSender<Result<Change<Key, Proxy>, Infallible>>,
        p2c::Balance<
            Pin<Box<UnboundedReceiverStream<Result<Change<Key, Proxy>, Infallible>>>>,
            TcpAccept<usize>,
        >,
    ),
>;

impl<
        Address: ToSocketAddrs + Display,
        UpstreamId: Send + Sync + Default + Hash + Eq + Debug + 'static,
        UpstreamKey: Eq + Hash + Debug + Clone + Send + Sync + 'static,
        Router: Service<SocketAddr, Response = UpstreamId> + Send + Sync,
        MakeRouter: Service<(), Response = Router> + Send + Clone + 'static,
        Proxy: Service<TcpAccept<usize>> + Load + Send + Sync + 'static,
    > TcpSource<Address, UpstreamId, UpstreamKey, Proxy, MakeRouter>
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
    async fn poll_config_task(
        &mut self,
        upstreams: Arc<UpstreamStore<UpstreamId, UpstreamKey, Proxy>>,
        connection_tracker: Arc<TaskTracker>,
        token: CancellationToken,
    ) -> Result<(), anyhow::Error> {
        while let Some(msg) = self.config_rx.recv().await {
            match msg {
                SourceConfigProto::Shutdown => {
                    log::info!("Received a shutdown signal");
                    connection_tracker.close();
                    token.cancel();
                    break;
                }
                SourceConfigProto::UpstreamAdd(id, key, svc) => {
                    // NOTE: be careful to not deadlock here
                    match upstreams.get_async(&id).await {
                        Some(entry) => {
                            let (tx, _) = entry.get();
                            log::debug!("received a new upstream id={:?} key={:?}", id, key);
                            tx.send(Ok(Change::Insert(key, svc))).unwrap();
                        }
                        None => {
                            log::debug!("creating a new load balanced service for the upstream id={:?} key={:?}", id, key);
                            let (tx, rx) = mpsc::unbounded_channel::<
                                Result<Change<UpstreamKey, Proxy>, Infallible>,
                            >();
                            tx.send(Ok(Change::Insert(key, svc))).unwrap();

                            let balanced_svc =
                                p2c::Balance::new(Box::pin(UnboundedReceiverStream::new(rx)));

                            let _ = upstreams.insert_async(id, (tx, balanced_svc)).await;
                        }
                    }
                }
                SourceConfigProto::UpstreamRemove(id, key) => {
                    match upstreams.get_async(&id).await {
                        Some(entry) => {
                            let (tx, _) = entry.get();
                            tx.send(Ok(Change::Remove(key))).unwrap();
                        }
                        None => {
                            log::warn!("No upstream found with the id: {:?}", id);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub async fn io_loop(&mut self) -> Result<(), anyhow::Error> {
        let init_router_svc = self.router_rx.borrow().clone();
        let make_router = Arc::new(Mutex::new(init_router_svc));

        let upstreams: Arc<UpstreamStore<UpstreamId, UpstreamKey, Proxy>> =
            Arc::new(scc::HashMap::new());

        let connection_tracker = Arc::new(TaskTracker::new());
        let token = CancellationToken::new();

        let listener = tokio::net::TcpListener::bind(&self.listen_addr).await?;
        log::info!("Listener started on {}", self.listen_addr);

        let upstreams_clone = upstreams.clone();
        let connection_tracker_clone = connection_tracker.clone();
        let child_token = token.child_token();
        let accept_task_handle = tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    
                    () = child_token.cancelled() => {
                        break;
                    },
                    res = listener.accept() => match res {
                        Ok((inbound, peer_addr)) => {
                            let accept: TcpAccept<usize> = TcpAccept {
                                stream: inbound,
                                addr: peer_addr,
                                tag: 0,
                            };
            
                            let mut make_router_lock = make_router.lock().await;
                            let router_fut = make_router_lock.call(());
                            drop(make_router_lock);
            
                            let upstreams_clone = upstreams_clone.clone();
                            connection_tracker_clone.spawn(async move {
                                let mut router = router_fut.await.unwrap();
                                let upstream = router.call(peer_addr).await.unwrap();
                                let entry = upstreams_clone.entry(upstream);
            
                                match entry {
                                    Entry::Vacant(entry) => {
                                        log::error!("no upstream found for: {:?}:", entry.key());
                                    }
                                    Entry::Occupied(mut proxy_entry) => {
                                        let (_, proxy) = proxy_entry.get_mut();
                                        let proxy_fut = proxy.ready().await.unwrap().call(accept);
                                        drop(proxy);
                                        drop(proxy_entry);
                                        if let Err(e) = proxy_fut.await {
                                            log::error!("error when proxying: {:?}", e);
                                        }
                                    }
                                }
                            });
                        },
                        Err(_) => {
                            break;
                        },
                    },
                }
            }
        });

        self.poll_config_task(upstreams.clone(), connection_tracker.clone(), token).await?;
        accept_task_handle.await?;
        connection_tracker.wait().await; // wait for all the pending connections to complete

        Ok(())
    }
}

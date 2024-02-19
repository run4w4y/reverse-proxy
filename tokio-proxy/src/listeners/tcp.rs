use std::{convert::Infallible, fmt::Display, net::SocketAddr, pin::Pin, sync::Arc, task::Poll};

use tokio::{
    io::copy_bidirectional, 
    net::{TcpStream, ToSocketAddrs}, 
    sync::{
        mpsc::{self, error::SendError}, 
        watch::Receiver, 
        RwLock
    }
};
use tokio_tower::pipeline;
use tower::{discover::ServiceList, Service};
use futures::{future::poll_fn, pin_mut, Future};

// NOTE: consider just providing an Arc<RwLock> of ServiceList instead
#[derive(Debug, Clone)]
pub enum Sink<S> {
    SingleSink(S),
    MultiSink(Vec<S>)
}

// pub type TcpAccept = (TcpStream, SocketAddr);

#[derive(Debug, Clone)]
pub struct SourceConfig<S> {
    pub sink: Sink<S>
}

#[async_trait::async_trait]
pub trait Source<Req, S: Service<Req> + Send> {
    type Error;

    async fn io_loop(
        self, 
        rx: Receiver<SourceConfig<S>>
    ) -> Result<(), Self::Error>;
}

#[derive(Debug, Clone, Copy)]
pub struct TcpSource<A> {
    pub listen_addr: A
}

impl<A: ToSocketAddrs> TcpSource<A> {
    pub fn new(listen_addr: A) -> Self {
        TcpSource {
            listen_addr,
        }
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TcpProxy<A> {
    pub target_addr: A
}

impl<A: ToSocketAddrs + Copy + Send + 'static> Service<TcpStream> for TcpProxy<A> {
    type Error = std::io::Error;
    type Response = (u64, u64);
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;
    
    fn poll_ready(&mut self, cx: &mut std::task::Context<'_>) -> std::task::Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }
    
    fn call(&mut self, mut req: TcpStream) -> Self::Future {
        let addr = self.target_addr.clone();
        Box::pin(async move {
            let mut outbound = TcpStream::connect(addr).await?;
            copy_bidirectional(&mut req, &mut outbound).await
        })
    }
}

#[derive(Debug)]
pub struct TcpProxyChannelTransport<S, R> {
    tx: mpsc::UnboundedSender<S>,
    rx: mpsc::UnboundedReceiver<R>,
}

impl<S, R> futures::Sink<S> for TcpProxyChannelTransport<S, R> {
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

impl<S, R> futures::Stream for TcpProxyChannelTransport<S, R> {
    type Item = Result<R, anyhow::Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut std::task::Context) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx).map(|i| i.map(Ok))
    }
}

#[async_trait::async_trait]
impl<
    A: ToSocketAddrs + Send + Sync + Clone + Display, 
    S: Service<TcpStream> + Send + Sync + Clone + Copy + 'static,
> Source<TcpStream, S> for TcpSource<A> 
where
    S::Error: Send,
    S::Response: Send,
    S::Future: Send
{
    type Error = anyhow::Error;

    // TODO: might also be a good idea to extend the passed channel capabilities (make a "protocol")
    //       e.g passing an abortion signal for a graceful shutdown and things like that
    async fn io_loop(
        self,
        mut rx: Receiver<SourceConfig<S>>
    ) -> Result<(), Self::Error> {
        let init_config = rx.borrow().clone();
        // NOTE: locks can be expensive, but for now i dont really see any workaround
        let config = Arc::new(RwLock::new(init_config));
        let listener = tokio::net::TcpListener::bind(&self.listen_addr).await?;
        log::info!("Listener started on {}", self.listen_addr);
        
        let proxy_config = config.clone();
        // NOTE: might want to make it unbounded
        let proxy_handle = tokio::task::spawn(async move {
            let config = proxy_config;
            while let Ok((inbound, peer_addr)) = listener.accept().await {
                log::debug!("Accepted TCP connection from: {}", peer_addr);
                let config = config.read().await;
                match config.sink {
                    // NOTE: now that the Send contraint is implemented, is there any reason to use tokio_tower?
                    Sink::SingleSink(svc) => {
                        let (tx1, rx1) = mpsc::unbounded_channel();
                        let (tx2, rx2) = mpsc::unbounded_channel();
                        let pair1 = TcpProxyChannelTransport { tx: tx1, rx: rx2 };
                        let pair2 = TcpProxyChannelTransport { tx: tx2, rx: rx1 };

                        tokio::spawn(pipeline::Server::new(pair1, svc));

                        let mut client: pipeline::Client<_, tokio_tower::Error<_, _>, _> = pipeline::Client::new(pair2);
                        
                        poll_fn(|cx| client.poll_ready(cx)).await.unwrap();
                        let resp = client.call(inbound).await;
                    },
                    Sink::MultiSink(_) => panic!("not implemented")
                }
            }
        });
        
        while rx.changed().await.is_ok() {
            let mut config = config.write().await;
            *config = rx.borrow().clone();
        }

        proxy_handle.await?;

        Ok(())
    }
}

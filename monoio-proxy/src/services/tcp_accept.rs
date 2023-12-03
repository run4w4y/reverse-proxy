use monoio::{
    io::{AsyncReadRent, AsyncWriteRent, Split},
    net::{TcpListener, TcpStream},
};
use std::{
    future::Future,
    net::SocketAddr,
    rc::Rc,
    task::{Context, Poll},
};
use tower::{Layer, Service};

pub type TcpAccept = (TcpStream, SocketAddr);

pub trait Accept<S: Split + AsyncReadRent + AsyncWriteRent + 'static> {
    fn stream(self) -> S;
    fn socket_addr(self) -> SocketAddr;
}

impl Accept<TcpStream> for TcpAccept {
    fn stream(self) -> TcpStream {
        self.0
    }

    fn socket_addr(self) -> SocketAddr {
        self.1
    }
}

#[derive(Default, Clone)]
pub struct TcpAcceptService;

impl Service<Rc<TcpListener>> for TcpAcceptService {
    type Response = TcpAccept;
    type Error = anyhow::Error;
    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, listener: Rc<TcpListener>) -> Self::Future {
        async move {
            match listener.accept().await {
                Ok(accept) => {
                    log::info!("accept a connection");
                    return Ok(accept);
                }
                Err(err) => anyhow::bail!("{}", err),
            }
        }
    }
}

#[derive(Default, Clone)]
pub struct TcpAcceptLayer;

impl<S> Layer<S> for TcpAcceptLayer {
    type Service = TcpAcceptService;

    fn layer(&self, _service: S) -> Self::Service {
        TcpAcceptService {}
    }
}

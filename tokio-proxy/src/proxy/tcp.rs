use std::{net::SocketAddr, pin::Pin, task::{Context, Poll}};

use futures::Future;
use tokio::{io::{copy_bidirectional, AsyncWriteExt}, net::TcpStream};
use tower::Service;

use crate::server::tcp::{TcpAccept, TcpTransferReponse};

#[derive(Debug, Clone, Copy)]
pub struct TcpProxy {
    pub target_addr: SocketAddr,
}

impl TcpProxy {
    pub fn new(target_addr: SocketAddr) -> Self {
        TcpProxy { target_addr }
    }
}

impl Service<TcpAccept> for TcpProxy {
    type Error = std::io::Error;
    type Response = TcpTransferReponse;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + Sync>>;

    fn poll_ready(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, accept: TcpAccept) -> Self::Future {
        let addr = self.target_addr.clone();
        Box::pin(async move {
            let mut inbound = accept.stream;
            log::debug!("proxying tcp traffic from {:?} to {:?}", accept.addr, addr);
            let mut outbound = TcpStream::connect(addr).await?;
            let res = copy_bidirectional(&mut inbound, &mut outbound).await?;
            let _ = inbound.shutdown().await; // its probably fine if i dont do that
            let _ = outbound.shutdown().await;
            Ok(TcpTransferReponse::new(res))
        })
    }
}

use std::{convert::Infallible, fmt::Display};

use axum::{extract::Request, response::Response, serve::IncomingStream};
use derive_builder::Builder;
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio_util::sync::CancellationToken;
use tower::Service;

use super::Server;

#[derive(Builder)]
#[builder(pattern = "owned")]
pub struct AxumServer<Address, M> {
    pub router: M,

    #[builder(setter(name = "bind"))]
    pub listen_addr: Address,

    #[builder(default, setter(skip))]
    pub cancel_token: CancellationToken,
}

#[async_trait::async_trait]
impl<Address: ToSocketAddrs + Display + Send + Sync, M, S> Server for AxumServer<Address, M>
where
    M: for<'a> Service<IncomingStream<'a>, Error = Infallible, Response = S> + Send + Clone + 'static,
    for<'a> <M as Service<IncomingStream<'a>>>::Future: Send,
    S: Service<Request, Response = Response, Error = Infallible> + Clone + Send + 'static,
    S::Future: Send,
{
    type Error = anyhow::Error;

    fn cancel_token(&self) -> CancellationToken {
        self.cancel_token.clone()
    }

    async fn serve(&mut self) -> Result<(), Self::Error> {
        let listener = TcpListener::bind(&self.listen_addr).await.unwrap();
        let router = self.router.clone();
        let cancel_token = self.cancel_token.clone();

        log::info!("Axum server started on {}", self.listen_addr);

        axum::serve(listener, router)
            .with_graceful_shutdown(async move {
                cancel_token.cancelled().await
            })
            .await
            .unwrap();

        log::info!("Shutting down Axum server on {}", self.listen_addr);

        Ok(())
    }
}

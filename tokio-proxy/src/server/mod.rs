use tokio_util::sync::CancellationToken;

pub mod tcp;
pub mod util;
pub mod h1;
pub mod axum;

#[async_trait::async_trait]
pub trait Server {
    type Error;

    async fn serve(&mut self) -> Result<(), Self::Error>;

    fn cancel_token(&self) -> CancellationToken;
}


#[derive(Debug, Clone)]
pub enum UpstreamChange<UpstreamId, UpstreamKey, UpstreamService> {
    Add(UpstreamId, UpstreamKey, UpstreamService),
    Remove(UpstreamId, UpstreamKey),
}

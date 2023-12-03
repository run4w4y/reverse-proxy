use monoio::{
    io::{OwnedReadHalf, OwnedWriteHalf, Splitable},
    net::TcpStream,
};
use monoio_http::h1::codec::{decoder::RequestDecoder, encoder::GenericEncoder};
use std::{
    future::Future,
    task::{Context, Poll},
};
use tower::{Layer, Service};

use super::tcp_accept::Accept;

#[derive(Default, Clone)]
pub struct H1CodecService<S> {
    service: S,
}

impl<Sv, Req> Service<Req> for H1CodecService<Sv>
where
    Sv: Service<Req>,
    Sv::Response: Accept<TcpStream>,
    Sv::Future: Future<Output = Result<Sv::Response, Sv::Error>>,
{
    type Response = (
        RequestDecoder<OwnedReadHalf<TcpStream>>,
        GenericEncoder<OwnedWriteHalf<TcpStream>>,
    );
    type Error = Sv::Error;
    type Future = impl Future<Output = Result<Self::Response, Self::Error>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Req) -> Self::Future {
        let stream_fut = self.service.call(req);
        async move {
            let stream = stream_fut.await?.stream();
            let (local_read, local_write) = stream.into_split();
            let local_decoder = RequestDecoder::new(local_read);
            let local_encoder = GenericEncoder::new(local_write);

            Ok((local_decoder, local_encoder))
        }
    }
}

#[derive(Default, Clone)]
pub struct H1CodecLayer {}

impl<S> Layer<S> for H1CodecLayer {
    type Service = H1CodecService<S>;

    fn layer(&self, service: S) -> Self::Service {
        H1CodecService { service }
    }
}

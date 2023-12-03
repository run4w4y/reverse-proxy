use crate::services::h1_codec::H1CodecLayer;
use crate::services::tcp_accept::TcpAcceptService;

use super::Proxy;

use anyhow::bail;
use monoio::io::sink::SinkExt;
use monoio::io::stream::Stream;
use monoio::{
    io::Splitable,
    net::{ListenerConfig, TcpListener, TcpStream},
};
use monoio_http::common::body::StreamHint;
use monoio_http::common::request::Request;
use monoio_http::common::response::Response;
use http::response;
use monoio_http::h1::codec::ClientCodec;
use monoio_http::h1::payload::{Payload, FixedPayload};
use monoio_http::common::body::Body;
use std::rc::Rc;
use std::{future::Future, net::SocketAddr};
use tower::{Service, ServiceBuilder};

pub struct HttpProxy {}

impl HttpProxy {
    pub fn new() -> Self {
        HttpProxy {}
    }
}

impl Proxy for HttpProxy {
    type Error = anyhow::Error;
    type OutputFuture<'a> = impl Future<Output = Result<(), Self::Error>> + 'a where Self: 'a;

    fn io_loop(&self) -> Self::OutputFuture<'_> {
        async {
            let listen_addr = "0.0.0.0:8080";
            let listener = TcpListener::bind_with_config(listen_addr, &ListenerConfig::default());
            if let Err(e) = listener {
                bail!("Error when binding to an address: {}", e);
            }
            let listener = Rc::new(listener.unwrap());
            let (tx, rx) = async_channel::bounded(1);
            let mut svc = ServiceBuilder::new()
                .layer(H1CodecLayer {})
                .service(TcpAcceptService);

            loop {
                let (mut decoder, mut encoder) = svc.call(listener.clone()).await.unwrap();

                match decoder.next().await {
                    Some(Ok(req)) => {
                        let req: Request<Payload> = req;
                        let res = send_request(req).await;
                        encoder.send_and_flush(res).await?;
                    }
                    Some(Err(err)) => {
                        // TODO: fallback to tcp
                        log::warn!("{}", err);
                        break;
                    }
                    None => {
                        // log::info!("http client {} closed", socketaddr);
                        break;
                    }
                }
            }

            rx.close();
            let _ = tx.send(()).await;
            Ok(())
        }
    }
}

async fn send_request(req: Request) -> Response {
    let conn = monoio::net::TcpStream::connect("localhost:8000")
        .await
        .expect("unable to connect");
    let mut codec = ClientCodec::new(conn);

    codec
        .send_and_flush(req)
        .await
        .expect("unable to send request");

    let builder = response::Builder::new();

    let resp = codec
        .next()
        .await
        .expect("disconnected")
        .expect("parse response failed");

    let mut body = resp.into_body().with_io(codec);
    if body.stream_hint() != StreamHint::Fixed {
        panic!("unexpected body type");
    }

    let data = body
        .next_data()
        .await
        .unwrap()
        .expect("unable to read response body");

    builder.body(Payload::Fixed(FixedPayload::new(data))).unwrap()
}

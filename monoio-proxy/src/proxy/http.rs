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
use monoio_http::common::body::{StreamHint, HttpBody};
use monoio_http::common::request::Request;
use monoio_http::common::response::Response;
use http::{response, Uri};
use monoio_http::h1::codec::ClientCodec;
use monoio_http::h1::codec::decoder::RequestDecoder;
use monoio_http::h1::codec::encoder::GenericEncoder;
use monoio_http::h1::payload::{Payload, FixedPayload};
use monoio_http::common::body::Body;
use monoio_http_client::Client;
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
            // let (tx, rx) = async_channel::bounded(1);
            let h1_client = monoio_http_client::Builder::new().http1_client().build();
            
            loop {
                let accept = listener.accept().await;

                match accept {
                    Ok((mut conn, client_addr)) => {
                        let (read, write) = conn.into_split();
                        let mut decoder = RequestDecoder::new(read);
                        let mut encoder = GenericEncoder::new(write);
                        let client = h1_client.clone();
                        
                        monoio::spawn(async move {
                            match decoder.next().await {
                                Some(Ok(req)) => {
                                    let req: Request<Payload> = req;
                                    let res = send_request(client, req).await;
                                    let _ = encoder.send_and_flush(res).await;
                                }
                                Some(Err(err)) => {
                                    // TODO: fallback to tcp
                                    log::warn!("{}", err);
                                }
                                None => {
                                    log::info!("http client {} closed", client_addr);
                                }
                            }
                        });
                    }
                    Err(_) => {
                        log::warn!("failed to accept a connection");
                    }
                }
            }

            // rx.close();
            // let _ = tx.send(()).await;
            Ok(())
        }
    }
}

async fn send_request(client: Client, mut req: Request) -> Response<HttpBody> {
    *req.uri_mut() = Uri::builder()
        .scheme("http")
        .authority("127.0.0.1:8000")
        .path_and_query("")
        .build()
        .unwrap();

    let resp = client.send_request(req).await.unwrap();
    
    resp
    // let (parts, mut body) = resp.into_parts();

    // let mut body = resp.into_body().with_io(codec);
    // if body.stream_hint() != StreamHint::Fixed {
    //     panic!("unexpected body type");
    // }

    // let data = body
    //     .next_data()
    //     .await
    //     .unwrap()
    //     .expect("unable to read response body");

    // builder.body(Payload::Fixed(FixedPayload::new(data))).unwrap()
}

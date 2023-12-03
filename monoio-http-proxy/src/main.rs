use http::{response, request, HeaderMap, StatusCode, Version};
use monoio::{
    io::{
        sink::{Sink, SinkExt},
        stream::Stream,
        Splitable,
    },
    net::{TcpListener, TcpStream},
};
use monoio_http::{
    common::{error::HttpError, request::Request, response::Response, body::{Body, StreamHint}},
    h1::{
        codec::{decoder::{RequestDecoder, ResponseDecoder, PayloadDecoder, FixedBodyDecoder, ChunkedBodyDecoder}, encoder::GenericEncoder, ClientCodec},
        payload::{FixedPayload, Payload, FramedPayload},
    },
    util::spsc::{spsc_pair, SPSCReceiver},
};

#[monoio::main(threads = 24, timer_enabled = true)]
async fn main() {
    let listener = TcpListener::bind("127.0.0.1:8080").unwrap();
    println!("Listening");
    loop {
        let incoming = listener.accept().await;
        match incoming {
            Ok((stream, addr)) => {
                // println!("accepted a connection from {}", addr);
                monoio::spawn(handle_connection(stream));
            }
            Err(e) => {
                // println!("accepted connection failed: {}", e);
            }
        }
    }
}

async fn handle_connection(stream: TcpStream) {
    let (r, w) = stream.into_split();
    let sender = GenericEncoder::new(w);
    let mut receiver = RequestDecoder::new(r);
    let (mut tx, rx) = spsc_pair();
    monoio::spawn(handle_task(rx, sender));

    loop {
        match receiver.next().await {
            None => {
                // println!("connection closed, connection handler exit");
                return;
            }
            Some(Err(_)) => {
                // println!("receive request failed, connection handler exit");
                return;
            }
            Some(Ok(item)) => match tx.send(item).await {
                Err(_) => {
                    // println!("request handler dropped, connection handler exit");
                    return;
                }
                Ok(_) => {
                    // println!("request handled success");
                }
            },
        }
    }
}

async fn handle_task(
    mut receiver: SPSCReceiver<Request>,
    mut sender: impl Sink<Response, Error = impl Into<HttpError>>,
) -> Result<(), HttpError> {
    loop {
        let request = match receiver.recv().await {
            Some(r) => r,
            None => {
                return Ok(());
            }
        };
        let resp = handle_request(request).await;
        match sender.send_and_flush(resp).await.map_err(Into::into) {
            Ok(_) => {
                // println!("task success");
                // Ok(())
            },
            Err(err) => {
                // println!("task failed: {:?}", err);
                Err(err)?;
                // Err(err)
            }
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

async fn handle_request(req: Request) -> Response {
    let mut request_builder = request::Builder::new()
        .version(Version::HTTP_11)
        .uri(req.uri())
        .method(req.method());

    {
        let headers = request_builder.headers_mut().unwrap();
        req.headers().clone_into(headers);
        headers.insert(http::header::HOST, "localhost".parse().unwrap());
    }

    // let mut has_error = false;
    // let mut has_payload = false;
    match req.into_body() {
        Payload::None => {
            let request = request_builder.body(Payload::None).unwrap();
            send_request(request).await
        },
        Payload::Fixed(mut p) => match p.next().await.unwrap() {
            Ok(data) => {
                let request = request_builder.body(Payload::Fixed(FixedPayload::new(data))).unwrap();
                send_request(request).await
            }
            Err(_) => {
                let request = request_builder.body(Payload::None).unwrap();
                send_request(request).await
            }
        },
        Payload::Stream(_) => unimplemented!(),
    }
}

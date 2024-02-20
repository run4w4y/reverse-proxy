mod listeners;

use std::convert::Infallible;
use std::net::SocketAddr;
use std::time::Duration;
use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Client, Request, Response, Server};
use listeners::tcp::{RouteAll, Source, SourceConfigProto, TcpProxy, TcpSource};
use tower::load::{CompleteOnResponse, PendingRequests};

// type HttpClient = Client<hyper::client::HttpConnector>;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
enum UpstreamId {
    DefaultUpstream
}

// TODO: replace async_channel with tokio::sync::watch
#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    // create the tcp listener  
    let listener = TcpSource::new("0.0.0.0:8080");

    let proxy = PendingRequests::new(
        TcpProxy::new("127.0.0.1:8000"), 
        CompleteOnResponse::default()
    );

    let (routing_tx, mut routing_rx) = tokio::sync::watch::channel(RouteAll::new(UpstreamId::DefaultUpstream));
    let (proto_tx, mut proto_rx) = tokio::sync::mpsc::channel(10);
    
    let fut = tokio::spawn(listener.io_loop(proto_rx, routing_rx));

    proto_tx.send(SourceConfigProto::UpstreamAdd(UpstreamId::DefaultUpstream, "key", proxy)).await.unwrap();

    tokio::time::sleep(Duration::from_secs(10)).await;

    let another_proxy = PendingRequests::new(
        TcpProxy::new("127.0.0.1:8888"),
        CompleteOnResponse::default()
    );

    proto_tx.send(SourceConfigProto::UpstreamAdd(UpstreamId::DefaultUpstream, "another-key", another_proxy)).await.unwrap();
    
    fut.await.unwrap().unwrap();
}

// #[tokio::main]
// async fn main() {
//     let bind_addr = SocketAddr::from(([0, 0, 0, 0], 8080));

//     let client = Client::builder()
//         .http1_title_case_headers(true)
//         .http1_preserve_header_case(true)
//         .build_http();

//     let make_service = make_service_fn(move |_| {
//         let client = client.clone();
//         async move { 
//             Ok::<_, Infallible>(service_fn(move |req| proxy(client.clone(), req))) 
//         }
//     });

//     let server = Server::bind(&bind_addr)
//         .http1_preserve_header_case(true)
//         .http1_title_case_headers(true)
//         .serve(make_service);

//     println!("Listening on http://{}", bind_addr);

//     if let Err(e) = server.await {
//         eprintln!("server error: {}", e);
//     }
// }

// async fn proxy(client: HttpClient, req: Request<Body>) -> Result<Response<Body>, hyper::Error> {
//     let target_url = "http://127.0.0.1:8000".to_owned();
//     let path = req.uri().path().to_string();
//     let resp = get_response(client, req, &target_url, &path).await?;
//     Ok(resp)
// }

// async fn get_response(client: HttpClient, req: Request<Body>, target_url: &str, path: &str) -> Result<Response<Body>, hyper::Error> {
//     let target_url = format!("{}{}", target_url, path);
//     let headers = req.headers().clone();
//     let mut request_builder = Request::builder()
//         .method(req.method())
//         .uri(target_url)
//         .body(req.into_body())
//         .unwrap();

//     *request_builder.headers_mut() = headers;
//     let response = client.request(request_builder).await?;
//     let body = hyper::body::to_bytes(response.into_body()).await?;
//     let body = String::from_utf8(body.to_vec()).unwrap();

//     let mut resp = Response::new(Body::from(body));
//     *resp.status_mut() = http::StatusCode::OK;
//     Ok(resp)
// }

mod application;
mod proxy;
mod routing;
mod server;

use std::convert::Infallible;

use application::Application;
use futures::future;
use http::HeaderValue;
// use hyper::service::{make_service_fn, service_fn};
// use hyper::{Body, Client, Request, Response, Server};
use proxy::{http::HttpProxy, tcp::TcpProxy};
use routing::RouteAll;
use server::{h1::HttpServerBuilder, tcp::TcpServerBuilder, UpstreamChange};
// use std::convert::Infallible;
// use std::net::SocketAddr;
// use std::time::Duration;
use tokio::sync::watch;
use tower::{
    load::{CompleteOnResponse, PendingRequests},
    service_fn,
};

// type HttpClient = Client<hyper::client::HttpConnector>;

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
enum UpstreamId {
    DefaultUpstream,
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
    let router_svc = service_fn(|mut req_info: http::request::Parts| {
        if let Some(host) = req_info.headers.get("Host") {
            log::info!("Host header: {:?}", host);
            if *host == "foo.localhost".parse::<HeaderValue>().unwrap() {
                log::info!("foo match");
                req_info.uri = "/foo".parse().unwrap();
            } else if *host == "bar.localhost".parse::<HeaderValue>().unwrap() {
                log::info!("bar match");
                req_info.uri = "/bar".parse().unwrap();
            }
        }

        future::ready(Ok::<_, Infallible>(UpstreamId::DefaultUpstream))
    });
    let (router_tx, router_rx) = watch::channel(router_svc);
    let srv = HttpServerBuilder::default()
        .bind("0.0.0.0:8080")
        .router_channel(router_rx)
        .upstreams_channel(rx)
        .build()
        .unwrap();

    let proxy = PendingRequests::new(
        HttpProxy::new("127.0.0.1:8000".parse().unwrap()),
        CompleteOnResponse::default(),
    );
    tx.send(UpstreamChange::Add(
        UpstreamId::DefaultUpstream,
        "hello-world-1",
        proxy,
    ))
    .unwrap();

    Application::new().server(srv).serve_all().await;
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

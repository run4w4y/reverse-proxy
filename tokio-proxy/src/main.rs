mod application;
mod proxy;
mod routing;
mod server;

use application::Application;
use proxy::{http::HttpProxy, tcp::TcpProxy};
use routing::RouteAll;
use server::{h1::HttpServerBuilder, tcp::TcpServerBuilder, UpstreamChange};
use tokio::sync::watch;
use tower::{
    load::{CompleteOnResponse, PendingRequests},
    service_fn,
};

#[derive(PartialEq, Eq, Hash, Clone, Copy, Debug)]
enum UpstreamId {
    DefaultUpstream,
}

#[tokio::main]
async fn main() {
    pretty_env_logger::init();

    let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

    let router_svc = RouteAll::new(UpstreamId::DefaultUpstream);
    let (router_tx, router_rx) = watch::channel(router_svc);
    let srv = HttpServerBuilder::default()
        .bind("0.0.0.0:8080")
        .router_channel(router_rx)
        .upstreams_channel(rx)
        .build()
        .unwrap();

    let hostnames = vec![
        "hello-world-1".to_string(),
        "hello-world-2".to_string(),
        "hello-world-3".to_string()
    ];

    for hostname in hostnames {
        // let address = resolver.lookup_ip(*hostname).unwrap();
        let target = format!("{hostname}:8000");

        let proxy = PendingRequests::new(
            HttpProxy::new(target),
            CompleteOnResponse::default(),
        );
    
        tx.send(UpstreamChange::Add(
            UpstreamId::DefaultUpstream,
            hostname,
            proxy,
        ))
        .unwrap();
    }

    Application::new().server(srv).serve_all().await;
}

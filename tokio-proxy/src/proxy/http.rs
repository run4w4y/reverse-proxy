use bytes::Bytes;
use futures::Future;
use http::{Request, Response, Uri};
use http_body_util::{combinators::BoxBody, BodyExt};
use hyper::body::Incoming;
use hyper_util::{
    client::legacy::{connect::HttpConnector, Client},
    rt::{TokioExecutor, TokioTimer},
};
use std::{
    pin::Pin,
    task::{Context, Poll},
    time::Duration,
};
use tower::Service;

pub type HttpClient = Client<HttpConnector, Incoming>;

#[derive(Debug, Clone)]
pub struct HttpProxy {
    target_host: String,
    client: HttpClient,
}

impl HttpProxy {
    pub fn new(target_host: String) -> Self {
        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(30))
            .pool_timer(TokioTimer::new())
            .build_http();

        HttpProxy {
            target_host,
            client,
        }
    }

    pub fn with_client(target_host: String, client: HttpClient) -> Self {
        HttpProxy {
            target_host,
            client,
        }
    }
}

impl Service<Request<Incoming>> for HttpProxy {
    type Error = hyper_util::client::legacy::Error;
    type Response = Response<BoxBody<Bytes, hyper::Error>>;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + Sync>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, mut req: Request<Incoming>) -> Self::Future {
        let host = self.target_host.clone();
        let client = self.client.clone();

        Box::pin(async move {
            let uri = Uri::builder()
                .scheme("http")
                .authority(host)
                .path_and_query(
                    req.uri()
                        .path_and_query()
                        .map(|x| x.as_str())
                        .unwrap_or("/"),
                )
                .build()
                .unwrap();
            *req.uri_mut() = uri;

            let resp = client.request(req).await?;
            Ok(resp.map(|b| b.boxed()))
        })
    }
}

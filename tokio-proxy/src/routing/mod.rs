use std::{convert::Infallible, pin::Pin, task::{Context, Poll}};

use futures::{future, Future};
use tower::Service;

#[derive(Debug, Clone)]
pub struct RouteAll<T> {
    pub upstream: T,
}

impl<T> RouteAll<T> {
    pub fn new(upstream: T) -> Self {
        RouteAll { upstream }
    }

    pub fn into_make_service(self) -> IntoMakeRouteAll<T> {
        IntoMakeRouteAll { inner: self }
    }
}

#[derive(Debug, Clone)]
pub struct IntoMakeRouteAll<T> {
    inner: RouteAll<T>,
}

impl<UpstreamId: Default> Default for IntoMakeRouteAll<UpstreamId> {
    fn default() -> Self {
        IntoMakeRouteAll {
            inner: RouteAll::new(UpstreamId::default()),
        }
    }
}

impl<T: Copy + Send + Sync + 'static> Service<()> for IntoMakeRouteAll<T> {
    type Response = RouteAll<T>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + Sync>>;

    fn poll_ready(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _target: ()) -> Self::Future {
        Box::pin(future::ready(Ok(RouteAll {
            upstream: self.inner.upstream,
        })))
    }
}

impl<Req, T: Copy + Send + Sync + 'static> Service<Req> for RouteAll<T> {
    type Response = T;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send + Sync>>;

    fn poll_ready(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _: Req) -> Self::Future {
        Box::pin(future::ready(Ok(self.upstream)))
    }
}
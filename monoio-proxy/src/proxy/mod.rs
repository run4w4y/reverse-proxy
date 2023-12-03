pub mod tcp;
pub mod http;

use std::future::Future;

pub trait Proxy {
    type Error;
    type OutputFuture<'a>: Future<Output = Result<(), Self::Error>>
    where
        Self: 'a;

    fn io_loop(&self) -> Self::OutputFuture<'_>;
}

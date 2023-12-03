#![feature(impl_trait_in_assoc_type)]
#![feature(trait_alias)]

mod proxy;
mod util;
mod services;

use std::{thread, num::NonZeroUsize};

use monoio::RuntimeBuilder;
use proxy::{tcp::TcpProxy, Proxy, http::HttpProxy};

#[monoio::main(enable_timer = true)]
async fn main() {
    pretty_env_logger::init();

    let max_thread_count = thread::available_parallelism().unwrap_or(NonZeroUsize::new(1).unwrap());
    let mut handlers = vec![];

    for _ in 0..max_thread_count.get() {
        let handler = thread::spawn(move || {
            let mut rt = RuntimeBuilder::<monoio::IoUringDriver>::new()
                .enable_timer()
                .with_entries(32768)
                .build()
                .unwrap();

            let _ = rt.block_on(async move {
                let proxy = HttpProxy::new();
                proxy.io_loop().await?;
                Ok::<(), anyhow::Error>(())
            });
        });
        handlers.push(handler);
    }

    for handler in handlers.into_iter() {
        let _ = handler.join();
    }
}



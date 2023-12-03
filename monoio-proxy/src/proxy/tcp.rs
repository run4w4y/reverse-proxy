use crate::util::copy_data;

use super::Proxy;

use std::future::Future;
use monoio::{
    io::Splitable,
    net::{ListenerConfig, TcpListener, TcpStream},
};

pub struct TcpProxy {
    listener_config: ListenerConfig,
}

impl Proxy for TcpProxy {
    type Error = anyhow::Error;
    type OutputFuture<'a> = impl Future<Output = Result<(), Self::Error>> + 'a where Self: 'a;

    fn io_loop(&self) -> Self::OutputFuture<'_> {
        async {
            let bind_addr = "0.0.0.0:8080";
            let peer_addr = "127.0.0.1:8000";
            let listener = TcpListener::bind_with_config(bind_addr, &self.listener_config)
                .expect(&format!("cannot bind a tcp listener on address: {}", bind_addr));

            loop {
                let accept = listener.accept().await;

                match accept {
                    Ok((mut conn, _)) => {
                        monoio::spawn(async move {
                            let remote_conn = TcpStream::connect(peer_addr).await;

                            match remote_conn {
                                Ok(mut remote) => {
                                    let (mut local_read, mut local_write) = conn.into_split();
                                    let (mut remote_read, mut remote_write) = remote.into_split();

                                    let _ = monoio::join!(
                                        copy_data(&mut local_read, &mut remote_write),
                                        copy_data(&mut remote_read, &mut local_write)
                                    );
                                }

                                Err(_) => {
                                    eprintln!("unable to connect addr: {}", peer_addr);
                                }
                            }
                        });
                    }
                    Err(_) => {
                        eprintln!("failed to accept connections.");
                    }
                }
            }
        }
    }
}

impl TcpProxy {
    pub fn new() -> Self {
        TcpProxy { listener_config: ListenerConfig::default() }
    }
}

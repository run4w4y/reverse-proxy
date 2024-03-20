use futures::{stream::FuturesUnordered, StreamExt};
use tokio::signal::unix::{signal, SignalKind};

use crate::server::Server;

pub struct Application {
    servers: Vec<Box<dyn Server<Error = anyhow::Error>>>,
}

impl Default for Application {
    fn default() -> Self {
        Application { servers: vec![] }
    }
}

impl Application {
    pub fn new() -> Self {
        Application::default()
    }

    pub fn server(
        mut self,
        srv: impl Server<Error = anyhow::Error> + 'static,
    ) -> Application {
        self.servers.push(Box::new(srv));
        self
    }

    pub async fn serve_all(mut self) {
        let cancel_tokens = self
            .servers
            .iter()
            .map(|srv| srv.cancel_token())
            .collect::<Vec<_>>();

        let handle = tokio::spawn(async move {
            let mut sigterm = signal(SignalKind::terminate()).unwrap();
            let mut sigint = signal(SignalKind::interrupt()).unwrap();
            let cancel_all = || cancel_tokens.iter().for_each(|token| token.cancel());

            tokio::select! {
                _ = sigterm.recv() => {
                    cancel_all();
                },
                _ = sigint.recv() => {
                    cancel_all();
                },
            }
        });

        let server_results = self
            .servers
            .iter_mut()
            .map(|srv| srv.serve())
            .collect::<FuturesUnordered<_>>()
            .collect::<Vec<_>>()
            .await;

        let _ = handle.await;
    }
}

use std::{convert::Infallible, error::Error, hash::Hash, pin::Pin, sync::Arc, fmt::Debug};

use tokio::sync::{mpsc, watch, Mutex, RwLock};
use tokio_stream::wrappers::UnboundedReceiverStream;
use tower::{balance::p2c, discover::Change, load::Load, BoxError, Service};

use super::UpstreamChange;

pub type UpstreamStore<Req, Id, Key, Proxy> = scc::HashMap<
    Id,
    (
        mpsc::UnboundedSender<Result<Change<Key, Proxy>, Infallible>>,
        p2c::Balance<
            Pin<Box<UnboundedReceiverStream<Result<Change<Key, Proxy>, Infallible>>>>,
            Req,
        >,
    ),
>;

pub async fn poll_upstream_changes<
    Req,
    UpstreamId: Eq + Hash + Debug,
    UpstreamKey: Eq + Hash + Debug,
    Proxy: Service<Req> + Load,
>(
    upstream_rx: &mut mpsc::UnboundedReceiver<UpstreamChange<UpstreamId, UpstreamKey, Proxy>>,
    upstreams: Arc<UpstreamStore<Req, UpstreamId, UpstreamKey, Proxy>>,
) -> ()
where
    Proxy::Error: Send + Sync + Error + Into<BoxError> + Debug,
{
    while let Some(msg) = upstream_rx.recv().await {
        match msg {
            UpstreamChange::Add(id, key, svc) => {
                // NOTE: be careful to not deadlock here
                match upstreams.get_async(&id).await {
                    Some(entry) => {
                        let (tx, _) = entry.get();
                        log::debug!("received a new upstream id={:?} key={:?}", id, key);
                        tx.send(Ok(Change::Insert(key, svc))).unwrap();
                    }
                    None => {
                        log::debug!("creating a new load balanced service for the upstream id={:?} key={:?}", id, key);
                        let (tx, rx) = mpsc::unbounded_channel::<
                            Result<Change<UpstreamKey, Proxy>, Infallible>,
                        >();
                        tx.send(Ok(Change::Insert(key, svc))).unwrap();

                        let balanced_svc =
                            p2c::Balance::new(Box::pin(UnboundedReceiverStream::new(rx)));

                        let _ = upstreams.insert_async(id, (tx, balanced_svc)).await;
                    }
                }
            }
            UpstreamChange::Remove(id, key) => match upstreams.get_async(&id).await {
                Some(entry) => {
                    let (tx, _) = entry.get();
                    tx.send(Ok(Change::Remove(key))).unwrap();
                }
                None => {
                    log::warn!("No upstream found with the id: {:?}", id);
                }
            },
        }
    }
}

pub async fn poll_router_changes<MakeRouter: Clone>(
    router_rx: &mut watch::Receiver<MakeRouter>,
    make_router: Arc<Mutex<MakeRouter>>
) -> () {
    while let Ok(()) = router_rx.changed().await {
        let mut lock = make_router.lock().await;
        *lock = router_rx.borrow().clone();
    }
}

pub async fn poll_router_changes_rw_lock<MakeRouter: Clone>(
    router_rx: &mut watch::Receiver<MakeRouter>,
    make_router: Arc<RwLock<MakeRouter>>
) -> () {
    while let Ok(()) = router_rx.changed().await {
        let mut lock = make_router.write().await;
        *lock = router_rx.borrow().clone();
    }
}

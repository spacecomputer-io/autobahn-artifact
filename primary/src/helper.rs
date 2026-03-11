// Copyright(C) Facebook, Inc. and its affiliates.
use crate::primary::PrimaryMessage;
use bytes::Bytes;
use config::Committee;
use crypto::{Digest, PublicKey};
use log::{error, warn};
use network::SimpleSender;
use store::Store;
use tokio::sync::mpsc::Receiver;
use tokio::sync::Semaphore;
use std::sync::Arc;

/// Maximum number of concurrent sync response tasks.
/// Allows the helper to serve multiple digest lookups in parallel
/// instead of processing them sequentially.
const MAX_CONCURRENT_RESPONSES: usize = 8;

/// A task dedicated to help other authorities by replying to their certificates requests.
pub struct Helper {
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// Input channel to receive certificates requests.
    rx_primaries_certs: Receiver<(Vec<Digest>, PublicKey)>,

    /// Input channel to receive certificates requests.
    rx_primaries_headers: Receiver<(Vec<Digest>, PublicKey)>,
    /// A network sender to reply to the sync requests.
    network: SimpleSender,
    /// Semaphore to bound concurrent response tasks.
    semaphore: Arc<Semaphore>,
}

impl Helper {
    pub fn spawn(
        committee: Committee,
        store: Store,
        rx_primaries_certs: Receiver<(Vec<Digest>, PublicKey)>,
        rx_primaries_headers: Receiver<(Vec<Digest>, PublicKey)>,
    ) {
        tokio::spawn(async move {
            Self {
                committee,
                store,
                rx_primaries_certs,
                rx_primaries_headers,
                network: SimpleSender::new(),
                semaphore: Arc::new(Semaphore::new(MAX_CONCURRENT_RESPONSES)),
            }
            .run()
            .await;
        });
    }

    /// Serve a batch of certificate digest requests concurrently.
    async fn serve_cert_requests(
        store: Store,
        network: &mut SimpleSender,
        semaphore: Arc<Semaphore>,
        digests: Vec<Digest>,
        address: std::net::SocketAddr,
    ) {
        for digest in digests {
            let permit = semaphore.clone().acquire_owned().await
                .expect("Semaphore closed unexpectedly");
            let mut store = store.clone();
            let address = address;
            // Collect the response bytes inside the spawned task, then send
            // from the main task to avoid needing a shared network sender.
            let handle = tokio::spawn(async move {
                let result = store.read(digest.to_vec()).await;
                drop(permit);
                result
            });
            match handle.await {
                Ok(Ok(Some(data))) => {
                    // Send raw storage bytes wrapped in a Certificate message.
                    // The storage already contains the serialized Certificate,
                    // so we deserialize + reserialize to wrap it in PrimaryMessage.
                    // TODO: Store pre-wrapped messages to eliminate this overhead.
                    match bincode::deserialize(&data) {
                        Ok(certificate) => {
                            let bytes = bincode::serialize(&PrimaryMessage::Certificate(certificate))
                                .expect("Failed to serialize our own certificate");
                            network.send(address, Bytes::from(bytes)).await;
                        }
                        Err(e) => error!("Failed to deserialize certificate: {}", e),
                    }
                }
                Ok(Ok(None)) => (),
                Ok(Err(e)) => error!("{}", e),
                Err(e) => error!("Spawn join error: {}", e),
            }
        }
    }

    /// Serve a batch of header digest requests concurrently.
    async fn serve_header_requests(
        store: Store,
        network: &mut SimpleSender,
        semaphore: Arc<Semaphore>,
        digests: Vec<Digest>,
        address: std::net::SocketAddr,
    ) {
        for digest in digests {
            let permit = semaphore.clone().acquire_owned().await
                .expect("Semaphore closed unexpectedly");
            let mut store = store.clone();
            let handle = tokio::spawn(async move {
                let result = store.read(digest.to_vec()).await;
                drop(permit);
                result
            });
            match handle.await {
                Ok(Ok(Some(data))) => {
                    match bincode::deserialize(&data) {
                        Ok(header) => {
                            let bytes = bincode::serialize(&PrimaryMessage::Header(header, true))
                                .expect("Failed to serialize our own header");
                            network.send(address, Bytes::from(bytes)).await;
                        }
                        Err(e) => error!("Failed to deserialize header: {}", e),
                    }
                }
                Ok(Ok(None)) => (),
                Ok(Err(e)) => error!("{}", e),
                Err(e) => error!("Spawn join error: {}", e),
            }
        }
    }

    async fn run(&mut self) {
        loop{
            tokio::select! {
                Some((digests, origin)) = self.rx_primaries_certs.recv() => {
                    // TODO [issue #195]: Do some accounting to prevent bad nodes from monopolizing our resources.

                    // get the requestors address.
                    let address = match self.committee.primary(&origin) {
                        Ok(x) => x.primary_to_primary,
                        Err(e) => {
                            warn!("Unexpected certificate request: {}", e);
                            continue;
                        }
                    };

                    // Reply to the request with bounded parallelism.
                    Self::serve_cert_requests(
                        self.store.clone(),
                        &mut self.network,
                        self.semaphore.clone(),
                        digests,
                        address,
                    ).await;
                },
                Some((digests, origin)) = self.rx_primaries_headers.recv() => {
                    // TODO [issue #195]: Do some accounting to prevent bad nodes from monopolizing our resources.

                    // get the requestors address.
                    let address = match self.committee.primary(&origin) {
                        Ok(x) => x.primary_to_primary,
                        Err(e) => {
                            warn!("Unexpected certificate request: {}", e);
                            continue;
                        }
                    };

                    // Reply to the request with bounded parallelism.
                    Self::serve_header_requests(
                        self.store.clone(),
                        &mut self.network,
                        self.semaphore.clone(),
                        digests,
                        address,
                    ).await;
                },
            };
        }

    }
}

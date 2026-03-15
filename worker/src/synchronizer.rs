// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::{Round, WorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use network::SimpleSender;
use primary::PrimaryWorkerMessage;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};
use store::{Store, StoreError};
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/synchronizer_tests.rs"]
pub mod synchronizer_tests;

/// Resolution of the timer managing retrials of sync requests (in ms).
const TIMER_RESOLUTION: u64 = 1_000;
/// Minimum initial fanout for commit-critical batch recovery.
const INITIAL_COMMITTED_SYNC_FANOUT: usize = 3;

#[derive(Clone, Copy, Eq, PartialEq)]
enum SyncPriority {
    Background,
    CommitCritical,
}

struct PendingBatchSync {
    round: Round,
    cancel: Sender<()>,
    timestamp: u128,
    target: PublicKey,
    priority: SyncPriority,
}

// The `Synchronizer` is responsible to keep the worker in sync with the others.
pub struct Synchronizer {
    /// The public key of this authority.
    name: PublicKey,
    /// The id of this worker.
    id: WorkerId,
    /// The committee information.
    committee: Committee,
    // The persistent storage.
    store: Store,
    /// The depth of the garbage collection.
    gc_depth: Round,
    /// The delay to wait before re-trying to send sync requests.
    sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-requests. These nodes
    /// are picked at random from the committee.
    sync_retry_nodes: usize,
    /// Input channel to receive the commands from the primary.
    rx_message: Receiver<PrimaryWorkerMessage>,
    /// A network sender to send requests to the other workers.
    network: SimpleSender,
    /// Loosely keep track of the primary's round number (only used for cleanup).
    round: Round,
    /// Keeps the digests (of batches) that are waiting to be processed by the primary. Their
    /// processing will resume when we get the missing batches in the store or we no longer need them.
    /// It also keeps the round number and a timestamp (`u128`) of each request we sent.
    pending: HashMap<Digest, PendingBatchSync>,
}

impl Synchronizer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        id: WorkerId,
        committee: Committee,
        store: Store,
        gc_depth: Round,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        rx_message: Receiver<PrimaryWorkerMessage>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                id,
                committee,
                store,
                gc_depth,
                sync_retry_delay,
                sync_retry_nodes,
                rx_message,
                network: SimpleSender::new(),
                round: Round::default(),
                pending: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for a batch to become available in the storage
    /// and then delivers its digest.
    async fn waiter(
        missing: Digest,
        mut store: Store,
        deliver: Digest,
        mut handler: Receiver<()>,
    ) -> Result<Option<Digest>, StoreError> {
        tokio::select! {
            result = store.notify_read(missing.to_vec()) => {
                result.map(|_| Some(deliver))
            }
            _ = handler.recv() => Ok(None),
        }
    }

    async fn send_sync_request(
        &mut self,
        digests: Vec<Digest>,
        target: PublicKey,
        priority: SyncPriority,
        retry: bool,
    ) {
        if digests.is_empty() {
            return;
        }

        let message = WorkerMessage::BatchRequest(digests, self.name);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own message");

        match (priority, retry) {
            (SyncPriority::Background, false) => {
                let address = match self.committee.worker(&target, &self.id) {
                    Ok(address) => address.worker_to_worker,
                    Err(e) => {
                        error!("The primary asked us to sync with an unknown node: {}", e);
                        return;
                    }
                };
                self.network.send(address, Bytes::from(serialized)).await;
            }
            (SyncPriority::Background, true) => {
                let addresses: Vec<_> = self
                    .committee
                    .others_workers(&self.name, &self.id)
                    .iter()
                    .map(|(_, address)| address.worker_to_worker)
                    .collect();
                self.network
                    .lucky_broadcast(addresses, Bytes::from(serialized), self.sync_retry_nodes)
                    .await;
            }
            (SyncPriority::CommitCritical, false) => {
                let target_address = match self.committee.worker(&target, &self.id) {
                    Ok(address) => address.worker_to_worker,
                    Err(e) => {
                        error!("The primary asked us to sync with an unknown node: {}", e);
                        return;
                    }
                };
                self.network
                    .send(target_address, Bytes::from(serialized.clone()))
                    .await;

                let other_addresses: Vec<_> = self
                    .committee
                    .others_workers(&self.name, &self.id)
                    .iter()
                    .filter(|(name, _)| *name != target)
                    .map(|(_, address)| address.worker_to_worker)
                    .collect();

                if !other_addresses.is_empty() {
                    let fanout = other_addresses
                        .len()
                        .min(self.sync_retry_nodes.max(INITIAL_COMMITTED_SYNC_FANOUT));
                    self.network
                        .lucky_broadcast(other_addresses, Bytes::from(serialized), fanout)
                        .await;
                }
            }
            (SyncPriority::CommitCritical, true) => {
                let addresses: Vec<_> = self
                    .committee
                    .others_workers(&self.name, &self.id)
                    .iter()
                    .map(|(_, address)| address.worker_to_worker)
                    .collect();

                if !addresses.is_empty() {
                    self.network
                        .lucky_broadcast(addresses.clone(), Bytes::from(serialized), addresses.len())
                        .await;
                }
            }
        }
    }

    /// Main loop listening to the primary's messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Handle primary's messages.
                Some(message) = self.rx_message.recv() => match message {
                    PrimaryWorkerMessage::Synchronize(digests, target) => {
                        let priority = SyncPriority::Background;
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();

                        let mut missing = Vec::new();
                        for digest in digests {
                            if let Some(existing) = self.pending.get_mut(&digest) {
                                if priority == SyncPriority::CommitCritical
                                    && existing.priority != SyncPriority::CommitCritical
                                {
                                    existing.priority = SyncPriority::CommitCritical;
                                    existing.target = target;
                                    existing.timestamp = now;
                                    missing.push(digest.clone());
                                }
                                continue;
                            }

                            match self.store.read(digest.to_vec()).await {
                                Ok(None) => {
                                    missing.push(digest.clone());
                                    debug!("Requesting sync for batch {}", digest);
                                },
                                Ok(Some(_)) => {}
                                Err(e) => {
                                    error!("{}", e);
                                    continue;
                                }
                            }

                            let deliver = digest.clone();
                            let (tx_cancel, rx_cancel) = channel(1);
                            let fut = Self::waiter(digest.clone(), self.store.clone(), deliver, rx_cancel);
                            waiting.push(fut);
                            self.pending.insert(
                                digest,
                                PendingBatchSync {
                                    round: self.round,
                                    cancel: tx_cancel,
                                    timestamp: now,
                                    target,
                                    priority,
                                },
                            );
                        }

                        self.send_sync_request(missing, target, priority, false).await;
                    }
                    PrimaryWorkerMessage::SynchronizeCommitted(digests, target) => {
                        let priority = SyncPriority::CommitCritical;
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();

                        let mut missing = Vec::new();
                        for digest in digests {
                            // Ensure we do not send twice the same sync request, but
                            // allow commit-critical recovery to upgrade an existing wait.
                            if let Some(existing) = self.pending.get_mut(&digest) {
                                if priority == SyncPriority::CommitCritical
                                    && existing.priority != SyncPriority::CommitCritical
                                {
                                    existing.priority = SyncPriority::CommitCritical;
                                    existing.target = target;
                                    existing.timestamp = now;
                                    missing.push(digest.clone());
                                }
                                continue;
                            }

                            // Check if we received the batch in the meantime.
                            match self.store.read(digest.to_vec()).await {
                                Ok(None) => {
                                    missing.push(digest.clone());
                                    debug!("Requesting sync for batch {}", digest);
                                },
                                Ok(Some(_)) => {
                                    // The batch arrived in the meantime: no need to request it.
                                },
                                Err(e) => {
                                    error!("{}", e);
                                    continue;
                                }
                            }

                            // Add the digest to the waiter.
                            let deliver = digest.clone();
                            let (tx_cancel, rx_cancel) = channel(1);
                            let fut = Self::waiter(digest.clone(), self.store.clone(), deliver, rx_cancel);
                            waiting.push(fut);
                            self.pending.insert(
                                digest,
                                PendingBatchSync {
                                    round: self.round,
                                    cancel: tx_cancel,
                                    timestamp: now,
                                    target,
                                    priority,
                                },
                            );
                        }

                        self.send_sync_request(missing, target, priority, false).await;
                    }
                    PrimaryWorkerMessage::Cleanup(round) => {
                        // Keep track of the primary's round number.
                        self.round = round;

                        // Cleanup internal state.
                        if self.round < self.gc_depth {
                            continue;
                        }

                        let mut gc_round = self.round - self.gc_depth;
                        for request in self.pending.values() {
                            if request.round <= gc_round {
                                let _ = request.cancel.send(()).await;
                            }
                        }
                        self.pending.retain(|_, request| request.round > gc_round);
                    }
                },

                // Stream out the futures of the `FuturesUnordered` that completed.
                Some(result) = waiting.next() => match result {
                    Ok(Some(digest)) => {
                        // We got the batch, remove it from the pending list.
                        self.pending.remove(&digest);
                    },
                    Ok(None) => {
                        // The sync request for this batch has been canceled.
                    },
                    Err(e) => error!("{}", e)
                },

                // Triggers on timer's expiration.
                () = &mut timer => {
                    // We optimistically sent sync requests to a single node. If this timer triggers,
                    // it means we were wrong to trust it. We are done waiting for a reply and we now
                    // broadcast the request to a bunch of other nodes (selected at random).
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    let mut retry_background = Vec::new();
                    let mut retry_committed = Vec::new();
                    for (digest, request) in &mut self.pending {
                        if request.timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting sync for batch {} (retry)", digest);
                            request.timestamp = now;
                            if request.priority == SyncPriority::CommitCritical {
                                retry_committed.push((digest.clone(), request.target));
                            } else {
                                retry_background.push((digest.clone(), request.target));
                            }
                        }
                    }

                    let retry_background_digests: Vec<_> =
                        retry_background.into_iter().map(|(digest, _)| digest).collect();
                    if !retry_background_digests.is_empty() {
                        let target = self
                            .pending
                            .get(&retry_background_digests[0])
                            .map(|request| request.target)
                            .unwrap_or(self.name);
                        self.send_sync_request(
                            retry_background_digests,
                            target,
                            SyncPriority::Background,
                            true,
                        )
                        .await;
                    }

                    let retry_committed_digests: Vec<_> =
                        retry_committed.into_iter().map(|(digest, _)| digest).collect();
                    if !retry_committed_digests.is_empty() {
                        let target = self
                            .pending
                            .get(&retry_committed_digests[0])
                            .map(|request| request.target)
                            .unwrap_or(self.name);
                        self.send_sync_request(
                            retry_committed_digests,
                            target,
                            SyncPriority::CommitCritical,
                            true,
                        )
                        .await;
                    }

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },
            }
        }
    }
}

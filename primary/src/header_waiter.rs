#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::{DagError, DagResult};
use crate::messages::{ConsensusMessage, Header, Proposal, proposal_digest};
use crate::metrics::{
    DISSEMINATION_HEADER_SYNC_REQUESTS_RECEIVED_TOTAL, DISSEMINATION_HOLE_DEPENDENTS,
    DISSEMINATION_INFLIGHT_HOLES, DISSEMINATION_RECOVERED_HEADERS_TOTAL,
    DISSEMINATION_SYNC_RETRIES_TOTAL,
};
use crate::primary::{Height, Slot, PrimaryMessage, PrimaryWorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, Hash, PublicKey};
use futures::future::try_join_all;
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, warn, error};
use network::SimpleSender;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use store::Store;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

/// The resolution of the timer that checks whether we received replies to our sync requests, and triggers
/// new sync requests if we didn't.
const TIMER_RESOLUTION: u64 = 1_000;

/// Number of nodes to target for initial sync fan-out (E2).
/// Higher values get missing data faster at the cost of more network traffic.
const INITIAL_SYNC_FANOUT: usize = 3;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PayloadSyncMode {
    Live(bool),
    Historical,
}

/// The commands that can be sent to the `Waiter`.
#[derive(Debug)]
pub enum WaiterMessage {
    SyncBatches(HashMap<Digest, WorkerId>, Header, PayloadSyncMode),
    SyncProposals(Vec<Proposal>, ConsensusMessage, Header),
    SyncParent(Digest, Header),
    SyncHeader(Digest),
    SyncCommittedProposal(Proposal, Height, Slot),
    ClearCommittedProposalSync(Digest),
}

/// Waits for missing parent certificates and batches' digests.
pub struct HeaderWaiter {
    /// The name of this authority.
    name: PublicKey,
    /// The committee information.
    committee: Committee,
    /// The persistent storage.
    store: Store,
    /// The current consensus round (used for cleanup).
    consensus_round: Arc<AtomicU64>,
    /// The depth of the garbage collector.
    gc_depth: Height,
    /// The delay to wait before re-trying sync requests.
    sync_retry_delay: u64,
    /// Determine with how many nodes to sync when re-trying to send sync-request.
    sync_retry_nodes: usize,

    /// Receives sync commands from the `Synchronizer`.
    rx_synchronizer: Receiver<WaiterMessage>,
    /// Loops back to the core headers for which we got all parents and batches.
    tx_core: Sender<Header>,
    /// Loops back commit messages to the committer for reprocessing
    tx_consensus_loopback: Sender<(ConsensusMessage, Header)>,

    /// Network driver allowing to send messages.
    network: SimpleSender,

    /// Keeps the digests of the all certificates for which we sent a sync request,
    /// along with a timestamp (`u128`) indicating when we sent the request.
    parent_requests: HashMap<Digest, (Height, u128)>,
    //same, but for special parents
    header_requests: HashMap<Digest, (Height, u128)>,
    /// Keeps the digests of the all tx batches for which we sent a sync request,
    /// similarly to `header_requests`.
    batch_requests: HashMap<Digest, (Height, bool)>,
    /// List of digests (either certificates, headers or tx batch) that are waiting
    /// to be processed. Their processing will resume when we get all their dependencies.
    pending: HashMap<Digest, (Height, Sender<()>)>,
    /// Headers whose payload waiters should not loop back through the live core path
    /// when the missing batches arrive. These are historical/commit-recovery headers
    /// that are already in the store and only need their payload to become available.
    historical_payload_waiters: HashSet<Digest>,
    /// [E1] Tracks proposal header digests that are already being fetched,
    /// to avoid sending redundant sync requests when multiple consensus messages
    /// (e.g., Prepare for the same slot across different views) reference the same proposal.
    inflight_proposals: HashSet<Digest>,
    /// [F] Pending proposal sync requests ordered by slot, so lower slots get
    /// processed first. This is a simple Vec that we sort before draining.
    pending_proposal_syncs: Vec<PendingProposalSync>,
    /// Newly arrived commit-time suffix sync requests ordered by slot.
    pending_commit_syncs: Vec<PendingCommitSync>,
    /// Commit-time suffix sync requests keyed by proposal digest.
    proposal_sync_requests: HashMap<Digest, PendingSuffixSync>,
}

/// [F] Tracks a deferred proposal sync request with its consensus slot for priority ordering.
struct PendingProposalSync {
    slot: Slot,
    missing: Vec<Proposal>,
    consensus_message: ConsensusMessage,
    header: Header,
}

struct PendingCommitSync {
    slot: Slot,
    digest: Digest,
}

#[derive(Clone)]
struct PendingSuffixSync {
    proposal: Proposal,
    stop_height: Height,
    slot: Slot,
    timestamp: u128,
}

impl HeaderWaiter {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        store: Store,
        consensus_round: Arc<AtomicU64>,
        gc_depth: Height,
        sync_retry_delay: u64,
        sync_retry_nodes: usize,
        rx_synchronizer: Receiver<WaiterMessage>,
        tx_core: Sender<Header>,
        tx_consensus_loopback: Sender<(ConsensusMessage, Header)>,
    ) {
        tokio::spawn(async move {
            Self {
                name,
                committee,
                store,
                consensus_round,
                gc_depth,
                sync_retry_delay,
                sync_retry_nodes,
                rx_synchronizer,
                tx_core,
                tx_consensus_loopback,
                network: SimpleSender::new(),
                parent_requests: HashMap::new(),
                header_requests: HashMap::new(),
                batch_requests: HashMap::new(),
                pending: HashMap::new(),
                historical_payload_waiters: HashSet::new(),
                inflight_proposals: HashSet::new(),
                pending_proposal_syncs: Vec::new(),
                pending_commit_syncs: Vec::new(),
                proposal_sync_requests: HashMap::new(),
            }
            .run()
            .await;
        });
    }

    /// Helper function. It waits for particular data to become available in the storage
    /// and then delivers the specified header.
    async fn waiter(
        mut missing: Vec<(Vec<u8>, Store)>,
        deliver: Header,
        mut handler: Receiver<()>,
    ) -> DagResult<Option<Header>> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some(deliver)).map_err(DagError::from)
            }
            _ = handler.recv() => Ok(None),
        }
    }


    async fn proposal_waiter(
        mut missing: Vec<(Vec<u8>, Store)>,
        deliver: (ConsensusMessage, Header),
        mut handler: Receiver<()>,
    ) -> DagResult<Option<(ConsensusMessage, Header)>> {
        let waiting: Vec<_> = missing
            .iter_mut()
            .map(|(x, y)| y.notify_read(x.to_vec()))
            .collect();
        tokio::select! {
            result = try_join_all(waiting) => {
                result.map(|_| Some(deliver)).map_err(DagError::from)
            }
            _ = handler.recv() => Ok(None),
        }
    }

    /// Extract the consensus slot from a ConsensusMessage.
    fn consensus_slot(msg: &ConsensusMessage) -> Slot {
        match msg {
            ConsensusMessage::Prepare { slot, .. } => *slot,
            ConsensusMessage::Confirm { slot, .. } => *slot,
            ConsensusMessage::Commit { slot, .. } => *slot,
        }
    }

    /// [F] Process pending proposal syncs in slot-priority order (lowest slot first).
    /// This ensures that sync bandwidth is focused on unblocking the oldest stalled
    /// consensus slots, which is critical during partition recovery.
    async fn drain_pending_proposal_syncs(&mut self) {
        if self.pending_proposal_syncs.is_empty() {
            return;
        }

        // Sort by slot ascending — lowest (most critical) slots first
        self.pending_proposal_syncs.sort_by_key(|p| p.slot);

        // Drain all pending syncs
        let syncs: Vec<_> = self.pending_proposal_syncs.drain(..).collect();
        for sync in syncs {
            self.execute_proposal_sync(sync.missing, sync.consensus_message, sync.header).await;
        }
    }

    fn update_recovery_metrics(&self) {
        let mut inflight_holes = HashSet::new();
        inflight_holes.extend(self.parent_requests.keys().cloned());
        inflight_holes.extend(self.header_requests.keys().cloned());
        inflight_holes.extend(self.batch_requests.keys().cloned());
        inflight_holes.extend(self.proposal_sync_requests.keys().cloned());

        DISSEMINATION_INFLIGHT_HOLES.set(inflight_holes.len() as i64);
        DISSEMINATION_HOLE_DEPENDENTS
            .set((self.pending.len() + self.pending_proposal_syncs.len() + self.pending_commit_syncs.len()) as i64);
    }

    fn certifier_addresses(&self, proposal: &Proposal) -> Vec<std::net::SocketAddr> {
        proposal
            .certificate
            .votes
            .iter()
            .map(|(pk, _)| *pk)
            .filter(|pk| *pk != self.name)
            .filter_map(|pk| self.committee.primary(&pk).ok())
            .map(|addresses| addresses.primary_to_primary)
            .collect()
    }

    fn other_primary_addresses(&self) -> Vec<std::net::SocketAddr> {
        self.committee
            .others_primaries(&self.name)
            .iter()
            .map(|(_, addresses)| addresses.primary_to_primary)
            .collect()
    }

    async fn dispatch_pending_commit_syncs(&mut self) {
        if self.pending_commit_syncs.is_empty() {
            return;
        }

        self.pending_commit_syncs.sort_by_key(|pending| pending.slot);
        let syncs: Vec<_> = self.pending_commit_syncs.drain(..).collect();
        let mut seen = HashSet::new();

        for pending in syncs {
            if !seen.insert(pending.digest.clone()) {
                continue;
            }

            let Some(request) = self.proposal_sync_requests.get(&pending.digest).cloned() else {
                continue;
            };

            let certifiers = self.certifier_addresses(&request.proposal);
            let message = PrimaryMessage::ProposalHeadersRequest(
                request.proposal,
                request.stop_height,
                self.name,
            );
            let bytes = bincode::serialize(&message)
                .expect("Failed to serialize proposal suffix request");

            if certifiers.is_empty() {
                let addresses = self.other_primary_addresses();
                self.network
                    .lucky_broadcast(addresses, Bytes::from(bytes), INITIAL_SYNC_FANOUT)
                    .await;
            } else {
                let fanout = certifiers.len();
                self.network
                    .lucky_broadcast(certifiers, Bytes::from(bytes), fanout)
                    .await;
            }
        }
    }

    /// Execute a single proposal sync: register waiters and send network requests.
    async fn execute_proposal_sync(
        &mut self,
        missing: Vec<Proposal>,
        consensus_message: ConsensusMessage,
        header: Header,
    ) {
        let height = header.height();
        let author = header.author;
        let id = proposal_digest(&consensus_message);

        // Ensure we sync only once per proposal
        if self.pending.contains_key(&id) {
            return;
        }

        // Add the header to the waiter pool.
        let wait_for = missing
            .iter()
            .cloned()
            .map(|x| (x.header_digest.to_vec(), self.store.clone()))
            .collect();
        let (tx_cancel, rx_cancel) = channel(1);
        self.pending.insert(id, (height, tx_cancel));

        // We can't push to the FuturesUnordered from here because of borrow issues,
        // so we return the future. Instead, we handle the loopback through tx_consensus_loopback
        // by spawning a task.
        let tx_loopback = self.tx_consensus_loopback.clone();
        let pending_id = proposal_digest(&consensus_message);
        tokio::spawn(async move {
            let result = Self::proposal_waiter(wait_for, (consensus_message, header), rx_cancel).await;
            match result {
                Ok(Some(deliver)) => {
                    let _ = tx_loopback.send(deliver).await;
                }
                Ok(None) => {
                    // Cancelled by GC
                }
                Err(e) => {
                    error!("Proposal waiter error: {}", e);
                }
            }
        });

        // [E1] Send sync requests only for proposals not already in-flight.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to measure time")
            .as_millis();
        let mut requires_sync = Vec::new();
        for proposal in &missing {
            // [E1] Skip if we're already fetching this proposal header
            if self.inflight_proposals.contains(&proposal.header_digest) {
                continue;
            }
            self.inflight_proposals.insert(proposal.header_digest.clone());

            self.parent_requests.entry(proposal.header_digest.clone()).or_insert_with(|| {
                requires_sync.push(proposal.header_digest.clone());
                (proposal.height, now)
            });
        }
        if !requires_sync.is_empty() {
            // [E2] Fan out to multiple nodes instead of just the author.
            // During partition recovery, the author might be a returning node that's
            // still syncing itself. Targeting multiple nodes increases the chance of
            // a fast response.
            let addresses: Vec<_> = self.committee
                .others_primaries(&self.name)
                .iter()
                .map(|(_, x)| x.primary_to_primary)
                .collect();
            let message = PrimaryMessage::HeadersRequest(requires_sync, self.name);
            let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
            self.network.lucky_broadcast(addresses, Bytes::from(bytes), INITIAL_SYNC_FANOUT).await;
        }
    }

    /// Main loop listening to the `Synchronizer` messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                Some(message) = self.rx_synchronizer.recv() => {
                    match message {
                        WaiterMessage::SyncBatches(missing, header, mode) => {
                            // Track sync request received
                            DISSEMINATION_HEADER_SYNC_REQUESTS_RECEIVED_TOTAL.inc();

                            debug!("Synching the payload of {}", header);
                            let header_id = header.id.clone();
                            let round = header.height;
                            let author = header.author;
                            let force_sync = match mode {
                                PayloadSyncMode::Live(force_sync) => force_sync,
                                PayloadSyncMode::Historical => true,
                            };
                            let should_loopback = !matches!(mode, PayloadSyncMode::Historical);

                            // Ensure we sync only once per header waiter, but still allow
                            // commit-critical payload recovery to upgrade an existing wait.
                            if self.pending.contains_key(&header_id) {
                                if should_loopback {
                                    self.historical_payload_waiters.remove(&header_id);
                                }
                                if !force_sync {
                                    continue;
                                }
                            } else {
                                // Add the header to the waiter pool. The waiter will return it to when all
                                // its parents are in the store.
                                let wait_for = missing
                                    .iter()
                                    .map(|(digest, worker_id)| {
                                        let key = [digest.as_ref(), &worker_id.to_le_bytes()].concat();
                                        (key.to_vec(), self.store.clone())
                                    })
                                    .collect();
                                let (tx_cancel, rx_cancel) = channel(1);
                                self.pending.insert(header_id.clone(), (round, tx_cancel));
                                let fut = Self::waiter(wait_for, header, rx_cancel);
                                waiting.push(fut);
                                if !should_loopback {
                                    self.historical_payload_waiters.insert(header_id);
                                } else {
                                    self.historical_payload_waiters.remove(&header_id);
                                }
                            }

                            if force_sync {
                                // Ensure we didn't already send a sync request for these parents.
                                let mut requires_sync = HashMap::new();
                                for (digest, worker_id) in missing.into_iter() {
                                    let entry = self
                                        .batch_requests
                                        .entry(digest.clone())
                                        .or_insert((round, false));
                                    if !entry.1 {
                                        requires_sync
                                            .entry(worker_id)
                                            .or_insert_with(Vec::new)
                                            .push(digest.clone());
                                        entry.1 = true;
                                    }
                                    entry.0 = round;
                                }
                                for (worker_id, digests) in requires_sync {
                                    // PrimaryWorkerMessage is delivered to our local worker over
                                    // `primary_to_worker`; the remote author is carried inside the
                                    // message so the worker can fetch from that authority's worker.
                                    let address = self.committee
                                        .worker(&self.name, &worker_id)
                                        .expect("Our worker is not in the committee")
                                        .primary_to_worker;
                                    debug!("Sent syncbatches message for height {}", round);
                                    let message = PrimaryWorkerMessage::SynchronizeCommitted(digests, author);
                                    let bytes = bincode::serialize(&message)
                                        .expect("Failed to serialize batch sync request");
                                    self.network.send(address, Bytes::from(bytes)).await;
                                }
                            }
                        }

                        WaiterMessage::SyncHeader(missing) => {
                            debug!("Syncing on header with digest {}", missing);

                            let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();

                            let mut requires_sync = Vec::new();
                            self.header_requests.entry(missing.clone()).or_insert_with(|| {
                                requires_sync.push(missing);
                                (0, now)
                            });

                            if !requires_sync.is_empty() {
                                let addresses = self.committee
                                .others_primaries(&self.name)
                                .iter()
                                .map(|(_, x)| x.primary_to_primary)
                                .collect();

                                let message = PrimaryMessage::HeadersRequest(requires_sync, self.name);
                                let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
                                // [E2] Use wider fan-out for initial header sync
                                self.network.lucky_broadcast(addresses, Bytes::from(bytes), INITIAL_SYNC_FANOUT).await;
                            }
                        }

                        WaiterMessage::SyncCommittedProposal(proposal, stop_height, slot) => {
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();

                            let digest = proposal.header_digest.clone();
                            let mut queue_request = false;
                            match self.proposal_sync_requests.get_mut(&digest) {
                                Some(request) => {
                                    if slot < request.slot {
                                        request.slot = slot;
                                        queue_request = true;
                                    }
                                    if stop_height < request.stop_height {
                                        request.stop_height = stop_height;
                                        queue_request = true;
                                    }
                                }
                                None => {
                                    self.proposal_sync_requests.insert(
                                        digest.clone(),
                                        PendingSuffixSync {
                                            proposal,
                                            stop_height,
                                            slot,
                                            timestamp: now,
                                        },
                                    );
                                    queue_request = true;
                                }
                            }

                            if queue_request {
                                self.pending_commit_syncs.push(PendingCommitSync { slot, digest });
                                self.dispatch_pending_commit_syncs().await;
                            }
                        }

                        WaiterMessage::ClearCommittedProposalSync(digest) => {
                            self.proposal_sync_requests.remove(&digest);
                        }

                        WaiterMessage::SyncParent(missing, header) => {
                            debug!("Synching the parents of {}", header);
                            let header_id = header.id.clone();
                            let height = header.height();
                            let author = header.author;

                            // Ensure we sync only once per header.
                            if self.pending.contains_key(&header_id) {
                                continue;
                            }

                            // Add the header to the waiter pool. The waiter will return it to us
                            // when all its parents are in the store.
                            let mut wait_for = Vec::new();
                            wait_for.push((missing.to_vec(), self.store.clone()));
                            let (tx_cancel, rx_cancel) = channel(1);
                            self.pending.insert(header_id, (height, tx_cancel));
                            let fut = Self::waiter(wait_for, header, rx_cancel);
                            waiting.push(fut);

                            // Ensure we didn't already sent a sync request for these parents.
                            // [E2] Send to multiple nodes instead of just the author for faster response.
                            let now = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis();
                            let mut requires_sync = Vec::new();
                            self.parent_requests.entry(missing.clone()).or_insert_with(|| {
                                requires_sync.push(missing);
                                (height, now)
                            });
                            if !requires_sync.is_empty() {
                                let addresses: Vec<_> = self.committee
                                    .others_primaries(&self.name)
                                    .iter()
                                    .map(|(_, x)| x.primary_to_primary)
                                    .collect();
                                let message = PrimaryMessage::HeadersRequest(requires_sync, self.name);
                                let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
                                self.network.lucky_broadcast(addresses, Bytes::from(bytes), INITIAL_SYNC_FANOUT).await;
                            }
                        }


                        WaiterMessage::SyncProposals(missing, consensus_message, header) => {
                            let slot = Self::consensus_slot(&consensus_message);
                            let id = proposal_digest(&consensus_message);

                            // Ensure we sync only once per proposal (existing dedup)
                            if self.pending.contains_key(&id) {
                                continue;
                            }

                            // [F] Queue this sync request for priority-ordered processing
                            self.pending_proposal_syncs.push(PendingProposalSync {
                                slot,
                                missing,
                                consensus_message,
                                header,
                            });
                        }
                    }
                },

                Some(result) = waiting.next() => match result {
                    Ok(Some(header)) => {
                        debug!("Finished synching {:?}", header);
                        let _ = self.pending.remove(&header.id);
                        for x in header.payload.keys() {
                            let _ = self.batch_requests.remove(x);
                        }
                        let _ = self.parent_requests.remove(&header.parent_cert.header_digest);
                        DISSEMINATION_RECOVERED_HEADERS_TOTAL.inc();

                        if !self.historical_payload_waiters.remove(&header.id) {
                            self.tx_core.send(header).await.expect("Failed to send header");
                        }
                    },
                    Ok(None) => {
                        // This request has been canceled.
                    },
                    Err(e) => {
                        error!("{}", e);
                        panic!("Storage failure: killing node.");
                    }
                },

                // Note: Proposal sync waiters are now handled by spawned tasks
                // that send directly to tx_consensus_loopback when complete.

                () = &mut timer => {
                    self.dispatch_pending_commit_syncs().await;

                    // [F] First, process any queued proposal syncs in slot-priority order.
                    // Sort by slot ascending so lowest (most critical) slots sync first.
                    self.drain_pending_proposal_syncs().await;

                    // We optimistically sent sync requests to a single node. If this timer triggers,
                    // it means we were wrong to trust it. We are done waiting for a reply and we now
                    // broadcast the request to all nodes.
                    let now = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Failed to measure time")
                        .as_millis();

                    //Retry CertificateRequests
                    let mut retry = Vec::new();
                    for (digest, (_, timestamp)) in self.parent_requests.iter_mut() {
                        if *timestamp + (self.sync_retry_delay as u128) < now {
                            debug!("Requesting retry sync for parent header {} (retry)", digest);
                            retry.push(digest.clone());
                            *timestamp = now;
                        }
                    }
                    if !retry.is_empty() {
                        DISSEMINATION_SYNC_RETRIES_TOTAL
                            .with_label_values(&["parent"])
                            .inc_by(retry.len() as u64);
                        let addresses = self.committee.others_primaries(&self.name).iter().map(|(_, x)| x.primary_to_primary).collect();
                        let message = PrimaryMessage::HeadersRequest(retry, self.name);
                        let bytes = bincode::serialize(&message).expect("Failed to serialize cert request");
                        self.network.lucky_broadcast(addresses, Bytes::from(bytes), self.sync_retry_nodes).await;
                    }

                    let mut suffix_retry = Vec::new();
                    for request in self.proposal_sync_requests.values_mut() {
                        if request.timestamp + (self.sync_retry_delay as u128) < now {
                            request.timestamp = now;
                            suffix_retry.push(request.clone());
                        }
                    }
                    if !suffix_retry.is_empty() {
                        DISSEMINATION_SYNC_RETRIES_TOTAL
                            .with_label_values(&["suffix"])
                            .inc_by(suffix_retry.len() as u64);
                        let addresses: Vec<_> = self
                            .committee
                            .others_primaries(&self.name)
                            .iter()
                            .map(|(_, x)| x.primary_to_primary)
                            .collect();
                        for request in suffix_retry {
                            let message = PrimaryMessage::ProposalHeadersRequest(
                                request.proposal,
                                request.stop_height,
                                self.name,
                            );
                            let bytes = bincode::serialize(&message)
                                .expect("Failed to serialize proposal suffix request");
                            self.network
                                .lucky_broadcast(
                                    addresses.clone(),
                                    Bytes::from(bytes),
                                    self.sync_retry_nodes,
                                )
                                .await;
                        }
                    }

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                }
            }

            // Cleanup internal state.
            let round = self.consensus_round.load(Ordering::Relaxed);
            if round > self.gc_depth {
                let mut gc_round = round - self.gc_depth;

                for (r, handler) in self.pending.values() {
                    if r <= &gc_round {
                        let _ = handler.send(()).await;
                    }
                }
                self.pending.retain(|_, (r, _)| r > &mut gc_round);
                self.batch_requests.retain(|_, (r, _)| r > &mut gc_round);
                self.parent_requests.retain(|_, (r, _)| r > &mut gc_round);
                self.header_requests.retain(|_, (r, _)| r > &mut gc_round);
                self.proposal_sync_requests
                    .retain(|_, request| request.proposal.height > gc_round);
                let active_proposal_syncs: HashSet<_> =
                    self.proposal_sync_requests.keys().cloned().collect();
                self.pending_commit_syncs
                    .retain(|pending| active_proposal_syncs.contains(&pending.digest));
                // Keep only proposal digests that still have outstanding header fetch state.
                let active_parent_requests: HashSet<_> =
                    self.parent_requests.keys().cloned().collect();
                self.inflight_proposals
                    .retain(|digest| active_parent_requests.contains(digest));
                let active_pending_headers: HashSet<_> = self.pending.keys().cloned().collect();
                self.historical_payload_waiters
                    .retain(|digest| active_pending_headers.contains(digest));
            }
            self.update_recovery_metrics();
        }
    }
}

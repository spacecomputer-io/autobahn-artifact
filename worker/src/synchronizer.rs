// Copyright(C) Facebook, Inc. and its affiliates.
use crate::metrics::{
    WORKER_RECOVERY_PENDING_BATCHES, WORKER_RECOVERY_STALLED_BATCHES,
    WORKER_RECOVERY_SYNC_REQUESTS_TOTAL,
};
use crate::worker::{Round, WorkerMessage};
use bytes::Bytes;
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey};
use futures::stream::futures_unordered::FuturesUnordered;
use futures::stream::StreamExt as _;
use log::{debug, error};
use network::SimpleSender;
use primary::PrimaryWorkerMessage;
use std::collections::{HashMap, HashSet};
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
/// Retry only a bounded number of commit-critical digests per timer tick, ordered oldest-first.
const MAX_COMMIT_CRITICAL_RETRIES_PER_TICK: usize = 128;
/// Temporarily de-prioritize targets that have been retried this many times for the same digest.
const TARGET_COOLDOWN_AFTER_ATTEMPTS: u32 = 3;
/// How long to keep a target on cooldown after repeated unsuccessful attempts.
const TARGET_COOLDOWN_MS: u128 = 5_000;
/// A pending batch recovery is considered "stalled" after this many milliseconds.
const STALLED_BATCH_THRESHOLD_MS: u128 = 5_000;

#[derive(Clone, Copy, Eq, PartialEq, Hash)]
enum SyncPriority {
    Background,
    CommitCritical,
}

struct PendingBatchSync {
    round: Round,
    blocked_height: Round,
    cancel: Sender<()>,
    timestamp: u128,
    target: PublicKey,
    priority: SyncPriority,
    dependents: u64,
    attempted_targets: HashSet<PublicKey>,
    target_attempts: HashMap<PublicKey, u32>,
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
    /// Targets that should be skipped temporarily during retry selection.
    target_cooldowns: HashMap<PublicKey, u128>,
}

impl Synchronizer {
    fn candidate_targets(&self) -> Vec<PublicKey> {
        self.committee
            .others_workers(&self.name, &self.id)
            .iter()
            .map(|(name, _)| name.clone())
            .collect()
    }

    fn select_retry_target_from_candidates(
        candidates: &[PublicKey],
        request: &mut PendingBatchSync,
        target_cooldowns: &HashMap<PublicKey, u128>,
        now: u128,
    ) -> PublicKey {
        if !candidates.is_empty() && request.attempted_targets.len() >= candidates.len() {
            request.attempted_targets.clear();
            request.attempted_targets.insert(request.target.clone());
        }

        if let Some(next) = candidates
            .iter()
            .filter(|candidate| {
                target_cooldowns
                    .get(*candidate)
                    .map(|until| *until <= now)
                    .unwrap_or(true)
            })
            .min_by_key(|candidate| {
                (
                    request.target_attempts.get(*candidate).copied().unwrap_or(0),
                    if **candidate == request.target { 1 } else { 0 },
                )
            })
            .cloned()
        {
            return next;
        }

        candidates
            .iter()
            .min_by_key(|candidate| {
                (
                    request.target_attempts.get(*candidate).copied().unwrap_or(0),
                    if **candidate == request.target { 1 } else { 0 },
                )
            })
            .cloned()
            .unwrap_or_else(|| request.target.clone())
    }

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
                target_cooldowns: HashMap::new(),
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

    /// Non-blocking send for both initial and retry batch sync requests.
    /// Uses try_send so the synchronizer event loop never blocks on a wedged
    /// connection. Returns (target_enqueued, fanout_dropped):
    /// - target_enqueued: whether the primary target send succeeded
    /// - fanout_dropped: number of secondary fanout sends that were dropped
    ///
    /// For initial Background sends, only targets the designated peer.
    /// For initial CommitCritical and all retries, also fans out to other workers.
    fn send_sync_request(
        &mut self,
        digests: Vec<Digest>,
        target: PublicKey,
        priority: SyncPriority,
        retry: bool,
    ) -> (bool, usize) {
        if digests.is_empty() {
            return (true, 0);
        }

        let message = WorkerMessage::BatchRequest(digests, self.name);
        let serialized =
            bincode::serialize(&message).expect("Failed to serialize our own message");

        // Send to primary target (non-blocking).
        let target_address = match self.committee.worker(&target, &self.id) {
            Ok(address) => address.worker_to_worker,
            Err(e) => {
                error!("The primary asked us to sync with an unknown node: {}", e);
                return (false, 0);
            }
        };
        WORKER_RECOVERY_SYNC_REQUESTS_TOTAL.inc();
        let target_enqueued = self
            .network
            .send_best_effort(target_address, Bytes::from(serialized.clone()));

        // Fan-out to other workers for retries and commit-critical initial sends.
        // Initial background sends target only the designated peer.
        let mut fanout_dropped = 0;
        let needs_fanout = retry || priority == SyncPriority::CommitCritical;
        if needs_fanout {
            let other_addresses: Vec<_> = self
                .committee
                .others_workers(&self.name, &self.id)
                .iter()
                .filter(|(name, _)| *name != target)
                .map(|(_, address)| address.worker_to_worker)
                .collect();
            if !other_addresses.is_empty() {
                let fanout = match priority {
                    SyncPriority::CommitCritical => other_addresses
                        .len()
                        .min(self.sync_retry_nodes.max(INITIAL_COMMITTED_SYNC_FANOUT)),
                    SyncPriority::Background => other_addresses.len().min(self.sync_retry_nodes),
                };
                fanout_dropped = self.network.lucky_broadcast_best_effort(
                    other_addresses,
                    Bytes::from(serialized),
                    fanout,
                );
            }
        }

        (target_enqueued, fanout_dropped)
    }

    /// Main loop listening to the primary's messages.
    async fn run(&mut self) {
        let mut waiting = FuturesUnordered::new();

        let timer = sleep(Duration::from_millis(TIMER_RESOLUTION));
        tokio::pin!(timer);

        loop {
            tokio::select! {
                // Handle primary's messages.
                Some(message) = self.rx_message.recv() => {
                    match message {
                    PrimaryWorkerMessage::Synchronize(digests, target) => {
                        let priority = SyncPriority::Background;
                        let now = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Failed to measure time")
                            .as_millis();

                        let mut missing = Vec::new();
                        for digest in digests {
                            if let Some(existing) = self.pending.get_mut(&digest) {
                                existing.dependents += 1;
                                existing.attempted_targets.insert(target.clone());
                                if priority == SyncPriority::CommitCritical
                                    && existing.priority != SyncPriority::CommitCritical
                                {
                                    existing.priority = SyncPriority::CommitCritical;
                                    existing.target = target.clone();
                                    existing.timestamp = now;
                                    missing.push(digest.clone());
                                }
                                continue;
                            }

                            // Skip store.read() pre-check: the waiter's notify_read
                            // resolves immediately if the batch is already present
                            // (store/src/lib.rs:47). Avoiding the store channel here
                            // keeps the event loop non-blocking under recovery load.
                            missing.push(digest.clone());

                            let deliver = digest.clone();
                            let (tx_cancel, rx_cancel) = channel(1);
                            let fut = Self::waiter(digest.clone(), self.store.clone(), deliver, rx_cancel);
                            waiting.push(fut);
                            self.pending.insert(
                                digest,
                                PendingBatchSync {
                                    round: self.round,
                                    blocked_height: Round::MAX,
                                    cancel: tx_cancel,
                                    timestamp: now,
                                    target: target.clone(),
                                    priority,
                                    dependents: 1,
                                    attempted_targets: HashSet::from([target.clone()]),
                                    target_attempts: HashMap::from([(target.clone(), 1)]),
                                },
                            );
                        }

                        let (target_ok, _fanout_drops) =
                            self.send_sync_request(missing.clone(), target, priority, false);
                        if !target_ok {
                            // Target enqueue failed — make these digests immediately
                            // eligible for retry on the next timer tick.
                            for digest in &missing {
                                if let Some(request) = self.pending.get_mut(digest) {
                                    request.timestamp = 0;
                                }
                            }
                        }
                    }
                    PrimaryWorkerMessage::SynchronizeCommitted(digests, target, blocked_height) => {
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
                                existing.dependents += 1;
                                existing.attempted_targets.insert(target.clone());
                                if existing.priority != SyncPriority::CommitCritical {
                                    existing.priority = SyncPriority::CommitCritical;
                                    existing.target = target.clone();
                                    existing.timestamp = now;
                                    missing.push(digest.clone());
                                }
                                existing.blocked_height = existing.blocked_height.min(blocked_height);
                                continue;
                            }

                            // Skip store.read() pre-check: the waiter's notify_read
                            // resolves immediately if the batch is already present.
                            missing.push(digest.clone());

                            // Add the digest to the waiter.
                            let deliver = digest.clone();
                            let (tx_cancel, rx_cancel) = channel(1);
                            let fut = Self::waiter(digest.clone(), self.store.clone(), deliver, rx_cancel);
                            waiting.push(fut);
                            self.pending.insert(
                                digest,
                                PendingBatchSync {
                                    round: self.round,
                                    blocked_height,
                                    cancel: tx_cancel,
                                    timestamp: now,
                                    target: target.clone(),
                                    priority,
                                    dependents: 1,
                                    attempted_targets: HashSet::from([target.clone()]),
                                    target_attempts: HashMap::from([(target.clone(), 1)]),
                                },
                            );
                        }

                        let (target_ok, _fanout_drops) =
                            self.send_sync_request(missing.clone(), target, priority, false);
                        if !target_ok {
                            for digest in &missing {
                                if let Some(request) = self.pending.get_mut(digest) {
                                    request.timestamp = 0;
                                }
                            }
                        }
                    }
                    PrimaryWorkerMessage::Cleanup(round) => {
                        // Keep track of the primary's round number.
                        self.round = round;

                        // Cleanup internal state.
                        if self.round < self.gc_depth {
                            continue;
                        }

                        let gc_round = self.round - self.gc_depth;
                        let mut evicted = HashSet::new();
                        for (digest, request) in &self.pending {
                            // Commit-critical batch recovery must outlive round GC. The committer
                            // may still be blocked on these payloads hundreds of rounds later,
                            // and the primary currently issues only one historical sync request
                            // per missing header before polling local storage.
                            if request.priority == SyncPriority::Background
                                && request.round <= gc_round
                            {
                                evicted.insert(digest.clone());
                            }
                        }
                        for digest in &evicted {
                            let request = self
                                .pending
                                .get(digest)
                                .expect("gc eviction digest should remain pending during cleanup");
                            let _ = request.cancel.clone().send(()).await;
                        }
                        self.pending.retain(|digest, request| {
                            request.priority == SyncPriority::CommitCritical
                                || request.round > gc_round
                                || !evicted.contains(digest)
                        });
                    }
                }},

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
                    self.target_cooldowns.retain(|_, until| *until > now);
                    let candidate_targets = self.candidate_targets();

                    let mut retry_groups: HashMap<(SyncPriority, PublicKey), Vec<Digest>> =
                        HashMap::new();
                    let mut background_retry = Vec::new();
                    let mut committed_retry = Vec::new();
                    for (digest, request) in &self.pending {
                        if request.timestamp + (self.sync_retry_delay as u128) < now {
                            match request.priority {
                                SyncPriority::Background => background_retry.push(digest.clone()),
                                SyncPriority::CommitCritical => committed_retry.push(digest.clone()),
                            }
                        }
                    }
                    committed_retry.sort_by_key(|digest| {
                        self.pending
                            .get(digest)
                            .map(|request| request.blocked_height)
                            .unwrap_or(Round::MAX)
                    });
                    // Shared budget: commit-critical first (priority), then background
                    // fills remaining slots. Total capped to prevent retry storms —
                    // each retry sends network fan-out to other workers.
                    let mut retry_list: Vec<Digest> = committed_retry.into_iter()
                        .chain(background_retry.into_iter())
                        .collect();
                    if retry_list.len() > MAX_COMMIT_CRITICAL_RETRIES_PER_TICK {
                        retry_list.truncate(MAX_COMMIT_CRITICAL_RETRIES_PER_TICK);
                    }

                    let retry_digests = retry_list.into_iter();
                    for digest in retry_digests {
                        let mut cooldown_target = None;
                        let (priority, next_target_for_group) = {
                            let Some(request) = self.pending.get_mut(&digest) else {
                                continue;
                            };
                            debug!("Requesting sync for batch {} (retry)", digest);
                            // NOTE: timestamp is NOT advanced here. It is only
                            // advanced after send_sync_request_nonblocking confirms
                            // the message was enqueued, to avoid suppressing retries
                            // when sends are dropped due to backpressure.
                            let next_target = Self::select_retry_target_from_candidates(
                                &candidate_targets,
                                request,
                                &self.target_cooldowns,
                                now,
                            );
                            let attempts = request.target_attempts.entry(next_target.clone()).or_insert(0);
                            *attempts += 1;
                            if *attempts >= TARGET_COOLDOWN_AFTER_ATTEMPTS {
                                cooldown_target = Some(next_target.clone());
                                *attempts = 0;
                            }
                            request.target = next_target.clone();
                            request.attempted_targets.insert(next_target.clone());
                            (request.priority, next_target)
                        };
                        if let Some(target) = cooldown_target {
                            self.target_cooldowns.insert(target, now + TARGET_COOLDOWN_MS);
                        }
                        retry_groups
                            .entry((priority, next_target_for_group))
                            .or_default()
                            .push(digest.clone());
                    }

                    for ((priority, target), digests) in retry_groups {
                        let (target_enqueued, _fanout_dropped) = self
                            .send_sync_request(digests.clone(), target, priority, true);
                        if target_enqueued {
                            // Primary target got the message — a meaningful retry
                            // went out. Advance timestamps regardless of fanout drops.
                            for digest in &digests {
                                if let Some(request) = self.pending.get_mut(digest) {
                                    request.timestamp = now;
                                }
                            }
                        }
                    }

                    // Update recovery state gauges.
                    WORKER_RECOVERY_PENDING_BATCHES.set(self.pending.len() as i64);
                    let stalled = self.pending.values()
                        .filter(|r| now.saturating_sub(r.timestamp) >= STALLED_BATCH_THRESHOLD_MS)
                        .count();
                    WORKER_RECOVERY_STALLED_BATCHES.set(stalled as i64);

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                },
            }
        }
    }
}

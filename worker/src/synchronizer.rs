// Copyright(C) Facebook, Inc. and its affiliates.
use crate::metrics::{
    WORKER_SYNC_COMPLETIONS_TOTAL, WORKER_SYNC_PENDING_BATCHES, WORKER_SYNC_PENDING_DEPENDENTS,
    WORKER_SYNC_RECOVERY_LATENCY_MS, WORKER_SYNC_REQUESTS_TOTAL, WORKER_SYNC_RETRIES_TOTAL,
    WORKER_SYNC_TARGET_ROTATIONS_TOTAL, WORKER_SYNC_GC_EVICTIONS_TOTAL,
    WORKER_SYNC_OLDEST_BLOCKED_HEIGHT, WORKER_SYNC_RETRY_BUDGET_SKIPS_TOTAL,
    WORKER_SYNC_STALLED_BATCHES, WORKER_SYNC_TARGET_COOLDOWNS_TOTAL,
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
/// A pending batch is considered stalled after this long without local recovery.
const STALLED_BATCH_THRESHOLD_MS: u128 = 5_000;
/// Temporarily de-prioritize targets that have been retried this many times for the same digest.
const TARGET_COOLDOWN_AFTER_ATTEMPTS: u32 = 3;
/// How long to keep a target on cooldown after repeated unsuccessful attempts.
const TARGET_COOLDOWN_MS: u128 = 5_000;

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
    first_request_timestamp: u128,
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
    fn priority_label(priority: SyncPriority) -> &'static str {
        match priority {
            SyncPriority::Background => "background",
            SyncPriority::CommitCritical => "commit_critical",
        }
    }

    fn update_sync_metrics(&self) {
        let mut background_batches = 0_i64;
        let mut committed_batches = 0_i64;
        let mut background_dependents = 0_i64;
        let mut committed_dependents = 0_i64;
        let mut background_stalled = 0_i64;
        let mut committed_stalled = 0_i64;
        let mut oldest_background_height = i64::MAX;
        let mut oldest_committed_height = i64::MAX;
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Failed to measure time")
            .as_millis();

        for request in self.pending.values() {
            match request.priority {
                SyncPriority::Background => {
                    background_batches += 1;
                    background_dependents += request.dependents as i64;
                    oldest_background_height =
                        oldest_background_height.min(request.blocked_height as i64);
                    if now.saturating_sub(request.first_request_timestamp)
                        >= STALLED_BATCH_THRESHOLD_MS
                    {
                        background_stalled += 1;
                    }
                }
                SyncPriority::CommitCritical => {
                    committed_batches += 1;
                    committed_dependents += request.dependents as i64;
                    oldest_committed_height =
                        oldest_committed_height.min(request.blocked_height as i64);
                    if now.saturating_sub(request.first_request_timestamp)
                        >= STALLED_BATCH_THRESHOLD_MS
                    {
                        committed_stalled += 1;
                    }
                }
            }
        }

        WORKER_SYNC_PENDING_BATCHES
            .with_label_values(&["background"])
            .set(background_batches);
        WORKER_SYNC_PENDING_BATCHES
            .with_label_values(&["commit_critical"])
            .set(committed_batches);
        WORKER_SYNC_PENDING_DEPENDENTS
            .with_label_values(&["background"])
            .set(background_dependents);
        WORKER_SYNC_PENDING_DEPENDENTS
            .with_label_values(&["commit_critical"])
            .set(committed_dependents);
        WORKER_SYNC_STALLED_BATCHES
            .with_label_values(&["background"])
            .set(background_stalled);
        WORKER_SYNC_STALLED_BATCHES
            .with_label_values(&["commit_critical"])
            .set(committed_stalled);
        WORKER_SYNC_OLDEST_BLOCKED_HEIGHT
            .with_label_values(&["background"])
            .set(if oldest_background_height == i64::MAX {
                0
            } else {
                oldest_background_height
            });
        WORKER_SYNC_OLDEST_BLOCKED_HEIGHT
            .with_label_values(&["commit_critical"])
            .set(if oldest_committed_height == i64::MAX {
                0
            } else {
                oldest_committed_height
            });
    }

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

        let phase = if retry { "retry" } else { "initial" };
        WORKER_SYNC_REQUESTS_TOTAL
            .with_label_values(&[Self::priority_label(priority), phase])
            .inc_by(digests.len() as u64);

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
                    let fanout = other_addresses.len().min(self.sync_retry_nodes);
                    self.network
                        .lucky_broadcast(other_addresses, Bytes::from(serialized), fanout)
                        .await;
                }
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
                        .lucky_broadcast(
                            other_addresses,
                            Bytes::from(serialized),
                            fanout,
                        )
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
                                existing.dependents += 1;
                                existing.attempted_targets.insert(target.clone());
                                if priority == SyncPriority::CommitCritical
                                    && existing.priority != SyncPriority::CommitCritical
                                {
                                    existing.priority = SyncPriority::CommitCritical;
                                    existing.target = target.clone();
                                    existing.timestamp = now;
                                    existing.blocked_height = existing.blocked_height.min(self.round);
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
                                    blocked_height: self.round,
                                    cancel: tx_cancel,
                                    timestamp: now,
                                    first_request_timestamp: now,
                                    target: target.clone(),
                                    priority,
                                    dependents: 1,
                                    attempted_targets: HashSet::from([target.clone()]),
                                    target_attempts: HashMap::from([(target.clone(), 1)]),
                                },
                            );
                        }

                        self.send_sync_request(missing, target, priority, false).await;
                        self.update_sync_metrics();
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
                                    blocked_height,
                                    cancel: tx_cancel,
                                    timestamp: now,
                                    first_request_timestamp: now,
                                    target: target.clone(),
                                    priority,
                                    dependents: 1,
                                    attempted_targets: HashSet::from([target.clone()]),
                                    target_attempts: HashMap::from([(target.clone(), 1)]),
                                },
                            );
                        }

                        self.send_sync_request(missing, target, priority, false).await;
                        self.update_sync_metrics();
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
                            WORKER_SYNC_GC_EVICTIONS_TOTAL
                                .with_label_values(&[Self::priority_label(request.priority)])
                                .inc();
                            let _ = request.cancel.clone().send(()).await;
                        }
                        self.pending.retain(|digest, request| {
                            request.priority == SyncPriority::CommitCritical
                                || request.round > gc_round
                                || !evicted.contains(digest)
                        });
                        self.update_sync_metrics();
                    }
                },

                // Stream out the futures of the `FuturesUnordered` that completed.
                Some(result) = waiting.next() => match result {
                    Ok(Some(digest)) => {
                        // We got the batch, remove it from the pending list.
                        if let Some(request) = self.pending.remove(&digest) {
                            WORKER_SYNC_COMPLETIONS_TOTAL
                                .with_label_values(&[Self::priority_label(request.priority)])
                                .inc();
                            let latency_ms = SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .expect("Failed to measure time")
                                .as_millis()
                                .saturating_sub(request.first_request_timestamp);
                            WORKER_SYNC_RECOVERY_LATENCY_MS
                                .with_label_values(&[Self::priority_label(request.priority)])
                                .observe(latency_ms as f64);
                        }
                        self.update_sync_metrics();
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
                    if committed_retry.len() > MAX_COMMIT_CRITICAL_RETRIES_PER_TICK {
                        WORKER_SYNC_RETRY_BUDGET_SKIPS_TOTAL
                            .with_label_values(&["commit_critical"])
                            .inc_by((committed_retry.len() - MAX_COMMIT_CRITICAL_RETRIES_PER_TICK) as u64);
                        committed_retry.truncate(MAX_COMMIT_CRITICAL_RETRIES_PER_TICK);
                    }

                    let retry_digests = background_retry.into_iter().chain(committed_retry.into_iter());
                    for digest in retry_digests {
                        let mut cooldown_target = None;
                        let (priority, next_target_for_group) = {
                            let Some(request) = self.pending.get_mut(&digest) else {
                                continue;
                            };
                            debug!("Requesting sync for batch {} (retry)", digest);
                            request.timestamp = now;
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
                            if next_target != request.target {
                                WORKER_SYNC_TARGET_ROTATIONS_TOTAL
                                    .with_label_values(&[Self::priority_label(request.priority)])
                                    .inc();
                            }
                            request.target = next_target.clone();
                            request.attempted_targets.insert(next_target.clone());
                            WORKER_SYNC_RETRIES_TOTAL
                                .with_label_values(&[Self::priority_label(request.priority)])
                                .inc();
                            (request.priority, next_target)
                        };
                        if let Some(target) = cooldown_target {
                            self.target_cooldowns.insert(target, now + TARGET_COOLDOWN_MS);
                            WORKER_SYNC_TARGET_COOLDOWNS_TOTAL
                                .with_label_values(&[Self::priority_label(priority)])
                                .inc();
                        }
                        retry_groups
                            .entry((priority, next_target_for_group))
                            .or_default()
                            .push(digest.clone());
                    }

                    for ((priority, target), digests) in retry_groups {
                        self.send_sync_request(digests, target, priority, true).await;
                    }

                    // Reschedule the timer.
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(TIMER_RESOLUTION));
                    self.update_sync_metrics();
                },
            }
        }
    }
}

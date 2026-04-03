use crypto::Digest;
use lazy_static::lazy_static;
use prometheus::{
    register_histogram, register_int_counter, register_int_counter_vec, register_int_gauge,
    Histogram, IntCounter, IntCounterVec, IntGauge,
};
use std::collections::HashMap;
use std::sync::Mutex;

/// Histogram bucket boundaries for latency observations (milliseconds).
const LATENCY_BUCKETS_MS: &[f64] = &[
    1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
];

lazy_static! {
    // ============================================================================
    // DISSEMINATION THROUGHPUT COUNTERS
    // ============================================================================

    pub static ref DISSEMINATION_HEADERS_CREATED_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_headers_created_total",
            "Total headers created by this node"
        )
        .expect("failed to register dissemination_headers_created_total");

    pub static ref DISSEMINATION_HEADERS_POA_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_headers_poa_total",
            "Total headers that received proof of availability"
        )
        .expect("failed to register dissemination_headers_poa_total");

    // ============================================================================
    // DISSEMINATION RECOVERY / QUALITY COUNTERS
    // ============================================================================

    pub static ref DISSEMINATION_MISSING_PARENT_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_missing_parent_total",
            "Total missing parent events encountered"
        )
        .expect("failed to register dissemination_missing_parent_total");

    pub static ref DISSEMINATION_MISSING_PAYLOAD_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_missing_payload_total",
            "Total missing payload events encountered"
        )
        .expect("failed to register dissemination_missing_payload_total");

    pub static ref DISSEMINATION_RECOVERED_HEADERS_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_recovered_headers_total",
            "Total headers recovered via sync"
        )
        .expect("failed to register dissemination_recovered_headers_total");

    pub static ref DISSEMINATION_SYNC_REQUESTS_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "dissemination_sync_requests_total",
            "Total sync requests sent by kind",
            &["kind"]
        )
        .expect("failed to register dissemination_sync_requests_total");

    // ============================================================================
    // DISSEMINATION STATE GAUGES
    // ============================================================================

    pub static ref DISSEMINATION_INFLIGHT_HOLES: IntGauge =
        register_int_gauge!(
            "dissemination_inflight_holes",
            "Current number of inflight holes in the DAG"
        )
        .expect("failed to register dissemination_inflight_holes");

    // ============================================================================
    // CONSENSUS THROUGHPUT COUNTERS
    // ============================================================================

    pub static ref CONSENSUS_SLOTS_COMMITTED_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_slots_committed_total",
            "Total slots committed"
        )
        .expect("failed to register consensus_slots_committed_total");

    pub static ref CONSENSUS_SLOTS_EXECUTED_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_slots_executed_total",
            "Total slots executed"
        )
        .expect("failed to register consensus_slots_executed_total");

    pub static ref CONSENSUS_FAST_PATH_COMMITS_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_fast_path_commits_total",
            "Total fast-path slot commits"
        )
        .expect("failed to register consensus_fast_path_commits_total");

    pub static ref CONSENSUS_SLOW_PATH_COMMITS_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_slow_path_commits_total",
            "Total slow-path slot commits"
        )
        .expect("failed to register consensus_slow_path_commits_total");

    // ============================================================================
    // CONSENSUS BEHAVIOR COUNTERS
    // ============================================================================

    pub static ref CONSENSUS_VIEW_CHANGES_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_view_changes_total",
            "Total view changes via timeout certificate"
        )
        .expect("failed to register consensus_view_changes_total");

    // ============================================================================
    // CONSENSUS STATE GAUGES
    // ============================================================================

    pub static ref CONSENSUS_CURRENT_SLOT: IntGauge =
        register_int_gauge!(
            "consensus_current_slot",
            "Current consensus slot number"
        )
        .expect("failed to register consensus_current_slot");

    pub static ref CONSENSUS_OLDEST_BLOCKED_SLOT: IntGauge =
        register_int_gauge!(
            "consensus_oldest_blocked_slot",
            "Oldest slot blocked on missing data"
        )
        .expect("failed to register consensus_oldest_blocked_slot");

    pub static ref CONSENSUS_CURRENT_VIEW: IntGauge =
        register_int_gauge!(
            "consensus_current_view",
            "Current consensus view number"
        )
        .expect("failed to register consensus_current_view");

    // ============================================================================
    // LATENCY HISTOGRAMS
    // ============================================================================

    pub static ref LATENCY_TX_TO_SLOT_COMMIT_MS: Histogram =
        register_histogram!(
            "latency_tx_to_slot_commit_ms",
            "Latency from transaction ingress to slot commit (ms)",
            LATENCY_BUCKETS_MS.to_vec()
        )
        .expect("failed to register latency_tx_to_slot_commit_ms");

    pub static ref LATENCY_TX_TO_SLOT_EXECUTE_MS: Histogram =
        register_histogram!(
            "latency_tx_to_slot_execute_ms",
            "Latency from transaction ingress to slot execute (ms)",
            LATENCY_BUCKETS_MS.to_vec()
        )
        .expect("failed to register latency_tx_to_slot_execute_ms");

    pub static ref LATENCY_HEADER_TO_POA_MS: Histogram =
        register_histogram!(
            "latency_header_to_poa_ms",
            "Latency from header creation to header PoA (ms)",
            LATENCY_BUCKETS_MS.to_vec()
        )
        .expect("failed to register latency_header_to_poa_ms");

    pub static ref LATENCY_SLOT_COMMIT_TO_EXECUTE_MS: Histogram =
        register_histogram!(
            "latency_slot_commit_to_execute_ms",
            "Latency from slot commit to slot execute (ms)",
            LATENCY_BUCKETS_MS.to_vec()
        )
        .expect("failed to register latency_slot_commit_to_execute_ms");

    // ============================================================================
    // INTERNAL STATE: TIMESTAMP TRACKING FOR LATENCY COMPUTATION
    // ============================================================================

    /// Maps batch digest -> creation timestamp (ms) for tx lifecycle latency.
    pub static ref BATCH_CREATION_TIMESTAMPS: Mutex<HashMap<Digest, u64>> =
        Mutex::new(HashMap::new());

    /// Maps header digest -> creation timestamp (ms) for header lifecycle latency.
    pub static ref HEADER_CREATION_TIMESTAMPS: Mutex<HashMap<Digest, u64>> =
        Mutex::new(HashMap::new());

    /// Maps slot -> commit timestamp (ms) for commit-to-execute latency.
    pub static ref SLOT_COMMIT_TIMESTAMPS: Mutex<HashMap<u64, u64>> =
        Mutex::new(HashMap::new());
}

// ============================================================================
// LATENCY HELPERS
// ============================================================================

/// Returns current wall-clock time in milliseconds.
pub fn now_ms() -> u64 {
    chrono::Utc::now().timestamp_millis() as u64
}

/// Record a batch creation timestamp for later latency computation.
pub fn record_batch_created(digest: &Digest, timestamp_ms: u64) {
    BATCH_CREATION_TIMESTAMPS
        .lock()
        .unwrap()
        .insert(digest.clone(), timestamp_ms);
}

/// Record a header creation timestamp for later latency computation.
pub fn record_header_created(digest: &Digest, timestamp_ms: u64) {
    HEADER_CREATION_TIMESTAMPS
        .lock()
        .unwrap()
        .insert(digest.clone(), timestamp_ms);
}

/// Record a slot commit timestamp for later latency computation.
pub fn record_slot_committed(slot: u64, timestamp_ms: u64) {
    SLOT_COMMIT_TIMESTAMPS
        .lock()
        .unwrap()
        .insert(slot, timestamp_ms);
}

/// Observe tx-to-commit latency for batches in a committed slot.
pub fn observe_batches_committed(batch_digests: &[Digest], commit_timestamp_ms: u64) {
    let timestamps = BATCH_CREATION_TIMESTAMPS.lock().unwrap();
    for digest in batch_digests {
        if let Some(&created_ms) = timestamps.get(digest) {
            let latency = commit_timestamp_ms.saturating_sub(created_ms);
            LATENCY_TX_TO_SLOT_COMMIT_MS.observe(latency as f64);
        }
    }
}

/// Observe tx-to-execute latency for batches in an executed slot.
pub fn observe_batches_executed(batch_digests: &[Digest], execute_timestamp_ms: u64) {
    let timestamps = BATCH_CREATION_TIMESTAMPS.lock().unwrap();
    for digest in batch_digests {
        if let Some(&created_ms) = timestamps.get(digest) {
            let latency = execute_timestamp_ms.saturating_sub(created_ms);
            LATENCY_TX_TO_SLOT_EXECUTE_MS.observe(latency as f64);
        }
    }
}

/// Observe header-to-PoA latency for a header that just received PoA.
pub fn observe_header_poa(digest: &Digest, poa_timestamp_ms: u64) {
    if let Some(created_ms) = HEADER_CREATION_TIMESTAMPS.lock().unwrap().remove(digest) {
        let latency = poa_timestamp_ms.saturating_sub(created_ms);
        LATENCY_HEADER_TO_POA_MS.observe(latency as f64);
    }
}

/// Observe commit-to-execute latency for a slot that was just executed.
pub fn observe_slot_executed(slot: u64, execute_timestamp_ms: u64) {
    if let Some(commit_ms) = SLOT_COMMIT_TIMESTAMPS.lock().unwrap().remove(&slot) {
        let latency = execute_timestamp_ms.saturating_sub(commit_ms);
        LATENCY_SLOT_COMMIT_TO_EXECUTE_MS.observe(latency as f64);
    }
}

/// Remove batch timestamps older than `max_age_ms` to bound memory usage.
pub fn gc_batch_timestamps(max_age_ms: u64) {
    let cutoff = now_ms().saturating_sub(max_age_ms);
    BATCH_CREATION_TIMESTAMPS
        .lock()
        .unwrap()
        .retain(|_, &mut ts| ts > cutoff);
}

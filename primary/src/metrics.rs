use lazy_static::lazy_static;
use prometheus::{
    register_gauge, register_histogram, register_histogram_vec, register_int_counter,
    register_int_counter_vec, register_int_gauge, Gauge, Histogram, HistogramVec, IntCounter,
    IntCounterVec, IntGauge,
};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;
use crypto::Digest;

/// Data structure to accumulate throughput metrics during a flush interval.
/// Latency metrics are now tracked via Prometheus Histograms directly;
/// this struct only stores the previous histogram sum/count so we can
/// derive per-interval averages for the convenience gauges.
#[derive(Debug)]
pub struct FlushIntervalData {
    pub commit_count: u64,
    pub digest_count: u64,
    pub byte_count: u64,
    pub transaction_count: u64,

    // Previous histogram cumulative values (for computing per-interval averages)
    pub prev_observer_sum: f64,
    pub prev_observer_count: u64,
    pub prev_tx_to_commit_sum: f64,
    pub prev_tx_to_commit_count: u64,
}

impl FlushIntervalData {
    pub fn new() -> Self {
        Self {
            commit_count: 0,
            digest_count: 0,
            byte_count: 0,
            transaction_count: 0,
            prev_observer_sum: 0.0,
            prev_observer_count: 0,
            prev_tx_to_commit_sum: 0.0,
            prev_tx_to_commit_count: 0,
        }
    }

    pub fn reset_throughput(&mut self) {
        self.commit_count = 0;
        self.digest_count = 0;
        self.byte_count = 0;
        self.transaction_count = 0;
    }
}

lazy_static! {
    // ============================================================================
    // DISSEMINATION METRICS (data dissemination layer, f+1)
    // ============================================================================

    pub static ref DISSEMINATION_HEADERS_PROPOSED_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_headers_proposed_total",
            "Total number of headers proposed by this primary"
        )
        .expect("failed to register dissemination_headers_proposed_total");

    pub static ref DISSEMINATION_HEADERS_COMMITTED_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_headers_committed_total",
            "Total number of headers committed by this primary"
        )
        .expect("failed to register dissemination_headers_committed_total");

    pub static ref DISSEMINATION_DIGESTS_OWN_BATCHES_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_digests_own_batches_total",
            "Total number of own batch digests received from this primary's worker (locally created)"
        ).expect("failed to register dissemination_digests_own_batches_total");

    pub static ref DISSEMINATION_DIGESTS_OTHERS_BATCHES_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_digests_others_batches_total",
            "Total number of others' batch digests received from this primary's worker (from network)"
        ).expect("failed to register dissemination_digests_others_batches_total");

    pub static ref DISSEMINATION_CERTIFICATES_FORMED_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_certificates_formed_total",
            "Total number of dissemination certificates formed (f+1 votes on headers)"
        ).expect("failed to register dissemination_certificates_formed_total");

    pub static ref DISSEMINATION_HEADERS_BROADCAST_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_headers_broadcast_total",
            "Total number of headers broadcast to other primaries"
        ).expect("failed to register dissemination_headers_broadcast_total");

    pub static ref DISSEMINATION_HEADERS_VOTED_ON_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_headers_voted_on_total",
            "Total number of headers from other primaries that this node voted on"
        ).expect("failed to register dissemination_headers_voted_on_total");

    pub static ref DISSEMINATION_CERT_SYNC_REQUESTS_SENT_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_cert_sync_requests_sent_total",
            "Total number of certificate sync requests sent to other primaries"
        ).expect("failed to register dissemination_cert_sync_requests_sent_total");

    pub static ref DISSEMINATION_DAG_HEIGHT: IntGauge =
        register_int_gauge!(
            "dissemination_dag_height",
            "Height of the highest committed header in the DAG (last committed round)"
        )
        .expect("failed to register dissemination_dag_height");

    pub static ref DISSEMINATION_DAG_NODE_HEIGHT: IntGauge =
        register_int_gauge!(
            "dissemination_dag_node_height",
            "Height of this primary's own lane in the DAG (latest header proposed by this node)"
        ).expect("failed to register dissemination_dag_node_height");

    pub static ref DISSEMINATION_HEADER_SYNC_REQUESTS_SENT_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_header_sync_requests_sent_total",
            "Total number of header sync requests sent"
        ).expect("failed to register dissemination_header_sync_requests_sent_total");

    pub static ref DISSEMINATION_HEADER_SYNC_REQUESTS_RECEIVED_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_header_sync_requests_received_total",
            "Total number of header sync requests received"
        ).expect("failed to register dissemination_header_sync_requests_received_total");

    pub static ref DISSEMINATION_SYNC_RETRIES_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "dissemination_sync_retries_total",
            "Total number of sync retries triggered by recovery logic, by kind",
            &["kind"]
        ).expect("failed to register dissemination_sync_retries_total");

    pub static ref DISSEMINATION_INFLIGHT_HOLES: IntGauge =
        register_int_gauge!(
            "dissemination_inflight_holes",
            "Number of unique missing digests currently being recovered"
        ).expect("failed to register dissemination_inflight_holes");

    pub static ref DISSEMINATION_HOLE_DEPENDENTS: IntGauge =
        register_int_gauge!(
            "dissemination_hole_dependents",
            "Number of blocked waiters currently depending on hole recovery"
        ).expect("failed to register dissemination_hole_dependents");

    pub static ref DISSEMINATION_RECOVERED_HEADERS_TOTAL: IntCounter =
        register_int_counter!(
            "dissemination_recovered_headers_total",
            "Total number of headers delivered back into core after sync recovery"
        ).expect("failed to register dissemination_recovered_headers_total");

    pub static ref DISSEMINATION_RETRY_BUDGET_SKIPS_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "dissemination_retry_budget_skips_total",
            "Total number of eligible primary sync retries skipped because of the per-tick retry budget",
            &["kind"]
        ).expect("failed to register dissemination_retry_budget_skips_total");

    /// NEW: Time from header broadcast to f+1 certificate formation
    pub static ref DISSEMINATION_HEADER_TO_CERT_LATENCY: Histogram =
        register_histogram!(
            "dissemination_header_to_cert_latency_ms",
            "Time from header broadcast to f+1 certificate formation (ms)",
            vec![1.0, 2.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0]
        ).expect("failed to register dissemination_header_to_cert_latency_ms");

    // ============================================================================
    // CONSENSUS METRICS (slot consensus layer, 2f+1 BFT)
    // ============================================================================

    pub static ref CONSENSUS_PREPARE_MESSAGES_SENT_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_prepare_messages_sent_total",
            "Total number of Prepare messages sent by this primary (as leader)"
        ).expect("failed to register consensus_prepare_messages_sent_total");

    pub static ref CONSENSUS_PREPARE_VOTES_SENT_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_prepare_votes_sent_total",
            "Total number of Prepare votes (ConsensusVotes) sent for Prepare messages"
        ).expect("failed to register consensus_prepare_votes_sent_total");

    pub static ref CONSENSUS_CONFIRM_VOTES_SENT_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_confirm_votes_sent_total",
            "Total number of Confirm votes (ConsensusVotes) sent for Confirm messages"
        ).expect("failed to register consensus_confirm_votes_sent_total");

    pub static ref CONSENSUS_FAST_PATH_COMMITS_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_fast_path_commits_total",
            "Total number of commits via fast path (3f+1 votes)"
        ).expect("failed to register consensus_fast_path_commits_total");

    pub static ref CONSENSUS_SLOW_PATH_COMMITS_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_slow_path_commits_total",
            "Total number of commits via slow path (2f+1 Prepare + 2f+1 Confirm)"
        ).expect("failed to register consensus_slow_path_commits_total");

    pub static ref CONSENSUS_VIEW_CHANGES_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_view_changes_total",
            "Total number of successful view changes (timeouts with TC formed)"
        ).expect("failed to register consensus_view_changes_total");

    pub static ref CONSENSUS_LEADER_CHANGES_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_leader_changes_total",
            "Total number of leader changes"
        ).expect("failed to register consensus_leader_changes_total");

    pub static ref CONSENSUS_TIMEOUTS_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_timeouts_total",
            "Total number of timeouts fired"
        )
        .expect("failed to register consensus_timeouts_total");

    pub static ref CONSENSUS_TIMEOUTS_AS_LEADER_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_timeouts_as_leader_total",
            "Total number of timeouts occurred while being leader"
        )
        .expect("failed to register consensus_timeouts_as_leader_total");

    pub static ref CONSENSUS_VOTES_SENT_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "consensus_votes_sent_total",
            "Total number of votes sent by type",
            &["type"]
        ).expect("failed to register consensus_votes_sent_total");

    pub static ref CONSENSUS_VOTES_RECEIVED_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "consensus_votes_received_total",
            "Total number of votes received by type",
            &["type"]
        ).expect("failed to register consensus_votes_received_total");

    pub static ref CONSENSUS_VOTES_REFUSED_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "consensus_votes_refused_total",
            "Total number of votes refused by reason",
            &["reason"]
        ).expect("failed to register consensus_votes_refused_total");

    pub static ref CONSENSUS_SLOTS_EXECUTED_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_slots_executed_total",
            "Total number of consensus slots executed by the committer"
        )
        .expect("failed to register consensus_slots_executed_total");

    pub static ref CONSENSUS_PENDING_SLOTS: IntGauge =
        register_int_gauge!(
            "consensus_pending_slots",
            "Number of consensus slots waiting to be executed by the committer"
        )
        .expect("failed to register consensus_pending_slots");

    pub static ref CONSENSUS_SLOT_EXECUTION_LATENCY: Histogram =
        register_histogram!(
            "consensus_slot_execution_latency_ms",
            "Time taken to execute a single consensus slot (milliseconds)",
            vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0]
        )
        .expect("failed to register consensus_slot_execution_latency_ms");

    /// Commit latency with path label: "fast" or "slow"
    pub static ref CONSENSUS_COMMIT_LATENCY: HistogramVec =
        register_histogram_vec!(
            "consensus_commit_latency_ms",
            "Consensus commit latency: Prepare receive to Slot execute (ms), by path",
            &["path"],
            vec![5.0, 10.0, 25.0, 50.0, 75.0, 100.0, 150.0, 200.0, 300.0,
                 500.0, 750.0, 1000.0, 2500.0, 5000.0, 10000.0]
        ).expect("failed to register consensus_commit_latency_ms");

    pub static ref CONSENSUS_E2E_LATENCY: Histogram =
        register_histogram!(
            "consensus_e2e_latency_ms",
            "End-to-end latency: TX arrival at worker to Slot execute (ms)",
            vec![5.0, 10.0, 25.0, 50.0, 100.0, 150.0, 200.0, 300.0, 400.0,
                 500.0, 750.0, 1000.0, 2500.0, 5000.0, 10000.0]
        ).expect("failed to register consensus_e2e_latency_ms");

    /// NEW: Current view number gauge
    pub static ref CONSENSUS_CURRENT_VIEW: IntGauge =
        register_int_gauge!(
            "consensus_current_view",
            "Current consensus view number (steady = sync, jumps = async)"
        ).expect("failed to register consensus_current_view");

    /// NEW: Current slot number gauge
    pub static ref CONSENSUS_CURRENT_SLOT: IntGauge =
        register_int_gauge!(
            "consensus_current_slot",
            "Current consensus slot number (slope = throughput, stalls = async)"
        ).expect("failed to register consensus_current_slot");

    pub static ref CONSENSUS_OLDEST_BLOCKED_SLOT: IntGauge =
        register_int_gauge!(
            "consensus_oldest_blocked_slot",
            "Oldest slot currently blocked waiting on execution-time recovery"
        ).expect("failed to register consensus_oldest_blocked_slot");

    pub static ref CONSENSUS_COMMITTER_BLOCKED_TOTAL: IntCounter =
        register_int_counter!(
            "consensus_committer_blocked_total",
            "Total number of times the committer encountered missing execution data"
        ).expect("failed to register consensus_committer_blocked_total");

    pub static ref CONSENSUS_COMMITTER_WAITS_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "consensus_committer_waits_total",
            "Total number of committer waits by missing dependency kind",
            &["kind"]
        ).expect("failed to register consensus_committer_waits_total");

    pub static ref CONSENSUS_COMMITTER_SYNC_LATENCY: Histogram =
        register_histogram!(
            "consensus_committer_sync_latency_ms",
            "Time spent waiting for proposal headers and ancestors during execution (ms)",
            vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0]
        ).expect("failed to register consensus_committer_sync_latency_ms");

    pub static ref CONSENSUS_ACTIVE_NODES_IN_SLOT: IntGauge =
        register_int_gauge!(
            "consensus_active_nodes_in_slot",
            "Number of nodes that contributed to the most recently executed slot"
        ).expect("failed to register consensus_active_nodes_in_slot");

    pub static ref CONSENSUS_HEADERS_BY_NODE: IntCounterVec =
        register_int_counter_vec!(
            "consensus_headers_by_node",
            "Total number of headers committed per node (by author)",
            &["node"]
        ).expect("failed to register consensus_headers_by_node");

    pub static ref CONSENSUS_BYTES_BY_NODE: IntCounterVec =
        register_int_counter_vec!(
            "consensus_bytes_by_node",
            "Total bytes committed per node (by author)",
            &["node"]
        ).expect("failed to register consensus_bytes_by_node");

    pub static ref CONSENSUS_DIGESTS_BY_NODE: IntCounterVec =
        register_int_counter_vec!(
            "consensus_digests_by_node",
            "Total digests/batches committed per node (by author)",
            &["node"]
        ).expect("failed to register consensus_digests_by_node");

    // ============================================================================
    // THROUGHPUT METRICS (consensus output)
    // ============================================================================

    pub static ref CONSENSUS_THROUGHPUT_TOTAL_TRANSACTIONS: IntCounter =
        register_int_counter!(
            "consensus_throughput_total_transactions",
            "Cumulative total transactions committed (use rate() for throughput over arbitrary windows)"
        ).expect("failed to register consensus_throughput_total_transactions");

    pub static ref CONSENSUS_THROUGHPUT_TOTAL_BYTES: IntCounter =
        register_int_counter!(
            "consensus_throughput_total_bytes",
            "Cumulative total bytes committed (use rate() for throughput over arbitrary windows)"
        ).expect("failed to register consensus_throughput_total_bytes");

    pub static ref CONSENSUS_THROUGHPUT_TOTAL_DIGESTS: IntCounter =
        register_int_counter!(
            "consensus_throughput_total_digests",
            "Cumulative total batch digests committed (use rate() for throughput over arbitrary windows)"
        ).expect("failed to register consensus_throughput_total_digests");

    pub static ref CONSENSUS_FLUSH_COMMITS: Gauge =
        register_gauge!(
            "consensus_flush_commits",
            "Number of commits in the last flush interval"
        ).expect("failed to register consensus_flush_commits");

    pub static ref CONSENSUS_FLUSH_DIGESTS: Gauge =
        register_gauge!(
            "consensus_flush_digests",
            "Number of digests committed in the last flush interval"
        ).expect("failed to register consensus_flush_digests");

    pub static ref CONSENSUS_FLUSH_BYTES: Gauge =
        register_gauge!(
            "consensus_flush_bytes",
            "Total bytes committed in the last flush interval"
        ).expect("failed to register consensus_flush_bytes");

    pub static ref CONSENSUS_FLUSH_TRANSACTIONS: Gauge =
        register_gauge!(
            "consensus_flush_transactions",
            "Total transactions committed in the last flush interval"
        ).expect("failed to register consensus_flush_transactions");

    pub static ref CONSENSUS_COMMIT_RATE_PER_SECOND: Gauge =
        register_gauge!(
            "consensus_commit_rate_per_second",
            "Commits per second in the last flush interval"
        ).expect("failed to register consensus_commit_rate_per_second");

    pub static ref CONSENSUS_THROUGHPUT_BYTES_PER_SECOND: Gauge =
        register_gauge!(
            "consensus_throughput_bytes_per_second",
            "Bytes per second in the last flush interval"
        ).expect("failed to register consensus_throughput_bytes_per_second");

    pub static ref CONSENSUS_THROUGHPUT_TX_PER_SECOND: Gauge =
        register_gauge!(
            "consensus_throughput_tx_per_second",
            "Transactions per second in the last flush interval"
        ).expect("failed to register consensus_throughput_tx_per_second");

    // ============================================================================
    // LATENCY GAUGES - Flush Interval Averages
    // ============================================================================

    pub static ref CONSENSUS_COMMIT_LATENCY_AVG_MS: Gauge =
        register_gauge!(
            "consensus_commit_latency_avg_ms",
            "Average consensus commit latency: Prepare receive to Slot execute (ms)"
        ).expect("failed to register consensus_commit_latency_avg_ms");

    pub static ref CONSENSUS_E2E_LATENCY_AVG_MS: Gauge =
        register_gauge!(
            "consensus_e2e_latency_avg_ms",
            "Average end-to-end latency: TX arrival at worker to Slot execute (ms)"
        ).expect("failed to register consensus_e2e_latency_avg_ms");

    // ============================================================================
    // Internal State for Tracking
    // ============================================================================

    // Slot-level latency tracking
    static ref SLOT_PREPARE_TIMES: Mutex<HashMap<u64, Instant>> = Mutex::new(HashMap::new());

    // TX-level latency tracking
    static ref SUBMIT_MS_BY_BATCH: Mutex<HashMap<Digest, u64>> = Mutex::new(HashMap::new());

    // Batch metadata tracking
    static ref BATCH_SIZE_BYTES_BY_DIGEST: Mutex<HashMap<Digest, u64>> = Mutex::new(HashMap::new());
    static ref TX_COUNT_BY_DIGEST: Mutex<HashMap<Digest, u64>> = Mutex::new(HashMap::new());

    // Flush interval accumulator
    static ref FLUSH_INTERVAL_DATA: Mutex<FlushIntervalData> = Mutex::new(FlushIntervalData::new());
}

// ============================================================================
// Helper Functions - Latency Tracking
// ============================================================================

/// Record timestamp when observer receives Prepare message from leader
/// Only called when receiving Prepare from ANOTHER node (not self)
pub fn record_observer_prepare_receive(slot: u64) {
    let mut map = SLOT_PREPARE_TIMES.lock().unwrap();
    // Only record if not already recorded (avoid duplicate receives)
    map.entry(slot).or_insert(Instant::now());
}

/// Calculate observer latency for a slot execution (called from committer)
/// Measures: Prepare receive -> Slot execution (observer perspective only)
/// Records directly into Prometheus HistogramVec with path label.
pub fn observe_slot_latency(slot: u64, path: &str) {
    let mut map = SLOT_PREPARE_TIMES.lock().unwrap();
    if let Some(start_time) = map.remove(&slot) {
        let latency_ms = start_time.elapsed().as_millis() as f64;
        CONSENSUS_COMMIT_LATENCY.with_label_values(&[path]).observe(latency_ms);
    }
}

/// Record when a TX was submitted to worker (keep existing for TX latency)
pub fn record_tx_submit_ms(digest: &Digest, ts_ms: u64) {
    let mut map = SUBMIT_MS_BY_BATCH.lock().unwrap();
    map.insert(digest.clone(), ts_ms);
}

/// Calculate TX latency when batch is committed.
/// Records directly into Prometheus Histogram.
pub fn observe_tx_submit_to_commit_latency(digest: &Digest) {
    let mut map = SUBMIT_MS_BY_BATCH.lock().unwrap();
    if let Some(start_ms) = map.remove(digest) {
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        if now_ms >= start_ms {
            let latency_ms = (now_ms - start_ms) as f64;
            CONSENSUS_E2E_LATENCY.observe(latency_ms);
        }
    }
}

// ============================================================================
// Helper Functions - Batch Size Tracking
// ============================================================================

pub fn record_batch_size_bytes(digest: &Digest, bytes: u64) {
    let mut map = BATCH_SIZE_BYTES_BY_DIGEST.lock().unwrap();
    map.insert(digest.clone(), bytes);
}

pub fn take_batch_size_bytes(digest: &Digest) -> u64 {
    let mut map = BATCH_SIZE_BYTES_BY_DIGEST.lock().unwrap();
    map.remove(digest).unwrap_or(0)
}

pub fn record_tx_count(digest: &Digest, tx_count: u64) {
    let mut map = TX_COUNT_BY_DIGEST.lock().unwrap();
    map.insert(digest.clone(), tx_count);
}

pub fn take_tx_count(digest: &Digest) -> u64 {
    let mut map = TX_COUNT_BY_DIGEST.lock().unwrap();
    map.remove(digest).unwrap_or(0)
}

// ============================================================================
// Helper Functions - Flush Interval Tracking
// ============================================================================

pub fn record_flush_interval_commit(num_digests: usize, bytes: u64, transactions: u64) {
    // Update flush interval accumulator (reset every flush)
    let mut flush_data = FLUSH_INTERVAL_DATA.lock().unwrap();
    flush_data.commit_count += 1;
    flush_data.digest_count += num_digests as u64;
    flush_data.byte_count += bytes;
    flush_data.transaction_count += transactions;

    // Update cumulative counters (monotonically increasing, never reset)
    CONSENSUS_THROUGHPUT_TOTAL_TRANSACTIONS.inc_by(transactions);
    CONSENSUS_THROUGHPUT_TOTAL_BYTES.inc_by(bytes);
    CONSENSUS_THROUGHPUT_TOTAL_DIGESTS.inc_by(num_digests as u64);
}

/// Calculate rates and update flush interval metrics, then reset the accumulator.
/// Latency averages are derived from histogram cumulative sum/count deltas.
/// Called every flush_interval_ms (default 5000ms).
pub fn flush_interval_metrics(interval_ms: u64) {
    let mut flush_data = FLUSH_INTERVAL_DATA.lock().unwrap();

    // Update throughput counts
    CONSENSUS_FLUSH_COMMITS.set(flush_data.commit_count as f64);
    CONSENSUS_FLUSH_DIGESTS.set(flush_data.digest_count as f64);
    CONSENSUS_FLUSH_BYTES.set(flush_data.byte_count as f64);
    CONSENSUS_FLUSH_TRANSACTIONS.set(flush_data.transaction_count as f64);

    // Calculate rates (per second)
    let interval_sec = interval_ms as f64 / 1000.0;
    if interval_sec > 0.0 {
        CONSENSUS_COMMIT_RATE_PER_SECOND.set(flush_data.commit_count as f64 / interval_sec);
        CONSENSUS_THROUGHPUT_BYTES_PER_SECOND.set(flush_data.byte_count as f64 / interval_sec);
        CONSENSUS_THROUGHPUT_TX_PER_SECOND.set(flush_data.transaction_count as f64 / interval_sec);
    }

    // Derive per-interval average latencies from histogram cumulative sum/count deltas.
    // For CONSENSUS_COMMIT_LATENCY (HistogramVec), sum across both paths.
    let fast_metric = CONSENSUS_COMMIT_LATENCY.with_label_values(&["fast"]);
    let slow_metric = CONSENSUS_COMMIT_LATENCY.with_label_values(&["slow"]);
    let cur_observer_sum = fast_metric.get_sample_sum() + slow_metric.get_sample_sum();
    let cur_observer_count = fast_metric.get_sample_count() + slow_metric.get_sample_count();
    let delta_observer_sum = cur_observer_sum - flush_data.prev_observer_sum;
    let delta_observer_count = cur_observer_count - flush_data.prev_observer_count;
    let avg_observer = if delta_observer_count > 0 {
        delta_observer_sum / delta_observer_count as f64
    } else {
        0.0
    };
    flush_data.prev_observer_sum = cur_observer_sum;
    flush_data.prev_observer_count = cur_observer_count;

    let cur_tx_sum = CONSENSUS_E2E_LATENCY.get_sample_sum();
    let cur_tx_count = CONSENSUS_E2E_LATENCY.get_sample_count();
    let delta_tx_sum = cur_tx_sum - flush_data.prev_tx_to_commit_sum;
    let delta_tx_count = cur_tx_count - flush_data.prev_tx_to_commit_count;
    let avg_tx_to_commit = if delta_tx_count > 0 {
        delta_tx_sum / delta_tx_count as f64
    } else {
        0.0
    };
    flush_data.prev_tx_to_commit_sum = cur_tx_sum;
    flush_data.prev_tx_to_commit_count = cur_tx_count;

    CONSENSUS_COMMIT_LATENCY_AVG_MS.set(avg_observer);
    CONSENSUS_E2E_LATENCY_AVG_MS.set(avg_tx_to_commit);

    // Reset throughput accumulators for next interval
    flush_data.reset_throughput();
}

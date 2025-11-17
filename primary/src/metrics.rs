use lazy_static::lazy_static;
use prometheus::{
    register_gauge, register_histogram, register_histogram_vec, register_int_counter, register_int_counter_vec, register_int_gauge,
    Gauge, Histogram, HistogramVec, IntCounter, IntCounterVec, IntGauge
};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;
use crypto::Digest;

/// Data structure to accumulate metrics during a flush interval
#[derive(Debug)]
pub struct FlushIntervalData {
    pub commit_count: u64,
    pub digest_count: u64,
    pub byte_count: u64,
    pub transaction_count: u64,
    pub propose_to_commit_latencies_ms: Vec<f64>,
    pub batch_ingress_to_commit_latencies_ms: Vec<f64>,
    pub tx_submit_to_commit_latencies_ms: Vec<f64>,
    pub slot_propose_to_execute_latencies_ms: Vec<f64>,
}

impl FlushIntervalData {
    pub fn new() -> Self {
        Self {
            commit_count: 0,
            digest_count: 0,
            byte_count: 0,
            transaction_count: 0,
            propose_to_commit_latencies_ms: Vec::new(),
            batch_ingress_to_commit_latencies_ms: Vec::new(),
            tx_submit_to_commit_latencies_ms: Vec::new(),
            slot_propose_to_execute_latencies_ms: Vec::new(),
        }
    }

    pub fn reset(&mut self) {
        self.commit_count = 0;
        self.digest_count = 0;
        self.byte_count = 0;
        self.transaction_count = 0;
        self.propose_to_commit_latencies_ms.clear();
        self.batch_ingress_to_commit_latencies_ms.clear();
        self.tx_submit_to_commit_latencies_ms.clear();
        self.slot_propose_to_execute_latencies_ms.clear();
    }

    pub fn calculate_avg_latency(latencies: &[f64]) -> f64 {
        if latencies.is_empty() {
            0.0
        } else {
            latencies.iter().sum::<f64>() / latencies.len() as f64
        }
    }
}

lazy_static! {
    // ============================================================================
    // EXISTING COUNTERS
    // ============================================================================

    pub static ref PRIMARY_HEADERS_PROPOSED_TOTAL: IntCounter =
        register_int_counter!(
            "primary_headers_proposed_total",
            "Total number of headers proposed by this primary"
        )
        .expect("failed to register primary_headers_proposed_total");

    pub static ref PRIMARY_COMMITS_TOTAL: IntCounter =
        register_int_counter!(
            "primary_commits_total",
            "Total number of headers committed by this primary"
        )
        .expect("failed to register primary_commits_total");

    pub static ref PRIMARY_SLOTS_EXECUTED_TOTAL: IntCounter =
        register_int_counter!(
            "primary_slots_executed_total",
            "Total number of consensus slots executed by the committer"
        )
        .expect("failed to register primary_slots_executed_total");

    pub static ref PRIMARY_SLOT_EXECUTION_LATENCY: Histogram =
        register_histogram!(
            "primary_slot_execution_latency_ms",
            "Time taken to execute a single consensus slot (milliseconds)",
            vec![1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0]
        )
        .expect("failed to register primary_slot_execution_latency_ms");

    pub static ref PRIMARY_PENDING_SLOTS: IntGauge =
        register_int_gauge!(
            "primary_pending_slots",
            "Number of consensus slots waiting to be executed by the committer"
        )
        .expect("failed to register primary_pending_slots");

    pub static ref PRIMARY_DAG_HEIGHT: IntGauge =
        register_int_gauge!(
            "primary_dag_height",
            "Height of the highest committed header in the DAG (last committed round)"
        )
        .expect("failed to register primary_dag_height");

    pub static ref PRIMARY_TIMEOUTS_TOTAL: IntCounter =
        register_int_counter!(
            "primary_timeouts_total",
            "Total number of timeouts fired"
        )
        .expect("failed to register primary_timeouts_total");

    pub static ref PRIMARY_TIMEOUTS_AS_LEADER_TOTAL: IntCounter =
        register_int_counter!(
            "primary_timeouts_as_leader_total",
            "Total number of timeouts occurred while being leader"
        )
        .expect("failed to register primary_timeouts_as_leader_total");

    // ============================================================================
    // DAG-LEVEL METRICS (f+1 consensus for header dissemination)
    // ============================================================================

    pub static ref PRIMARY_DAG_DIGESTS_OWN_BATCHES_TOTAL: IntCounter =
        register_int_counter!(
            "primary_dag_digests_own_batches_total",
            "Total number of own batch digests received from this primary's worker (locally created)"
        ).expect("failed to register primary_dag_digests_own_batches_total");

    pub static ref PRIMARY_DAG_DIGESTS_OTHERS_BATCHES_TOTAL: IntCounter =
        register_int_counter!(
            "primary_dag_digests_others_batches_total",
            "Total number of others' batch digests received from this primary's worker (from network)"
        ).expect("failed to register primary_dag_digests_others_batches_total");

    pub static ref PRIMARY_DAG_CERTIFICATES_FORMED_TOTAL: IntCounter =
        register_int_counter!(
            "primary_dag_certificates_formed_total",
            "Total number of dissemination certificates formed (f+1 votes on headers)"
        ).expect("failed to register primary_dag_certificates_formed_total");

    pub static ref PRIMARY_DAG_HEADERS_BROADCAST_TOTAL: IntCounter =
        register_int_counter!(
            "primary_dag_headers_broadcast_total",
            "Total number of headers broadcast to other primaries"
        ).expect("failed to register primary_dag_headers_broadcast_total");

    pub static ref PRIMARY_DAG_HEADERS_VOTED_ON_TOTAL: IntCounter =
        register_int_counter!(
            "primary_dag_headers_voted_on_total",
            "Total number of headers from other primaries that this node voted on"
        ).expect("failed to register primary_dag_headers_voted_on_total");

    pub static ref PRIMARY_DAG_NODE_HEIGHT: IntGauge =
        register_int_gauge!(
            "primary_dag_node_height",
            "Height of this primary's own lane in the DAG (latest header proposed by this node)"
        ).expect("failed to register primary_dag_node_height");

    pub static ref PRIMARY_DAG_CERTIFICATE_SYNC_REQUESTS_SENT_TOTAL: IntCounter =
        register_int_counter!(
            "primary_dag_certificate_sync_requests_sent_total",
            "Total number of certificate sync requests sent to other primaries"
        ).expect("failed to register primary_dag_certificate_sync_requests_sent_total");

    // ============================================================================
    // SLOT CONSENSUS-LEVEL METRICS (2f+1 Byzantine consensus)
    // ============================================================================

    pub static ref PRIMARY_CONSENSUS_PREPARE_MESSAGES_SENT_TOTAL: IntCounter =
        register_int_counter!(
            "primary_consensus_prepare_messages_sent_total",
            "Total number of Prepare messages sent by this primary (as leader)"
        ).expect("failed to register primary_consensus_prepare_messages_sent_total");

    pub static ref PRIMARY_CONSENSUS_PREPARE_VOTES_SENT_TOTAL: IntCounter =
        register_int_counter!(
            "primary_consensus_prepare_votes_sent_total",
            "Total number of Prepare votes (ConsensusVotes) sent for Prepare messages"
        ).expect("failed to register primary_consensus_prepare_votes_sent_total");

    pub static ref PRIMARY_CONSENSUS_CONFIRM_VOTES_SENT_TOTAL: IntCounter =
        register_int_counter!(
            "primary_consensus_confirm_votes_sent_total",
            "Total number of Confirm votes (ConsensusVotes) sent for Confirm messages"
        ).expect("failed to register primary_consensus_confirm_votes_sent_total");

    pub static ref PRIMARY_CONSENSUS_COMMIT_VOTES_SENT_TOTAL: IntCounter =
        register_int_counter!(
            "primary_consensus_commit_votes_sent_total",
            "Total number of Commit votes (ConsensusVotes) sent for Commit messages"
        ).expect("failed to register primary_consensus_commit_votes_sent_total");

    // ============================================================================
    // SLOT CONSENSUS PATH TRACKING
    // ============================================================================

    // Fast/Slow Path Tracking
    pub static ref PRIMARY_CONSENSUS_FAST_PATH_COMMITS_TOTAL: IntCounter =
        register_int_counter!(
            "primary_consensus_fast_path_commits_total",
            "Total number of commits via fast path (3f+1 votes)"
        ).expect("failed to register primary_consensus_fast_path_commits_total");

    pub static ref PRIMARY_CONSENSUS_SLOW_PATH_COMMITS_TOTAL: IntCounter =
        register_int_counter!(
            "primary_consensus_slow_path_commits_total",
            "Total number of commits via slow path (2f+1 Prepare + 2f+1 Confirm)"
        ).expect("failed to register primary_consensus_slow_path_commits_total");

    // View Changes (Consensus-level)
    pub static ref PRIMARY_CONSENSUS_VIEW_CHANGES_TOTAL: IntCounter =
        register_int_counter!(
            "primary_consensus_view_changes_total",
            "Total number of successful view changes (timeouts with TC formed)"
        ).expect("failed to register primary_consensus_view_changes_total");

    pub static ref PRIMARY_CONSENSUS_LEADER_CHANGES_TOTAL: IntCounter =
        register_int_counter!(
            "primary_consensus_leader_changes_total",
            "Total number of leader changes"
        ).expect("failed to register primary_consensus_leader_changes_total");

    // Vote Tracking
    pub static ref PRIMARY_VOTES_SENT_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "primary_votes_sent_total",
            "Total number of votes sent by type",
            &["type"]
        ).expect("failed to register primary_votes_sent_total");

    pub static ref PRIMARY_VOTES_RECEIVED_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "primary_votes_received_total",
            "Total number of votes received by type",
            &["type"]
        ).expect("failed to register primary_votes_received_total");

    pub static ref PRIMARY_VOTES_REFUSED_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "primary_votes_refused_total",
            "Total number of votes refused by reason",
            &["reason"]
        ).expect("failed to register primary_votes_refused_total");

    // Synchronization
    pub static ref PRIMARY_HEADER_SYNC_REQUESTS_SENT_TOTAL: IntCounter =
        register_int_counter!(
            "primary_header_sync_requests_sent_total",
            "Total number of header sync requests sent"
        ).expect("failed to register primary_header_sync_requests_sent_total");

    pub static ref PRIMARY_HEADER_SYNC_REQUESTS_RECEIVED_TOTAL: IntCounter =
        register_int_counter!(
            "primary_header_sync_requests_received_total",
            "Total number of header sync requests received"
        ).expect("failed to register primary_header_sync_requests_received_total");

    pub static ref PRIMARY_HEADER_SYNC_FAILURES_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "primary_header_sync_failures_total",
            "Total number of header sync failures by reason",
            &["reason"]
        ).expect("failed to register primary_header_sync_failures_total");

    // ============================================================================
    // LATENCY METRICS - Now in Milliseconds
    // ============================================================================

    pub static ref PRIMARY_LATENCY_MS: HistogramVec =
        register_histogram_vec!(
            "primary_latency_ms",
            "Primary latencies by phase (milliseconds)",
            &["phase"],
            // Buckets from 1ms to 60000ms (60 seconds)
            vec![1.0, 2.0, 5.0, 10.0, 20.0, 50.0, 100.0, 200.0, 500.0,
                 1000.0, 2000.0, 5000.0, 10000.0, 20000.0, 60000.0]
        ).expect("failed to register primary_latency_ms");

    // ============================================================================
    // THROUGHPUT METRICS - Flush Interval Gauges
    // ============================================================================

    pub static ref PRIMARY_FLUSH_INTERVAL_THROUGHPUT_COMMITS: Gauge =
        register_gauge!(
            "primary_flush_interval_throughput_commits",
            "Number of commits in the last flush interval"
        ).expect("failed to register primary_flush_interval_throughput_commits");

    pub static ref PRIMARY_FLUSH_INTERVAL_THROUGHPUT_DIGESTS: Gauge =
        register_gauge!(
            "primary_flush_interval_throughput_digests",
            "Number of digests committed in the last flush interval"
        ).expect("failed to register primary_flush_interval_throughput_digests");

    pub static ref PRIMARY_FLUSH_INTERVAL_THROUGHPUT_BYTES: Gauge =
        register_gauge!(
            "primary_flush_interval_throughput_bytes",
            "Total bytes committed in the last flush interval"
        ).expect("failed to register primary_flush_interval_throughput_bytes");

    pub static ref PRIMARY_FLUSH_INTERVAL_THROUGHPUT_TRANSACTIONS: Gauge =
        register_gauge!(
            "primary_flush_interval_throughput_transactions",
            "Total transactions committed in the last flush interval"
        ).expect("failed to register primary_flush_interval_throughput_transactions");

    // Rates (computed)
    pub static ref PRIMARY_COMMIT_RATE_PER_SECOND: Gauge =
        register_gauge!(
            "primary_commit_rate_per_second",
            "Commits per second in the last flush interval"
        ).expect("failed to register primary_commit_rate_per_second");

    pub static ref PRIMARY_THROUGHPUT_BYTES_PER_SECOND: Gauge =
        register_gauge!(
            "primary_throughput_bytes_per_second",
            "Bytes per second in the last flush interval"
        ).expect("failed to register primary_throughput_bytes_per_second");

    pub static ref PRIMARY_THROUGHPUT_TX_PER_SECOND: Gauge =
        register_gauge!(
            "primary_throughput_tx_per_second",
            "Transactions per second in the last flush interval"
        ).expect("failed to register primary_throughput_tx_per_second");

    // ============================================================================
    // LATENCY GAUGES - Flush Interval Averages
    // ============================================================================

    pub static ref PRIMARY_FLUSH_INTERVAL_LATENCY_PROPOSE_TO_COMMIT_AVG_MS: Gauge =
        register_gauge!(
            "primary_flush_interval_latency_propose_to_commit_avg_ms",
            "Average propose-to-commit latency in the last flush interval (milliseconds)"
        ).expect("failed to register primary_flush_interval_latency_propose_to_commit_avg_ms");

    pub static ref PRIMARY_FLUSH_INTERVAL_LATENCY_BATCH_TO_COMMIT_AVG_MS: Gauge =
        register_gauge!(
            "primary_flush_interval_latency_batch_to_commit_avg_ms",
            "Average batch-ingress-to-commit latency in the last flush interval (milliseconds)"
        ).expect("failed to register primary_flush_interval_latency_batch_to_commit_avg_ms");

    pub static ref PRIMARY_FLUSH_INTERVAL_LATENCY_TX_TO_COMMIT_AVG_MS: Gauge =
        register_gauge!(
            "primary_flush_interval_latency_tx_to_commit_avg_ms",
            "Average tx-submit-to-commit latency in the last flush interval (milliseconds)"
        ).expect("failed to register primary_flush_interval_latency_tx_to_commit_avg_ms");

    pub static ref PRIMARY_FLUSH_INTERVAL_LATENCY_SLOT_PROPOSE_TO_EXECUTE_AVG_MS: Gauge =
        register_gauge!(
            "primary_flush_interval_latency_slot_propose_to_execute_avg_ms",
            "Average slot-propose-to-execute latency in the last flush interval (milliseconds)"
        ).expect("failed to register primary_flush_interval_latency_slot_propose_to_execute_avg_ms");

    // ============================================================================
    // PER-NODE SLOT CONTRIBUTION METRICS
    // ============================================================================

    pub static ref PRIMARY_SLOT_HEADERS_BY_NODE: IntCounterVec =
        register_int_counter_vec!(
            "primary_slot_headers_by_node",
            "Total number of headers committed per node (by author)",
            &["node"]
        ).expect("failed to register primary_slot_headers_by_node");

    pub static ref PRIMARY_SLOT_BYTES_BY_NODE: IntCounterVec =
        register_int_counter_vec!(
            "primary_slot_bytes_by_node",
            "Total bytes committed per node (by author)",
            &["node"]
        ).expect("failed to register primary_slot_bytes_by_node");

    pub static ref PRIMARY_SLOT_DIGESTS_BY_NODE: IntCounterVec =
        register_int_counter_vec!(
            "primary_slot_digests_by_node",
            "Total digests/batches committed per node (by author)",
            &["node"]
        ).expect("failed to register primary_slot_digests_by_node");

    pub static ref PRIMARY_ACTIVE_NODES_IN_SLOT: IntGauge =
        register_int_gauge!(
            "primary_active_nodes_in_slot",
            "Number of nodes that contributed to the most recently executed slot"
        ).expect("failed to register primary_active_nodes_in_slot");

    // ============================================================================
    // Internal State for Tracking
    // ============================================================================

    static ref PROPOSE_TIMES: Mutex<HashMap<Digest, Instant>> = Mutex::new(HashMap::new());
    static ref BATCH_ARRIVAL_TIMES: Mutex<HashMap<Digest, Instant>> = Mutex::new(HashMap::new());
    static ref SUBMIT_MS_BY_BATCH: Mutex<HashMap<Digest, u64>> = Mutex::new(HashMap::new());
    static ref BATCH_SIZE_BYTES_BY_DIGEST: Mutex<HashMap<Digest, u64>> = Mutex::new(HashMap::new());
    static ref FLUSH_INTERVAL_DATA: Mutex<FlushIntervalData> = Mutex::new(FlushIntervalData::new());
    static ref SLOT_PROPOSE_TIMES: Mutex<HashMap<u64, Instant>> = Mutex::new(HashMap::new());
}

// ============================================================================
// Helper Functions - Latency Tracking (Now in Milliseconds)
// ============================================================================

pub fn record_propose_time(header_id: &Digest) {
    let mut map = PROPOSE_TIMES.lock().unwrap();
    map.insert(header_id.clone(), Instant::now());
}

pub fn observe_propose_to_commit_latency(header_id: &Digest) {
    let mut map = PROPOSE_TIMES.lock().unwrap();
    if let Some(start) = map.remove(header_id) {
        let latency_ms = start.elapsed().as_millis() as f64;
        PRIMARY_LATENCY_MS.with_label_values(&["propose_to_commit"]).observe(latency_ms);

        // Also accumulate for flush interval
        let mut flush_data = FLUSH_INTERVAL_DATA.lock().unwrap();
        flush_data.propose_to_commit_latencies_ms.push(latency_ms);
    }
}

pub fn record_batch_arrival(digest: &Digest) {
    let mut map = BATCH_ARRIVAL_TIMES.lock().unwrap();
    map.insert(digest.clone(), Instant::now());
}

pub fn observe_batch_ingress_to_commit_latency(digest: &Digest) {
    let mut map = BATCH_ARRIVAL_TIMES.lock().unwrap();
    if let Some(start) = map.remove(digest) {
        let latency_ms = start.elapsed().as_millis() as f64;
        PRIMARY_LATENCY_MS.with_label_values(&["batch_ingress_to_commit"]).observe(latency_ms);

        let mut flush_data = FLUSH_INTERVAL_DATA.lock().unwrap();
        flush_data.batch_ingress_to_commit_latencies_ms.push(latency_ms);
    }
}

pub fn record_tx_submit_ms(digest: &Digest, ts_ms: u64) {
    let mut map = SUBMIT_MS_BY_BATCH.lock().unwrap();
    map.insert(digest.clone(), ts_ms);
}

pub fn observe_tx_submit_to_commit_latency(digest: &Digest) {
    let mut map = SUBMIT_MS_BY_BATCH.lock().unwrap();
    if let Some(start_ms) = map.remove(digest) {
        let now_ms = chrono::Utc::now().timestamp_millis() as u64;
        if now_ms >= start_ms {
            let latency_ms = (now_ms - start_ms) as f64;
            PRIMARY_LATENCY_MS.with_label_values(&["tx_submit_to_commit"]).observe(latency_ms);

            let mut flush_data = FLUSH_INTERVAL_DATA.lock().unwrap();
            flush_data.tx_submit_to_commit_latencies_ms.push(latency_ms);
        }
    }
}

pub fn record_slot_propose_time(slot: u64) {
    let mut map = SLOT_PROPOSE_TIMES.lock().unwrap();
    map.insert(slot, Instant::now());
}

pub fn observe_slot_propose_to_execute_latency(slot: u64) {
    let mut map = SLOT_PROPOSE_TIMES.lock().unwrap();
    if let Some(start) = map.remove(&slot) {
        let latency_ms = start.elapsed().as_millis() as f64;
        PRIMARY_LATENCY_MS.with_label_values(&["slot_propose_to_execute"]).observe(latency_ms);

        // Also accumulate for flush interval
        let mut flush_data = FLUSH_INTERVAL_DATA.lock().unwrap();
        flush_data.slot_propose_to_execute_latencies_ms.push(latency_ms);
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

// ============================================================================
// Helper Functions - Flush Interval Tracking
// ============================================================================

pub fn record_flush_interval_commit(num_digests: usize, bytes: u64) {
    let mut flush_data = FLUSH_INTERVAL_DATA.lock().unwrap();
    flush_data.commit_count += 1;
    flush_data.digest_count += num_digests as u64;
    flush_data.byte_count += bytes;
    // Note: transaction_count would need to be passed in or computed separately
}

/// Calculate rates and update flush interval metrics, then reset the accumulator
/// Called every flush_interval_ms (default 5000ms)
pub fn flush_interval_metrics(interval_ms: u64) {
    let mut flush_data = FLUSH_INTERVAL_DATA.lock().unwrap();

    // Update throughput counts
    PRIMARY_FLUSH_INTERVAL_THROUGHPUT_COMMITS.set(flush_data.commit_count as f64);
    PRIMARY_FLUSH_INTERVAL_THROUGHPUT_DIGESTS.set(flush_data.digest_count as f64);
    PRIMARY_FLUSH_INTERVAL_THROUGHPUT_BYTES.set(flush_data.byte_count as f64);
    PRIMARY_FLUSH_INTERVAL_THROUGHPUT_TRANSACTIONS.set(flush_data.transaction_count as f64);

    // Calculate rates (per second)
    let interval_sec = interval_ms as f64 / 1000.0;
    if interval_sec > 0.0 {
        PRIMARY_COMMIT_RATE_PER_SECOND.set(flush_data.commit_count as f64 / interval_sec);
        PRIMARY_THROUGHPUT_BYTES_PER_SECOND.set(flush_data.byte_count as f64 / interval_sec);
        PRIMARY_THROUGHPUT_TX_PER_SECOND.set(flush_data.transaction_count as f64 / interval_sec);
    }

    // Calculate and update average latencies
    let avg_propose_to_commit = FlushIntervalData::calculate_avg_latency(&flush_data.propose_to_commit_latencies_ms);
    let avg_batch_to_commit = FlushIntervalData::calculate_avg_latency(&flush_data.batch_ingress_to_commit_latencies_ms);
    let avg_tx_to_commit = FlushIntervalData::calculate_avg_latency(&flush_data.tx_submit_to_commit_latencies_ms);
    let avg_slot_latency = FlushIntervalData::calculate_avg_latency(&flush_data.slot_propose_to_execute_latencies_ms);

    PRIMARY_FLUSH_INTERVAL_LATENCY_PROPOSE_TO_COMMIT_AVG_MS.set(avg_propose_to_commit);
    PRIMARY_FLUSH_INTERVAL_LATENCY_BATCH_TO_COMMIT_AVG_MS.set(avg_batch_to_commit);
    PRIMARY_FLUSH_INTERVAL_LATENCY_TX_TO_COMMIT_AVG_MS.set(avg_tx_to_commit);
    PRIMARY_FLUSH_INTERVAL_LATENCY_SLOT_PROPOSE_TO_EXECUTE_AVG_MS.set(avg_slot_latency);

    // Reset for next interval
    flush_data.reset();
}






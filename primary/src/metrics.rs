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
    // NEW METRICS - Consensus
    // ============================================================================

    // Fast/Slow Path Tracking
    pub static ref PRIMARY_FAST_PATH_COMMITS_TOTAL: IntCounter =
        register_int_counter!(
            "primary_fast_path_commits_total",
            "Total number of commits via fast path"
        ).expect("failed to register primary_fast_path_commits_total");

    pub static ref PRIMARY_SLOW_PATH_COMMITS_TOTAL: IntCounter =
        register_int_counter!(
            "primary_slow_path_commits_total",
            "Total number of commits via slow path"
        ).expect("failed to register primary_slow_path_commits_total");

    // View Changes
    pub static ref PRIMARY_VIEW_CHANGES_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "primary_view_changes_total",
            "Total number of view changes per slot",
            &["slot"]
        ).expect("failed to register primary_view_changes_total");

    pub static ref PRIMARY_LEADER_CHANGES_TOTAL: IntCounter =
        register_int_counter!(
            "primary_leader_changes_total",
            "Total number of leader changes"
        ).expect("failed to register primary_leader_changes_total");

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
    // Internal State for Tracking
    // ============================================================================

    static ref PROPOSE_TIMES: Mutex<HashMap<Digest, Instant>> = Mutex::new(HashMap::new());
    static ref BATCH_ARRIVAL_TIMES: Mutex<HashMap<Digest, Instant>> = Mutex::new(HashMap::new());
    static ref SUBMIT_MS_BY_BATCH: Mutex<HashMap<Digest, u64>> = Mutex::new(HashMap::new());
    static ref BATCH_SIZE_BYTES_BY_DIGEST: Mutex<HashMap<Digest, u64>> = Mutex::new(HashMap::new());
    static ref FLUSH_INTERVAL_DATA: Mutex<FlushIntervalData> = Mutex::new(FlushIntervalData::new());
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

    // Reset for next interval
    flush_data.reset();
}






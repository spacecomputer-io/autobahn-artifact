use lazy_static::lazy_static;
use prometheus::{register_gauge, register_histogram, register_int_counter, register_int_gauge, register_histogram_vec, Gauge, Histogram, HistogramVec, IntCounter, IntGauge};
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
    pub propose_to_commit_latencies: Vec<f64>,
    pub batch_ingress_to_commit_latencies: Vec<f64>,
    pub tx_submit_to_commit_latencies: Vec<f64>,
}

impl FlushIntervalData {
    pub fn new() -> Self {
        Self {
            commit_count: 0,
            digest_count: 0,
            byte_count: 0,
            propose_to_commit_latencies: Vec::new(),
            batch_ingress_to_commit_latencies: Vec::new(),
            tx_submit_to_commit_latencies: Vec::new(),
        }
    }
    
    pub fn reset(&mut self) {
        self.commit_count = 0;
        self.digest_count = 0;
        self.byte_count = 0;
        self.propose_to_commit_latencies.clear();
        self.batch_ingress_to_commit_latencies.clear();
        self.tx_submit_to_commit_latencies.clear();
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

    pub static ref PRIMARY_LAST_COMMITTED_HEIGHT: IntGauge =
        register_int_gauge!(
            "primary_last_committed_height",
            "Latest committed height"
        )
        .expect("failed to register primary_last_committed_height");

    pub static ref PRIMARY_CURRENT_HEIGHT: IntGauge =
        register_int_gauge!(
            "primary_current_height",
            "Current proposer height"
        )
        .expect("failed to register primary_current_height");

    pub static ref PRIMARY_LAST_DECIDED_VIEW: IntGauge =
        register_int_gauge!(
            "primary_last_decided_view",
            "Last decided view recorded on commit"
        )
        .expect("failed to register primary_last_decided_view");

    pub static ref PRIMARY_LAST_DECIDED_TIME_SECONDS: Gauge =
        register_gauge!(
            "primary_last_decided_time_seconds",
            "Unix timestamp of last commit"
        )
        .expect("failed to register primary_last_decided_time_seconds");

    pub static ref PRIMARY_VIEWS_PER_DECIDE: Histogram =
        register_histogram!(
            "primary_views_per_decide",
            "Number of views progressed per decide event"
        )
        .expect("failed to register primary_views_per_decide");

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

    pub static ref PRIMARY_LATENCY_SECONDS: HistogramVec =
        register_histogram_vec!(
            "primary_latency_seconds",
            "Primary latencies by phase (seconds)",
            &["phase"],
            // quasi-log buckets from 1ms to 60s
            vec![0.001, 0.002, 0.005, 0.01, 0.02, 0.05, 0.1, 0.2, 0.5, 1.0, 2.0, 5.0, 10.0, 20.0, 60.0]
        )
        .expect("failed to register primary_latency_seconds");

    pub static ref PRIMARY_NUM_DIGESTS_PER_HEADER: Histogram =
        register_histogram!(
            "primary_num_digests_per_header",
            "Number of worker digests included per proposed header"
        )
        .expect("failed to register primary_num_digests_per_header");

    static ref PROPOSE_TIMES: Mutex<HashMap<Digest, Instant>> = Mutex::new(HashMap::new());
    static ref BATCH_ARRIVAL_TIMES: Mutex<HashMap<Digest, Instant>> = Mutex::new(HashMap::new());
    static ref SUBMIT_MS_BY_BATCH: Mutex<HashMap<Digest, u64>> = Mutex::new(HashMap::new());
    static ref BATCH_SIZE_BYTES_BY_DIGEST: Mutex<HashMap<Digest, u64>> = Mutex::new(HashMap::new());
    static ref LAST_DECIDED_VIEW_TRACKER: Mutex<Option<u64>> = Mutex::new(None);
    pub static ref PRIMARY_LATEST_HEADER_NUM_DIGESTS: IntGauge = register_int_gauge!(
        "primary_latest_header_num_digests",
        "Number of digests included in the most recent proposed header"
    ).expect("failed to register primary_latest_header_num_digests");
    pub static ref PRIMARY_COMMITTED_DIGESTS_PER_SEC: Gauge = register_gauge!(
        "primary_committed_digests_per_sec",
        "Approximate moving average of committed digests per second over a short window"
    ).expect("failed to register primary_committed_digests_per_sec");
    pub static ref PRIMARY_COMMITTED_BYTES_PER_SEC: Gauge = register_gauge!(
        "primary_committed_bytes_per_sec",
        "Approximate moving average of committed payload bytes per second over a short window"
    ).expect("failed to register primary_committed_bytes_per_sec");
    pub static ref PRIMARY_LAST_COMMITTED_BYTES: IntGauge = register_int_gauge!(
        "primary_last_committed_bytes",
        "Total payload bytes in the most recently committed header"
    ).expect("failed to register primary_last_committed_bytes");
    static ref COMMIT_WINDOW: Mutex<Vec<(Instant, usize)>> = Mutex::new(Vec::new());
    static ref COMMIT_BYTES_WINDOW: Mutex<Vec<(Instant, u64)>> = Mutex::new(Vec::new());

    // Per-flush interval metrics
    pub static ref PRIMARY_FLUSH_INTERVAL_THROUGHPUT_DIGESTS: Gauge = register_gauge!(
        "primary_flush_interval_throughput_digests",
        "Number of digests committed during the last flush interval"
    ).expect("failed to register primary_flush_interval_throughput_digests");
    
    pub static ref PRIMARY_FLUSH_INTERVAL_THROUGHPUT_BYTES: Gauge = register_gauge!(
        "primary_flush_interval_throughput_bytes", 
        "Total bytes committed during the last flush interval"
    ).expect("failed to register primary_flush_interval_throughput_bytes");
    
    pub static ref PRIMARY_FLUSH_INTERVAL_THROUGHPUT_COMMITS: Gauge = register_gauge!(
        "primary_flush_interval_throughput_commits",
        "Number of headers committed during the last flush interval"
    ).expect("failed to register primary_flush_interval_throughput_commits");
    
    pub static ref PRIMARY_FLUSH_INTERVAL_LATENCY_PROPOSE_TO_COMMIT_AVG: Gauge = register_gauge!(
        "primary_flush_interval_latency_propose_to_commit_avg_seconds",
        "Average propose-to-commit latency during the last flush interval"
    ).expect("failed to register primary_flush_interval_latency_propose_to_commit_avg");
    
    pub static ref PRIMARY_FLUSH_INTERVAL_LATENCY_BATCH_INGRESS_TO_COMMIT_AVG: Gauge = register_gauge!(
        "primary_flush_interval_latency_batch_ingress_to_commit_avg_seconds",
        "Average batch-ingress-to-commit latency during the last flush interval"
    ).expect("failed to register primary_flush_interval_latency_batch_ingress_to_commit_avg");
    
    pub static ref PRIMARY_FLUSH_INTERVAL_LATENCY_TX_SUBMIT_TO_COMMIT_AVG: Gauge = register_gauge!(
        "primary_flush_interval_latency_tx_submit_to_commit_avg_seconds",
        "Average tx-submit-to-commit latency during the last flush interval"
    ).expect("failed to register primary_flush_interval_latency_tx_submit_to_commit_avg");

    // Storage for accumulating flush interval data
    static ref FLUSH_INTERVAL_DATA: Mutex<FlushIntervalData> = Mutex::new(FlushIntervalData::new());
}

pub fn record_propose_time(header_id: &Digest) {
    let mut map = PROPOSE_TIMES.lock().unwrap();
    map.insert(header_id.clone(), Instant::now());
}

pub fn observe_propose_to_commit_latency(header_id: &Digest) {
    let mut map = PROPOSE_TIMES.lock().unwrap();
    if let Some(start) = map.remove(header_id) {
        let secs = start.elapsed().as_secs_f64();
        PRIMARY_LATENCY_SECONDS.with_label_values(&["propose_to_commit"]).observe(secs);
        
        // Also accumulate for flush interval metrics
        record_flush_interval_propose_to_commit_latency(secs);
    }
}

pub fn update_last_decided_view(view: u64) {
    let mut last = LAST_DECIDED_VIEW_TRACKER.lock().unwrap();
    if let Some(prev) = *last {
        let delta = if view >= prev { view - prev } else { 0 } as f64;
        PRIMARY_VIEWS_PER_DECIDE.observe(delta);
    }
    *last = Some(view);
    PRIMARY_LAST_DECIDED_VIEW.set(view as i64);
}

pub fn observe_header_num_digests(num_digests: usize) {
    PRIMARY_NUM_DIGESTS_PER_HEADER.observe(num_digests as f64);
    PRIMARY_LATEST_HEADER_NUM_DIGESTS.set(num_digests as i64);
}

pub fn observe_commit_digests_count(num_digests: usize) {
    const WINDOW_SECS: f64 = 30.0;
    let now = Instant::now();
    let mut w = COMMIT_WINDOW.lock().unwrap();
    w.push((now, num_digests));
    // Prune old entries
    let cutoff = now - std::time::Duration::from_secs(WINDOW_SECS as u64);
    w.retain(|(t, _)| *t >= cutoff);
    let total: usize = w.iter().map(|(_, c)| *c).sum();
    let secs = if w.is_empty() { 1.0 } else { WINDOW_SECS };
    PRIMARY_COMMITTED_DIGESTS_PER_SEC.set(total as f64 / secs);
}

pub fn observe_commit_bytes(bytes: u64) {
    const WINDOW_SECS: f64 = 30.0;
    let now = Instant::now();
    let mut w = COMMIT_BYTES_WINDOW.lock().unwrap();
    w.push((now, bytes));
    // Prune old entries
    let cutoff = now - std::time::Duration::from_secs(WINDOW_SECS as u64);
    w.retain(|(t, _)| *t >= cutoff);
    let total: u64 = w.iter().map(|(_, c)| *c).sum();
    let secs = if w.is_empty() { 1.0 } else { WINDOW_SECS };
    PRIMARY_COMMITTED_BYTES_PER_SEC.set(total as f64 / secs);
    PRIMARY_LAST_COMMITTED_BYTES.set(bytes as i64);
}

pub fn record_batch_size_bytes(digest: &Digest, bytes: u64) {
    let mut map = BATCH_SIZE_BYTES_BY_DIGEST.lock().unwrap();
    map.insert(digest.clone(), bytes);
}

pub fn take_batch_size_bytes(digest: &Digest) -> u64 {
    let mut map = BATCH_SIZE_BYTES_BY_DIGEST.lock().unwrap();
    map.remove(digest).unwrap_or(0)
}

pub fn record_batch_arrival(digest: &Digest) {
    let mut map = BATCH_ARRIVAL_TIMES.lock().unwrap();
    map.insert(digest.clone(), Instant::now());
}

pub fn observe_batch_ingress_to_commit_latency(digest: &Digest) {
    let mut map = BATCH_ARRIVAL_TIMES.lock().unwrap();
    if let Some(start) = map.remove(digest) {
        let secs = start.elapsed().as_secs_f64();
        PRIMARY_LATENCY_SECONDS.with_label_values(&["batch_ingress_to_commit"]).observe(secs);
        
        // Also accumulate for flush interval metrics
        record_flush_interval_batch_ingress_to_commit_latency(secs);
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
            let delta_secs = (now_ms - start_ms) as f64 / 1000.0;
            PRIMARY_LATENCY_SECONDS.with_label_values(&["tx_submit_to_commit"]).observe(delta_secs);
            
            // Also accumulate for flush interval metrics
            let mut flush_data = FLUSH_INTERVAL_DATA.lock().unwrap();
            flush_data.tx_submit_to_commit_latencies.push(delta_secs);
        }
    }
}

/// Record a commit event for flush interval metrics
pub fn record_flush_interval_commit(num_digests: usize, bytes: u64) {
    let mut flush_data = FLUSH_INTERVAL_DATA.lock().unwrap();
    flush_data.commit_count += 1;
    flush_data.digest_count += num_digests as u64;
    flush_data.byte_count += bytes;
}

/// Record a propose-to-commit latency for flush interval metrics
pub fn record_flush_interval_propose_to_commit_latency(latency_secs: f64) {
    let mut flush_data = FLUSH_INTERVAL_DATA.lock().unwrap();
    flush_data.propose_to_commit_latencies.push(latency_secs);
}

/// Record a batch-ingress-to-commit latency for flush interval metrics
pub fn record_flush_interval_batch_ingress_to_commit_latency(latency_secs: f64) {
    let mut flush_data = FLUSH_INTERVAL_DATA.lock().unwrap();
    flush_data.batch_ingress_to_commit_latencies.push(latency_secs);
}

/// Calculate averages and update flush interval metrics, then reset the accumulator
pub fn flush_interval_metrics() {
    let mut flush_data = FLUSH_INTERVAL_DATA.lock().unwrap();
    
    // Update throughput metrics
    PRIMARY_FLUSH_INTERVAL_THROUGHPUT_COMMITS.set(flush_data.commit_count as f64);
    PRIMARY_FLUSH_INTERVAL_THROUGHPUT_DIGESTS.set(flush_data.digest_count as f64);
    PRIMARY_FLUSH_INTERVAL_THROUGHPUT_BYTES.set(flush_data.byte_count as f64);
    
    // Calculate and update average latency metrics
    let avg_propose_to_commit = FlushIntervalData::calculate_avg_latency(&flush_data.propose_to_commit_latencies);
    let avg_batch_ingress_to_commit = FlushIntervalData::calculate_avg_latency(&flush_data.batch_ingress_to_commit_latencies);
    let avg_tx_submit_to_commit = FlushIntervalData::calculate_avg_latency(&flush_data.tx_submit_to_commit_latencies);
    
    PRIMARY_FLUSH_INTERVAL_LATENCY_PROPOSE_TO_COMMIT_AVG.set(avg_propose_to_commit);
    PRIMARY_FLUSH_INTERVAL_LATENCY_BATCH_INGRESS_TO_COMMIT_AVG.set(avg_batch_ingress_to_commit);
    PRIMARY_FLUSH_INTERVAL_LATENCY_TX_SUBMIT_TO_COMMIT_AVG.set(avg_tx_submit_to_commit);
    
    // Reset for next flush interval
    flush_data.reset();
}



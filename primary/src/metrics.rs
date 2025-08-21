use lazy_static::lazy_static;
use prometheus::{register_gauge, register_histogram, register_int_counter, register_int_gauge, Gauge, Histogram, IntCounter, IntGauge};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Instant;
use crypto::Digest;

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

    pub static ref PRIMARY_PROPOSE_TO_COMMIT_LATENCY_MS: Histogram =
        register_histogram!(
            "primary_propose_to_commit_latency_ms",
            "Latency from header proposal to commit in milliseconds",
            vec![1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0]
        )
        .expect("failed to register primary_propose_to_commit_latency_ms");
    pub static ref PRIMARY_BATCH_INGRESS_TO_COMMIT_LATENCY_MS: Histogram =
        register_histogram!(
            "primary_batch_ingress_to_commit_latency_ms",
            "Latency from batch digest arrival at primary to commit in milliseconds",
            vec![1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0]
        )
        .expect("failed to register primary_batch_ingress_to_commit_latency_ms");
    pub static ref PRIMARY_TX_SUBMIT_TO_COMMIT_LATENCY_MS: Histogram =
        register_histogram!(
            "primary_tx_submit_to_commit_latency_ms",
            "Latency from first tx submission in a batch (reported by worker) to commit in milliseconds",
            vec![1.0, 2.5, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1_000.0, 2_500.0, 5_000.0]
        )
        .expect("failed to register primary_tx_submit_to_commit_latency_ms");

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
}

pub fn record_propose_time(header_id: &Digest) {
    let mut map = PROPOSE_TIMES.lock().unwrap();
    map.insert(header_id.clone(), Instant::now());
}

pub fn observe_propose_to_commit_latency(header_id: &Digest) {
    let mut map = PROPOSE_TIMES.lock().unwrap();
    if let Some(start) = map.remove(header_id) {
        let ms = start.elapsed().as_millis() as f64;
        PRIMARY_PROPOSE_TO_COMMIT_LATENCY_MS.observe(ms);
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
        let ms = start.elapsed().as_millis() as f64;
        PRIMARY_BATCH_INGRESS_TO_COMMIT_LATENCY_MS.observe(ms);
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
            let delta = now_ms - start_ms;
            PRIMARY_TX_SUBMIT_TO_COMMIT_LATENCY_MS.observe(delta as f64);
        }
    }
}



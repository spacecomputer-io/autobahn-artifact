use lazy_static::lazy_static;
use prometheus::{
    register_histogram_vec, register_int_counter, register_int_counter_vec,
    register_int_gauge, register_int_gauge_vec, HistogramVec, IntCounter, IntCounterVec,
    IntGauge, IntGaugeVec,
};

lazy_static! {
    pub static ref WORKER_BATCHES_SEALED_TOTAL: IntCounter =
        register_int_counter!(
            "worker_batches_sealed_total",
            "Total number of batches sealed by this worker"
        )
        .expect("failed to register worker_batches_sealed_total");

    pub static ref WORKER_DIGESTS_SENT_TO_PRIMARY_TOTAL: IntCounter =
        register_int_counter!(
            "worker_digests_sent_to_primary_total",
            "Total number of batch digests sent to the primary"
        )
        .expect("failed to register worker_digests_sent_to_primary_total");

    pub static ref WORKER_TRANSACTIONS_RECEIVED_TOTAL: IntCounter =
        register_int_counter!(
            "worker_transactions_received_total",
            "Total number of client transactions received"
        )
        .expect("failed to register worker_transactions_received_total");

    pub static ref WORKER_BATCH_SIZE_BYTES: IntGauge =
        register_int_gauge!(
            "worker_batch_size_bytes",
            "Size in bytes of the last sealed batch"
        )
        .expect("failed to register worker_batch_size_bytes");

    pub static ref WORKER_SYNC_PENDING_BATCHES: IntGaugeVec =
        register_int_gauge_vec!(
            "worker_sync_pending_batches",
            "Current number of pending batch syncs by priority",
            &["priority"]
        )
        .expect("failed to register worker_sync_pending_batches");

    pub static ref WORKER_SYNC_PENDING_DEPENDENTS: IntGaugeVec =
        register_int_gauge_vec!(
            "worker_sync_pending_dependents",
            "Current number of dependent waiters attached to pending batch syncs by priority",
            &["priority"]
        )
        .expect("failed to register worker_sync_pending_dependents");

    pub static ref WORKER_SYNC_RETRIES_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "worker_sync_retries_total",
            "Total number of worker batch sync retries by priority",
            &["priority"]
        )
        .expect("failed to register worker_sync_retries_total");

    pub static ref WORKER_SYNC_REQUESTS_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "worker_sync_requests_total",
            "Total number of worker batch sync requests sent by priority and phase",
            &["priority", "phase"]
        )
        .expect("failed to register worker_sync_requests_total");

    pub static ref WORKER_SYNC_COMPLETIONS_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "worker_sync_completions_total",
            "Total number of worker batch sync completions by priority",
            &["priority"]
        )
        .expect("failed to register worker_sync_completions_total");

    pub static ref WORKER_SYNC_RECOVERY_LATENCY_MS: HistogramVec =
        register_histogram_vec!(
            "worker_sync_recovery_latency_ms",
            "Latency from the first batch sync request until local recovery",
            &["priority"],
            vec![5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0]
        )
        .expect("failed to register worker_sync_recovery_latency_ms");

    pub static ref WORKER_SYNC_TARGET_ROTATIONS_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "worker_sync_target_rotations_total",
            "Total number of retry target rotations by priority",
            &["priority"]
        )
        .expect("failed to register worker_sync_target_rotations_total");

    pub static ref WORKER_SYNC_CHANNEL_BACKPRESSURE_TOTAL: IntCounter =
        register_int_counter!(
            "worker_sync_channel_backpressure_total",
            "Total number of times the worker synchronizer channel was observed full"
        )
        .expect("failed to register worker_sync_channel_backpressure_total");

    pub static ref WORKER_SYNC_GC_EVICTIONS_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "worker_sync_gc_evictions_total",
            "Total number of pending worker batch syncs evicted by round-based GC",
            &["priority"]
        )
        .expect("failed to register worker_sync_gc_evictions_total");

    pub static ref WORKER_SYNC_TARGET_COOLDOWNS_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "worker_sync_target_cooldowns_total",
            "Total number of times a target was temporarily cooled down for worker batch sync retries",
            &["priority"]
        )
        .expect("failed to register worker_sync_target_cooldowns_total");

    pub static ref WORKER_SYNC_STALLED_BATCHES: IntGaugeVec =
        register_int_gauge_vec!(
            "worker_sync_stalled_batches",
            "Current number of pending batch syncs older than the stalled threshold by priority",
            &["priority"]
        )
        .expect("failed to register worker_sync_stalled_batches");

    pub static ref WORKER_SYNC_OLDEST_BLOCKED_HEIGHT: IntGaugeVec =
        register_int_gauge_vec!(
            "worker_sync_oldest_blocked_height",
            "Oldest blocked height among pending batch syncs by priority",
            &["priority"]
        )
        .expect("failed to register worker_sync_oldest_blocked_height");

    pub static ref WORKER_SYNC_RETRY_BUDGET_SKIPS_TOTAL: IntCounterVec =
        register_int_counter_vec!(
            "worker_sync_retry_budget_skips_total",
            "Total number of eligible worker batch sync retries skipped because of the per-tick retry budget",
            &["priority"]
        )
        .expect("failed to register worker_sync_retry_budget_skips_total");
}

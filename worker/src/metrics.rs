use lazy_static::lazy_static;
use prometheus::{register_int_counter, register_int_gauge, IntCounter, IntGauge};

lazy_static! {
    // ============================================================================
    // WORKER INGRESS THROUGHPUT COUNTERS
    // ============================================================================

    pub static ref WORKER_INGRESS_TRANSACTIONS_RECEIVED_TOTAL: IntCounter =
        register_int_counter!(
            "worker_ingress_transactions_received_total",
            "Total transactions received by this worker"
        )
        .expect("failed to register worker_ingress_transactions_received_total");

    pub static ref WORKER_INGRESS_BATCHES_CREATED_TOTAL: IntCounter =
        register_int_counter!(
            "worker_ingress_batches_created_total",
            "Total batches sealed by this worker"
        )
        .expect("failed to register worker_ingress_batches_created_total");

    pub static ref WORKER_INGRESS_BATCH_BYTES_TOTAL: IntCounter =
        register_int_counter!(
            "worker_ingress_batch_bytes_total",
            "Total bytes sealed into batches"
        )
        .expect("failed to register worker_ingress_batch_bytes_total");

    // ============================================================================
    // WORKER RECOVERY STATE GAUGES
    // ============================================================================

    pub static ref WORKER_RECOVERY_PENDING_BATCHES: IntGauge =
        register_int_gauge!(
            "worker_recovery_pending_batches",
            "Current number of batches pending recovery"
        )
        .expect("failed to register worker_recovery_pending_batches");

    pub static ref WORKER_RECOVERY_STALLED_BATCHES: IntGauge =
        register_int_gauge!(
            "worker_recovery_stalled_batches",
            "Current number of stalled batch recoveries"
        )
        .expect("failed to register worker_recovery_stalled_batches");

    pub static ref WORKER_RECOVERY_SYNC_REQUESTS_TOTAL: IntCounter =
        register_int_counter!(
            "worker_recovery_sync_requests_total",
            "Payload and batch sync requests sent by this worker"
        )
        .expect("failed to register worker_recovery_sync_requests_total");
}

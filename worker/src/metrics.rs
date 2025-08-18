use lazy_static::lazy_static;
use prometheus::{register_gauge, register_int_counter, register_int_gauge, Gauge, IntCounter, IntGauge};

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
}



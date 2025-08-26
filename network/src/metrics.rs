use lazy_static::lazy_static;
use prometheus::{register_int_counter_vec, register_int_gauge, IntCounterVec, IntGauge};

lazy_static! {
    pub static ref NETWORK_MESSAGES_TOTAL: IntCounterVec = register_int_counter_vec!(
        "network_messages_total",
        "Total number of network messages by direction",
        &["direction"]
    ).expect("failed to register network_messages_total");
    pub static ref NETWORK_CONNECTED_PEERS: IntGauge = register_int_gauge!(
        "network_connected_peers",
        "Gauge for currently connected peers (best-effort)"
    ).expect("failed to register network_connected_peers");
}

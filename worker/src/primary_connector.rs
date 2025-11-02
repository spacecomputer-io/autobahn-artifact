// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use bytes::Bytes;
use log::{info, debug, warn, error};
use network::SimpleSender;
use std::net::SocketAddr;
use tokio::sync::mpsc::Receiver;

// Send batches' digests to the primary.
pub struct PrimaryConnector {
    /// The primary network address.
    primary_address: SocketAddr,
    /// Input channel to receive the digests to send to the primary.
    rx_digest: Receiver<SerializedBatchDigestMessage>,
    /// A network sender to send the baches' digests to the primary.
    network: SimpleSender,
}

impl PrimaryConnector {
    pub fn spawn(primary_address: SocketAddr, rx_digest: Receiver<SerializedBatchDigestMessage>) {
        tokio::spawn(async move {
            Self {
                primary_address,
                rx_digest,
                network: SimpleSender::new(),
            }
            .run()
            .await;
        });
    }

    async fn run(&mut self) {
        info!("PrimaryConnector: Connected to primary at {}", self.primary_address);
        
        let mut msg_count: u64 = 0;
        let mut last_log_time = std::time::Instant::now();
        
        while let Some(digest_message) = self.rx_digest.recv().await {
            let msg_size = digest_message.len();
            msg_count += 1;
            
            // Send the digest through the network.
            self.network
                .send(self.primary_address, Bytes::from(digest_message))
                .await;
            
            // Log aggregate stats every 5 seconds to reduce log spam
            if last_log_time.elapsed().as_secs() >= 5 {
                let rate = msg_count as f64 / last_log_time.elapsed().as_secs_f64();
                info!("PrimaryConnector: Sent {} digests to primary ({:.2} digests/s) in last {:.1}s", 
                      msg_count, rate, last_log_time.elapsed().as_secs_f64());
                msg_count = 0;
                last_log_time = std::time::Instant::now();
            }
        }
        
        warn!("PrimaryConnector: Channel closed, stopping");
    }
}

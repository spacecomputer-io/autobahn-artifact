// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use bytes::Bytes;
use log::{info, warn};
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
        let mut total_send_time_ms: u64 = 0;
        let mut slow_sends: u64 = 0;
        
        while let Some(digest_message) = self.rx_digest.recv().await {
            msg_count += 1;
            
            // Send the digest through the network with latency measurement.
            let send_start = std::time::Instant::now();
            self.network
                .send(self.primary_address, Bytes::from(digest_message))
                .await;
            let send_elapsed_ms = send_start.elapsed().as_millis() as u64;
            
            total_send_time_ms += send_elapsed_ms;
            if send_elapsed_ms > 10 {
                slow_sends += 1;
                warn!("PrimaryConnector: Slow send to primary took {}ms", send_elapsed_ms);
            }
            
            // Log aggregate stats every 5 seconds to reduce log spam
            if last_log_time.elapsed().as_secs() >= 5 {
                let rate = msg_count as f64 / last_log_time.elapsed().as_secs_f64();
                info!("PrimaryConnector: Sent {} digests to primary ({:.2} digests/s) in last {:.1}s", 
                      msg_count, rate, last_log_time.elapsed().as_secs_f64());
                
                // Channel capacity monitoring
                let rx_remaining = self.rx_digest.capacity() - self.rx_digest.len();
                let rx_usage_pct = (self.rx_digest.len() as f64 / self.rx_digest.capacity() as f64) * 100.0;
                
                info!("PrimaryConnector CHANNEL: rx_digest {}/{} slots ({:.1}% full, {} remaining)",
                      self.rx_digest.len(), self.rx_digest.capacity(), rx_usage_pct, rx_remaining);
                
                if rx_usage_pct > 80.0 {
                    warn!("PrimaryConnector: rx_digest channel {:.1}% full ({}/{}) - Primary may be slow to accept batches!",
                          rx_usage_pct, self.rx_digest.len(), self.rx_digest.capacity());
                }
                
                // Network send performance
                if msg_count > 0 {
                    let avg_send_ms = total_send_time_ms as f64 / msg_count as f64;
                    info!("PrimaryConnector NETWORK: {} sends to primary, avg {:.2}ms, {} slow (>10ms)",
                          msg_count, avg_send_ms, slow_sends);
                    total_send_time_ms = 0;
                    slow_sends = 0;
                }
                
                msg_count = 0;
                last_log_time = std::time::Instant::now();
            }
        }
        
        warn!("PrimaryConnector: Channel closed, stopping");
    }
}

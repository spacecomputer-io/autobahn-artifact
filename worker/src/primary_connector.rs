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
        let mut send_latencies: Vec<u64> = Vec::with_capacity(1000); // Track for percentiles
        
        while let Some(digest_message) = self.rx_digest.recv().await {
            msg_count += 1;
            
            // Send the digest through the network with latency measurement.
            let send_start = std::time::Instant::now();
            self.network
                .send(self.primary_address, Bytes::from(digest_message))
                .await;
            let send_elapsed_ms = send_start.elapsed().as_millis() as u64;
            
            total_send_time_ms += send_elapsed_ms;
            send_latencies.push(send_elapsed_ms);
            
            if send_elapsed_ms > 10 {
                slow_sends += 1;
                warn!("PrimaryConnector: Slow send to primary took {}ms - possible TCP backpressure!", send_elapsed_ms);
            }
            
            // Log severe network delays that indicate TCP congestion
            if send_elapsed_ms > 50 {
                warn!("PrimaryConnector: SEVERE network delay {}ms - TCP congestion likely!", send_elapsed_ms);
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
                
                // Network send performance with percentiles
                if msg_count > 0 {
                    let avg_send_ms = total_send_time_ms as f64 / msg_count as f64;
                    
                    // Calculate percentiles
                    let mut sorted_latencies = send_latencies.clone();
                    sorted_latencies.sort();
                    let p50_idx = (sorted_latencies.len() as f64 * 0.50) as usize;
                    let p95_idx = (sorted_latencies.len() as f64 * 0.95) as usize;
                    let p99_idx = (sorted_latencies.len() as f64 * 0.99) as usize;
                    let max_idx = sorted_latencies.len().saturating_sub(1);
                    
                    let p50 = sorted_latencies.get(p50_idx).copied().unwrap_or(0);
                    let p95 = sorted_latencies.get(p95_idx).copied().unwrap_or(0);
                    let p99 = sorted_latencies.get(p99_idx).copied().unwrap_or(0);
                    let max = sorted_latencies.get(max_idx).copied().unwrap_or(0);
                    
                    info!("PrimaryConnector NETWORK: {} sends, avg {:.2}ms, p50={}ms, p95={}ms, p99={}ms, max={}ms, {} slow (>10ms, {:.1}%)",
                          msg_count, avg_send_ms, p50, p95, p99, max, slow_sends,
                          (slow_sends as f64 / msg_count as f64) * 100.0);
                    
                    // Warn if network significantly degraded (indicates TCP congestion)
                    if p95 > 50 {
                        warn!("PrimaryConnector NETWORK SEVERELY DEGRADED: p95={}ms (threshold: 50ms) - TCP CONGESTION CONFIRMED!",
                              p95);
                    } else if p95 > 20 {
                        warn!("PrimaryConnector NETWORK DEGRADED: p95={}ms (threshold: 20ms) - TCP backpressure detected",
                              p95);
                    }
                    
                    total_send_time_ms = 0;
                    slow_sends = 0;
                    send_latencies.clear(); // Clear for next period
                }
                
                msg_count = 0;
                last_log_time = std::time::Instant::now();
            }
        }
        
        warn!("PrimaryConnector: Channel closed, stopping");
    }
}

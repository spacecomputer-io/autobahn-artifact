// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use log::{info, warn, error};
use primary::WorkerPrimaryMessage;
use std::convert::TryInto;
use store::Store;
use tokio::sync::mpsc::{Receiver, Sender};
use crate::metrics::WORKER_DIGESTS_SENT_TO_PRIMARY_TOTAL;

#[cfg(test)]
#[path = "tests/processor_tests.rs"]
pub mod processor_tests;

/// Indicates a serialized `WorkerMessage::Batch` message.
pub type SerializedBatchMessage = (Vec<u8>, Option<u64>);

/// Hashes and stores batches, it then outputs the batch's digest.
pub struct Processor;

impl Processor {
    pub fn spawn(
        // Our worker's id.
        id: WorkerId,
        // The persistent storage.
        mut store: Store,
        // Input channel to receive batches.
        mut rx_batch: Receiver<SerializedBatchMessage>,
        // Output channel to send out batches' digests.
        tx_digest: Sender<SerializedBatchDigestMessage>,    //sender channel connects to PrimaryConnector
        // Whether we are processing our own batches or the batches of other nodes.
        own_digest: bool,
    ) {
        tokio::spawn(async move {
            let mut batch_count: u64 = 0;
            let mut total_bytes: u64 = 0;
            let mut last_log_time = std::time::Instant::now();
            let mut total_store_write_time_ms: u64 = 0;
            let mut slow_store_writes: u64 = 0;
            
            while let Some((batch, first_tx_at_ms)) = rx_batch.recv().await {
                // Hash the batch.
                let digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());
                let batch_size_bytes = batch.len() as u64;

                // Store the batch with latency measurement.
                let store_start = std::time::Instant::now();
                store.write(digest.to_vec(), batch).await;
                let store_elapsed_ms = store_start.elapsed().as_millis() as u64;
                
                total_store_write_time_ms += store_elapsed_ms;
                if store_elapsed_ms > 10 {
                    slow_store_writes += 1;
                    warn!("WORKER[{}]: Slow store write took {}ms for {} bytes", 
                          id, store_elapsed_ms, batch_size_bytes);
                }

                // Deliver the batch's digest.
                let digest_copy = digest.clone();
                let message = match own_digest {
                    true => WorkerPrimaryMessage::OurBatch(digest, id, first_tx_at_ms.unwrap_or(0), batch_size_bytes),
                    false => WorkerPrimaryMessage::OthersBatch(digest, id, batch_size_bytes),
                };
                
                let serialized_message = match bincode::serialize(&message) {
                    Ok(msg) => msg,
                    Err(e) => {
                        error!("WORKER[{}]: CRITICAL - Failed to serialize batch digest {}: {:?}", id, digest_copy, e);
                        return;
                    }
                };
                
                // Track stats for periodic logging
                batch_count += 1;
                total_bytes += batch_size_bytes;
                
                // Try to send with timeout detection
                match tx_digest.try_send(serialized_message) {
                    Ok(_) => {
                        WORKER_DIGESTS_SENT_TO_PRIMARY_TOTAL.inc();
                    },
                    Err(tokio::sync::mpsc::error::TrySendError::Full(msg)) => {
                        warn!("WORKER[{}]: Channel to Primary FULL! Batch {} blocked - possible backpressure from primary", 
                              id, digest_copy);
                        // Fallback to blocking send
                        if tx_digest.send(msg).await.is_err() {
                            error!("WORKER[{}]: Failed to send digest {} after backpressure wait", id, digest_copy);
                        }
                        WORKER_DIGESTS_SENT_TO_PRIMARY_TOTAL.inc();
                    },
                    Err(e) => {
                        error!("WORKER[{}]: Channel closed - failed to send digest {}: {}", id, digest_copy, e);
                    }
                }
                
                // Log aggregate stats every 5 seconds
                if last_log_time.elapsed().as_secs() >= 5 {
                    let rate = batch_count as f64 / last_log_time.elapsed().as_secs_f64();
                    let throughput_mb = (total_bytes as f64 / last_log_time.elapsed().as_secs_f64()) / 1_048_576.0;
                    info!("WORKER[{}]: Processed {} batches ({:.2} batches/s, {:.2} MB/s) in last {:.1}s", 
                          id, batch_count, rate, throughput_mb, last_log_time.elapsed().as_secs_f64());
                    
                    // Channel capacity monitoring (only RX channels - Sender doesn't expose len())
                    let rx_remaining = rx_batch.capacity() - rx_batch.len();
                    let rx_usage_pct = (rx_batch.len() as f64 / rx_batch.capacity() as f64) * 100.0;
                    
                    info!("WORKER[{}] CHANNELS: rx_batch {}/{} slots ({:.1}% full, {} remaining)",
                          id, rx_batch.len(), rx_batch.capacity(), rx_usage_pct, rx_remaining);
                    
                    // Warn if channel getting full
                    if rx_usage_pct > 80.0 {
                        warn!("WORKER[{}]: rx_batch channel {:.1}% full ({}/{}) - BatchMaker may be blocked!",
                              id, rx_usage_pct, rx_batch.len(), rx_batch.capacity());
                    }
                    
                    // Store write performance
                    if batch_count > 0 {
                        let avg_store_ms = total_store_write_time_ms as f64 / batch_count as f64;
                        info!("WORKER[{}] STORAGE: {} writes, avg {:.2}ms, {} slow (>10ms)",
                              id, batch_count, avg_store_ms, slow_store_writes);
                        total_store_write_time_ms = 0;
                        slow_store_writes = 0;
                    }
                    
                    batch_count = 0;
                    total_bytes = 0;
                    last_log_time = std::time::Instant::now();
                }
            }
        });
    }
}

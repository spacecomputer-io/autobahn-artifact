#![allow(unused_variables)]
#![allow(unused_imports)]
// Copyright(C) Facebook, Inc. and its affiliates.
use crate::quorum_waiter::QuorumWaiterMessage;
use crate::worker::WorkerMessage;
use bytes::Bytes;
#[cfg(feature = "benchmark")]
use crypto::Digest;
use crypto::PublicKey;
#[cfg(feature = "benchmark")]
use ed25519_dalek::{Digest as _, Sha512};
use log::{debug, info, warn};
use network::{ReliableSender, SimpleSender};
#[cfg(feature = "benchmark")]
use std::convert::TryInto as _;
use std::net::SocketAddr;
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};
use crate::metrics::{WORKER_BATCHES_SEALED_TOTAL, WORKER_BATCH_SIZE_BYTES};
use std::time::{SystemTime, UNIX_EPOCH};
use crate::processor::SerializedBatchMessage;

#[cfg(test)]
#[path = "tests/batch_maker_tests.rs"]
pub mod batch_maker_tests;

//The message type received by clients
pub type Transaction = Vec<u8>;
//The message type forwarded to quorum waiters
pub type Batch = Vec<Transaction>;

/// Assemble clients transactions into batches.
pub struct BatchMaker {
    /// The preferred batch size (in bytes).
    batch_size: usize,
    /// The maximum delay after which to seal the batch (in ms).
    max_batch_delay: u64,
    /// Channel to receive transactions from the network.
    rx_transaction: Receiver<Transaction>,
   
    //tx_message: Sender<QuorumWaiterMessage>,  /// Output channel to deliver sealed batches to the `QuorumWaiter`.
    tx_batch: Sender<SerializedBatchMessage>,   // channel to forward batch digest (and first tx time) to processor in order for primary to propose.

    /// The network addresses of the other workers that share our worker id.
    workers_addresses: Vec<(PublicKey, SocketAddr)>,
    /// Holds the current batch.
    current_batch: Batch,
    /// Holds the size of the current batch (in bytes).
    current_batch_size: usize,
    /// A network sender to broadcast the batches to the other workers.
    network: SimpleSender,
    /// Timestamp in ms of the first tx received for the currently building batch (if any).
    first_tx_submit_ms: Option<u64>,
    /// Performance tracking for network broadcasts
    total_broadcast_time_ms: u64,
    broadcast_count: u64,
    slow_broadcasts: u64,
}

impl BatchMaker {
    pub fn spawn(
        batch_size: usize,
        max_batch_delay: u64,
        rx_transaction: Receiver<Transaction>, //receiver channel from worker.TxReceiverHandler 
        //tx_message: Sender<QuorumWaiterMessage>, //sender channel to worker.QuorumWaiter
        tx_batch: Sender<SerializedBatchMessage>,   // sender channel to worker.Processor
        workers_addresses: Vec<(PublicKey, SocketAddr)>,
    ) {
        tokio::spawn(async move {
            Self {
                batch_size,
                max_batch_delay,
                rx_transaction,
                //tx_message, //previously forwarded batch to Quorum_waiter; now skipping this step.
                tx_batch,  
                workers_addresses,
                current_batch: Batch::with_capacity(batch_size * 2),
                current_batch_size: 0,
                network: SimpleSender::new(),
                first_tx_submit_ms: None,
                total_broadcast_time_ms: 0,
                broadcast_count: 0,
                slow_broadcasts: 0,
            }
            .run()
            .await;
        });
    }

    /// Main loop receiving incoming transactions and creating batches.
    async fn run(&mut self) {
        let timer = sleep(Duration::from_millis(self.max_batch_delay));
        tokio::pin!(timer);
        let mut current_time = Instant::now();
        let mut tx_count: u64 = 0;
        let mut batch_sealed_count: u64 = 0;
        let mut last_stats_log = Instant::now();

        loop {
            tokio::select! {
                // Assemble client transactions into batches of preset size.
                Some(transaction) = self.rx_transaction.recv() => {
                    if self.current_batch.is_empty() && self.first_tx_submit_ms.is_none() {
                        let ts_ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0);
                        self.first_tx_submit_ms = Some(ts_ms);
                    }
                    self.current_batch_size += transaction.len();
                    self.current_batch.push(transaction);
                    tx_count += 1;
                    
                    if self.current_batch_size >= self.batch_size {
                        self.seal().await;
                        batch_sealed_count += 1;

                        debug!("Batch sealed in {:?} ms", current_time.elapsed().as_millis());
                        current_time = Instant::now();

                        timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                    }
                },

                // If the timer triggers, seal the batch even if it contains few transactions.
                () = &mut timer => {
                    if !self.current_batch.is_empty() {
                        debug!("BatchMaker: Timer triggered, sealing partial batch ({} bytes)", self.current_batch_size);
                        self.seal().await;
                        batch_sealed_count += 1;
                    }

                    current_time = Instant::now();
                    timer.as_mut().reset(Instant::now() + Duration::from_millis(self.max_batch_delay));
                }
            }
            
            // Log aggregate stats every 5 seconds
            if last_stats_log.elapsed().as_secs() >= 5 {
                let tx_rate = tx_count as f64 / last_stats_log.elapsed().as_secs_f64();
                let batch_rate = batch_sealed_count as f64 / last_stats_log.elapsed().as_secs_f64();
                
                // Channel capacity monitoring
                let rx_remaining = self.rx_transaction.capacity() - self.rx_transaction.len();
                let rx_usage_pct = (self.rx_transaction.len() as f64 / self.rx_transaction.capacity() as f64) * 100.0;
                
                info!("BatchMaker: Received {} txs ({:.1} tx/s), sealed {} batches ({:.2} batch/s) in last {:.1}s", 
                      tx_count, tx_rate, batch_sealed_count, batch_rate, last_stats_log.elapsed().as_secs_f64());
                
                info!("BatchMaker CHANNEL: rx_transaction {}/{} slots used ({:.1}% full, {} remaining)",
                      self.rx_transaction.len(), self.rx_transaction.capacity(), rx_usage_pct, rx_remaining);
                
                // Warn if channel getting full
                if rx_usage_pct > 80.0 {
                    warn!("BatchMaker: rx_transaction channel {:.1}% full ({}/{}) - possible backpressure from network!",
                          rx_usage_pct, self.rx_transaction.len(), self.rx_transaction.capacity());
                }
                
                // Network broadcast performance
                if self.broadcast_count > 0 {
                    let avg_broadcast_ms = self.total_broadcast_time_ms as f64 / self.broadcast_count as f64;
                    info!("BatchMaker NETWORK: {} broadcasts, avg {:.2}ms, {} slow (>20ms)",
                          self.broadcast_count, avg_broadcast_ms, self.slow_broadcasts);
                    self.total_broadcast_time_ms = 0;
                    self.broadcast_count = 0;
                    self.slow_broadcasts = 0;
                }
                
                tx_count = 0;
                batch_sealed_count = 0;
                last_stats_log = Instant::now();
            }

            // Give the change to schedule other tasks.
            tokio::task::yield_now().await;
        }
    }

    /// Seal and broadcast the current batch.
    async fn seal(&mut self) {
        let sealed_size = self.current_batch_size;

        // Look for sample txs (they all start with 0) and gather their txs id (the next 8 bytes).
        #[cfg(feature = "benchmark")]
        let tx_ids: Vec<_> = self
            .current_batch
            .iter()
            .filter(|tx| tx[0] == 0u8 && tx.len() > 8)
            .filter_map(|tx| tx[1..9].try_into().ok())
            .collect();

        // Serialize the batch.
        self.current_batch_size = 0;
        let batch: Vec<_> = self.current_batch.drain(..).collect();
        let message = WorkerMessage::Batch(batch);
        let serialized = bincode::serialize(&message).expect("Failed to serialize our own batch");
        let batch_size_bytes = serialized.len() as u64;

        #[cfg(feature = "benchmark")]
        {
            // NOTE: This is one extra hash that is only needed to print the following log entries.
            let digest = Digest(
                Sha512::digest(&serialized).as_slice()[..32]
                    .try_into()
                    .unwrap(),
            );

            for id in tx_ids {
                // NOTE: This log entry is used to compute performance.
            info!(
                "Batch {:?} contains sample tx {}",
                digest,
                u64::from_be_bytes(id)
            );
        }

        // NOTE: Removed individual batch size logging - now using aggregate stats
    }

        // Broadcast the batch through the network.

        //NEW:
        //Best-effort broadcast only. Any failure is correlated with the primary operating this node (running on same machine)
        let (_, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        let bytes = Bytes::from(serialized.clone());
        
        // Measure broadcast latency
        let broadcast_start = Instant::now();
        self.network.broadcast(addresses, bytes).await;
        let broadcast_elapsed_ms = broadcast_start.elapsed().as_millis() as u64;
        
        // Track broadcast performance
        self.broadcast_count += 1;
        self.total_broadcast_time_ms += broadcast_elapsed_ms;
        if broadcast_elapsed_ms > 20 {
            self.slow_broadcasts += 1;
            log::warn!("BatchMaker: Slow broadcast took {}ms to {} workers", 
                      broadcast_elapsed_ms, self.workers_addresses.len());
        } 

        let submit_ms = self.first_tx_submit_ms.take();
        self.tx_batch.send((serialized, submit_ms)).await.expect("Failed to deliver batch");
        WORKER_BATCHES_SEALED_TOTAL.inc();
        WORKER_BATCH_SIZE_BYTES.set(sealed_size as i64);

        //OLD:
        //This uses reliable sender. The receiver worker will reply with an ack. The Reply Handler is passed to Quorum Waiter.
        // let (names, addresses): (Vec<_>, _) = self.workers_addresses.iter().cloned().unzip();
        // let bytes = Bytes::from(serialized.clone());
        // let handlers = self.network.broadcast(addresses, bytes).await; 

        // // Send the batch through the deliver channel for further processing.
        // self.tx_message
        //     .send(QuorumWaiterMessage {
        //         batch: serialized,
        //         handlers: names.into_iter().zip(handlers.into_iter()).collect(),
        //     })
        //     .await
        //     .expect("Failed to deliver batch");
    }
}

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::worker::SerializedBatchDigestMessage;
use config::WorkerId;
use crypto::Digest;
use ed25519_dalek::Digest as _;
use ed25519_dalek::Sha512;
use log::{info, debug, warn, error};
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
            while let Some((batch, first_tx_at_ms)) = rx_batch.recv().await {
                // Hash the batch.
                let digest = Digest(Sha512::digest(&batch).as_slice()[..32].try_into().unwrap());
                let batch_size_bytes = batch.len() as u64;

                // Store the batch.
                store.write(digest.to_vec(), batch).await;
                //store.write(digest.to_vec(), Vec::default()).await;

                // Deliver the batch's digest.
                let digest_for_logging = digest.clone();
                let message = match own_digest {
                    true => {
                        info!("🔨 WORKER: Creating OurBatch message - digest: {}, worker: {}, size: {} bytes", 
                              digest_for_logging, id, batch_size_bytes);
                        WorkerPrimaryMessage::OurBatch(digest, id, first_tx_at_ms.unwrap_or(0), batch_size_bytes)
                    },
                    false => {
                        debug!("🔨 WORKER: Creating OthersBatch message - digest: {}, worker: {}, size: {} bytes", 
                              digest_for_logging, id, batch_size_bytes);
                        WorkerPrimaryMessage::OthersBatch(digest, id, batch_size_bytes)
                    },
                };
                
                let serialized_message = match bincode::serialize(&message) {
                    Ok(msg) => {
                        info!("✅ WORKER: Successfully serialized WorkerPrimaryMessage - {} bytes", msg.len());
                        msg
                    },
                    Err(e) => {
                        error!("❌ WORKER: Failed to serialize WorkerPrimaryMessage: {:?}", e);
                        return;
                    }
                };
                
                info!("📤 WORKER: Sending {} bytes to PrimaryConnector", serialized_message.len());
                match tx_digest.send(serialized_message).await {
                    Ok(_) => {
                        info!("✅ WORKER: Successfully sent digest {} to PrimaryConnector", digest_for_logging);
                        WORKER_DIGESTS_SENT_TO_PRIMARY_TOTAL.inc();
                    },
                    Err(e) => {
                        error!("❌ WORKER: Failed to send digest {} to PrimaryConnector: {}", digest_for_logging, e);
                    }
                }
            }
        });
    }
}

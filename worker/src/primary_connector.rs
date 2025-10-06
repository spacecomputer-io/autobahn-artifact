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
        info!("🚀 PrimaryConnector: Starting to listen for batch digests to send to primary {}", self.primary_address);
        
        while let Some(digest_message) = self.rx_digest.recv().await {
            info!("📡 PrimaryConnector: Received {} bytes to send to primary {}", 
                  digest_message.len(), self.primary_address);
            
            // Send the digest through the network.
            info!("🌐 PrimaryConnector: Sending WorkerPrimaryMessage via SimpleSender");
            self.network
                .send(self.primary_address, Bytes::from(digest_message))
                .await;
            info!("✅ PrimaryConnector: Message sent to primary (SimpleSender completed)");
        }
        
        warn!("🔚 PrimaryConnector: Channel closed, stopping message forwarding");
    }
}

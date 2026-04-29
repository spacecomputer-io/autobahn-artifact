// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::NetworkError;
use bytes::Bytes;
use futures::sink::SinkExt as _;
use futures::stream::StreamExt as _;
use log::{info, warn};
use crate::metrics::NETWORK_MESSAGES_TOTAL;
use rand::prelude::SliceRandom as _;
use rand::rngs::SmallRng;
use rand::SeedableRng as _;
use std::collections::HashMap;
use std::net::SocketAddr;
use tokio::net::TcpStream;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[cfg(test)]
#[path = "tests/simple_sender_tests.rs"]
pub mod simple_sender_tests;

/// We keep alive one TCP connection per peer, each connection is handled by a separate task (called `Connection`).
/// We communicate with our 'connections' through a dedicated channel kept by the HashMap called `connections`.
pub struct SimpleSender {
    /// A map holding the channels to our connections.
    connections: HashMap<SocketAddr, Sender<Bytes>>,
    /// Small RNG just used to shuffle nodes and randomize connections (not crypto related).
    rng: SmallRng,
}

impl std::default::Default for SimpleSender {
    fn default() -> Self {
        Self::new()
    }
}

impl SimpleSender {
    pub fn new() -> Self {
        Self {
            connections: HashMap::new(),
            rng: SmallRng::from_entropy(),
        }
    }

    /// Helper function to spawn a new connection.
    fn spawn_connection(address: SocketAddr) -> Sender<Bytes> {
        let (tx, rx) = channel(5_000);
        Connection::spawn(address, rx);
        tx
    }

    /// Try (best-effort) to send a message to a specific address.
    /// This is useful to answer sync requests.
    pub async fn send(&mut self, address: SocketAddr, data: Bytes) {
        NETWORK_MESSAGES_TOTAL.with_label_values(&["send"]).inc();
        // Try to re-use an existing connection if possible.
        if let Some(tx) = self.connections.get(&address) {
            if tx.send(data.clone()).await.is_ok() {
                return;
            }
        }

        // Otherwise make a new connection.
        let tx = Self::spawn_connection(address);
        if tx.send(data).await.is_ok() {
            self.connections.insert(address, tx);
        }
    }

    /// Try (best-effort) to broadcast the message to all specified addresses.
    pub async fn broadcast(&mut self, addresses: Vec<SocketAddr>, data: Bytes) {
        for address in addresses {
            self.send(address, data.clone()).await;
        }
    }

    /// Non-blocking best-effort broadcast: enqueue the message to each peer's channel
    /// without waiting. If a peer's channel is full (backpressure), the send to that
    /// peer is silently dropped — the caller must rely on sync/recovery for missed data.
    /// This is intended for batch data broadcast where the sync protocol handles gaps.
    pub fn broadcast_best_effort(&mut self, addresses: Vec<SocketAddr>, data: Bytes) -> usize {
        let mut dropped = 0;
        for address in addresses {
            NETWORK_MESSAGES_TOTAL.with_label_values(&["send"]).inc();
            if let Some(tx) = self.connections.get(&address) {
                match tx.try_send(data.clone()) {
                    Ok(()) => continue,
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        dropped += 1;
                        continue;
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                        // Connection died — spawn a fresh one and try once
                    }
                }
            }
            // No existing connection or it was closed — spawn a new one
            let tx = Self::spawn_connection(address);
            if tx.try_send(data.clone()).is_err() {
                dropped += 1;
            } else {
                self.connections.insert(address, tx);
            }
        }
        dropped
    }

    /// Non-blocking best-effort send: enqueue the message to the peer's channel without
    /// waiting. Returns true if enqueued, false if dropped (channel full or closed).
    pub fn send_best_effort(&mut self, address: SocketAddr, data: Bytes) -> bool {
        NETWORK_MESSAGES_TOTAL.with_label_values(&["send"]).inc();
        if let Some(tx) = self.connections.get(&address) {
            match tx.try_send(data.clone()) {
                Ok(()) => return true,
                Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => return false,
                Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                    // Connection died — spawn a fresh one below
                }
            }
        }
        let tx = Self::spawn_connection(address);
        let ok = tx.try_send(data).is_ok();
        if ok {
            self.connections.insert(address, tx);
        }
        ok
    }

    /// Non-blocking lucky_broadcast: pick `nodes` addresses at random and send best-effort.
    /// Returns the number of sends that were dropped due to backpressure.
    pub fn lucky_broadcast_best_effort(
        &mut self,
        mut addresses: Vec<SocketAddr>,
        data: Bytes,
        nodes: usize,
    ) -> usize {
        addresses.shuffle(&mut self.rng);
        addresses.truncate(nodes);
        self.broadcast_best_effort(addresses, data)
    }

    /// Pick a few addresses at random (specified by `nodes`) and try (best-effort) to send the
    /// message only to them. This is useful to pick nodes with whom to sync.
    pub async fn lucky_broadcast(
        &mut self,
        mut addresses: Vec<SocketAddr>,
        data: Bytes,
        nodes: usize,
    ) {
        addresses.shuffle(&mut self.rng);
        addresses.truncate(nodes);
        self.broadcast(addresses, data).await
    }
}

/// A connection is responsible to establish and keep alive (if possible) a connection with a single peer.
struct Connection {
    /// The destination address.
    address: SocketAddr,
    /// Channel from which the connection receives its commands.
    receiver: Receiver<Bytes>,
}

impl Connection {
    fn spawn(address: SocketAddr, receiver: Receiver<Bytes>) {
        tokio::spawn(async move {
            Self { address, receiver }.run().await;
        });
    }

    /// Main loop trying to connect to the peer and transmit messages.
    async fn run(&mut self) {
        // Try to connect to the peer.
        let (mut writer, mut reader) = match TcpStream::connect(self.address).await {
            Ok(stream) => {
                // Enable TCP_NODELAY to disable Nagle's algorithm for low-latency communication
                if let Err(e) = stream.set_nodelay(true) {
                    warn!("Failed to set TCP_NODELAY for connection to {}: {}", self.address, e);
                }
                Framed::new(stream, LengthDelimitedCodec::new()).split()
            },
            Err(e) => {
                warn!(
                    "{}",
                    NetworkError::FailedToConnect(self.address, /* retry */ 0, e)
                );
                return;
            }
        };
        info!("Outgoing connection established with {}", self.address);

        // Transmit messages once we have established a connection.
        loop {
            // Check if there are any new messages to send or if we get an ACK for messages we already sent.
            tokio::select! {
                Some(data) = self.receiver.recv() => {
                    if let Err(e) = writer.send(data).await {
                        warn!("{}", NetworkError::FailedToSendMessage(self.address, e));
                        // Count failed send on send side
                        NETWORK_MESSAGES_TOTAL.with_label_values(&["failed_send"]).inc();
                        return;
                    }
                },
                response = reader.next() => {
                    match response {
                        Some(Ok(_)) => {
                            // Sink the reply.
                        },
                        _ => {
                            // Something has gone wrong (either the channel dropped or we failed to read from it).
                            warn!("{}", NetworkError::FailedToReceiveAck(self.address));
                            NETWORK_MESSAGES_TOTAL.with_label_values(&["failed_send"]).inc();
                            return;
                        }
                    }
                },
            }
        }
    }
}

// shared counters are in network::metrics

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::error::NetworkError;
use async_trait::async_trait;
use bytes::Bytes;
use futures::stream::SplitSink;
use futures::stream::StreamExt as _;
use log::{debug, info, warn};
use lazy_static::lazy_static;
use prometheus::{register_int_counter, register_int_gauge, IntCounter, IntGauge};
use std::error::Error;
use std::net::SocketAddr;
use tokio::net::{TcpListener, TcpStream};
use tokio_util::codec::{Framed, LengthDelimitedCodec};

#[cfg(test)]
#[path = "tests/receiver_tests.rs"]
pub mod receiver_tests;

/// Convenient alias for the writer end of the TCP channel.
pub type Writer = SplitSink<Framed<TcpStream, LengthDelimitedCodec>, Bytes>;

#[async_trait]
pub trait MessageHandler: Clone + Send + Sync + 'static {
    /// Defines how to handle an incoming message. A typical usage is to define a `MessageHandler` with a
    /// number of `Sender<T>` channels. Then implement `dispatch` to deserialize incoming messages and
    /// forward them through the appropriate delivery channel. Then `writer` can be used to send back
    /// responses or acknowledgements to the sender machine (see unit tests for examples).
    async fn dispatch(&self, writer: &mut Writer, message: Bytes) -> Result<(), Box<dyn Error>>;
}

/// For each incoming request, we spawn a new runner responsible to receive messages and forward them
/// through the provided deliver channel.
pub struct Receiver<Handler: MessageHandler> {
    /// Address to listen to.
    address: SocketAddr,
    /// Struct responsible to define how to handle received messages.
    handler: Handler,
}

impl<Handler: MessageHandler> Receiver<Handler> {
    /// Spawn a new network receiver handling connections from any incoming peer.
    pub fn spawn(address: SocketAddr, handler: Handler) {
        tokio::spawn(async move {
            Self { address, handler }.run().await;
        });
    }

    /// Main loop responsible to accept incoming connections and spawn a new runner to handle it.
    async fn run(&self) {
        //println!("receiver address {}", self.address.clone().to_string());
        let listener = TcpListener::bind(&self.address)
            .await
            .expect("Failed to bind TCP port");

        debug!("Listening on {}", self.address);
        NET_LISTENERS_BOUND_TOTAL.inc();
        loop {
            let (socket, peer) = match listener.accept().await {
                Ok(value) => value,
                Err(e) => {
                    warn!("{}", NetworkError::FailedToListen(e));
                    continue;
                }
            };
            info!("Incoming connection established with {}", peer);
            NET_CONNECTED_PEERS_GAUGE.inc();
            Self::spawn_runner(socket, peer, self.handler.clone()).await;
        }
    }

    /// Spawn a new runner to handle a specific TCP connection. It receives messages and process them
    /// using the provided handler.
    async fn spawn_runner(socket: TcpStream, peer: SocketAddr, handler: Handler) {
        tokio::spawn(async move {
            let transport = Framed::new(socket, LengthDelimitedCodec::new());
            let (mut writer, mut reader) = transport.split();
            while let Some(frame) = reader.next().await {
                match frame.map_err(|e| NetworkError::FailedToReceiveMessage(peer, e)) {
                    Ok(message) => {
                        NET_RECV_MESSAGES_TOTAL.inc();
                        if let Err(e) = handler.dispatch(&mut writer, message.freeze()).await {
                            warn!("{}", e);
                            NET_FAILED_RECV_MESSAGES_TOTAL.inc();
                            return;
                        }
                    }
                    Err(e) => {
                        warn!("{}", e);
                        NET_FAILED_RECV_MESSAGES_TOTAL.inc();
                        return;
                    }
                }
            }
            warn!("Connection closed by peer {}", peer);
            NET_CONNECTED_PEERS_GAUGE.dec();
        });
    }
}

lazy_static! {
    static ref NET_RECV_MESSAGES_TOTAL: IntCounter = register_int_counter!(
        "network_recv_messages_total",
        "Total number of received network messages"
    ).expect("failed to register network_recv_messages_total");
    static ref NET_FAILED_RECV_MESSAGES_TOTAL: IntCounter = register_int_counter!(
        "network_failed_recv_messages_total",
        "Total number of failed network message receives"
    ).expect("failed to register network_failed_recv_messages_total");
    static ref NET_LISTENERS_BOUND_TOTAL: IntCounter = register_int_counter!(
        "network_listeners_bound_total",
        "Total number of receivers bound"
    ).expect("failed to register network_listeners_bound_total");
    static ref NET_CONNECTED_PEERS_GAUGE: IntGauge = register_int_gauge!(
        "network_connected_peers",
        "Gauge for currently connected peers (best-effort)"
    ).expect("failed to register network_connected_peers");
}

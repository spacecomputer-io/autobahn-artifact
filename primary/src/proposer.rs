#![allow(dead_code)]
use std::collections::{BTreeMap, HashMap};

// Copyright(C) Facebook, Inc. and its affiliates.
use crate::messages::{Certificate, Header, ConsensusMessage};
use crate::primary::Height;
use crate::metrics::{DISSEMINATION_HEADERS_CREATED_TOTAL, record_header_created, now_ms};
use config::{Committee, WorkerId};
use crypto::{Digest, PublicKey, SignatureService, Hash};
use log::{debug, info, warn, error};
use tokio::sync::mpsc::{Receiver, Sender};
use tokio::time::{sleep, Duration, Instant};

#[cfg(test)]
#[path = "tests/proposer_tests.rs"]
pub mod proposer_tests;

/// Maximum number of batch digests to include in a single header.
/// This prevents runaway header growth when the proposer is blocked waiting
/// for a parent certificate: without this cap, digests accumulate unboundedly
/// and a single header can reach megabytes (e.g. 47,291 digests = 1.51 MB),
/// which peers cannot sync/certify in time.  Remaining digests are kept in
/// the queue and included in subsequent headers.
const MAX_HEADER_DIGESTS: usize = 1_000;

/// The proposer creates new headers and send them to the core for broadcasting and further processing.
pub struct Proposer {
    /// The public key of this primary.
    name: PublicKey,
    /// The committee information
    committee: Committee,
    /// Service to sign headers.
    signature_service: SignatureService,
    /// The size of the headers' payload.
    header_size: usize,
    /// The maximum delay to wait for batches' digests.
    max_header_delay: u64,

    /// Receives the parents to include in the next header (along with their round number).
    rx_core: Receiver<Certificate>,
    /// Receives the batches' digests from our workers.
    rx_workers: Receiver<(Digest, WorkerId)>,
    // Receives new consensus instance
    rx_instance: Receiver<ConsensusMessage>,
    /// Sends newly created headers to the `Core`.
    tx_core: Sender<Header>,
   
    /// The current height of this validator's chain
    height: Height,
    /// Holds the certificate waiting to be included in the next header
    last_parent: Option<Certificate>,
    // Holds the consensus info for the last special header
    consensus_instances: HashMap<Digest, ConsensusMessage>,
    /// Holds the batches' digests waiting to be included in the next header.
    digests: Vec<(Digest, WorkerId)>,
    /// Keeps track of the size (in bytes) of batches' digests that we received so far.
    payload_size: usize,

    num_active_instances: usize, 
    use_special_rule: bool, 
    is_special: bool,
}

impl Proposer {
    #[allow(clippy::too_many_arguments)]
    pub fn spawn(
        name: PublicKey,
        committee: Committee,
        signature_service: SignatureService,
        header_size: usize,
        max_header_delay: u64,
        rx_core: Receiver<Certificate>,
        rx_workers: Receiver<(Digest, WorkerId)>,
        rx_instance: Receiver<ConsensusMessage>,
        tx_core: Sender<Header>,
    ) {
        /*let genesis: Vec<Digest> = Certificate::genesis(&committee)
            .iter()
            .map(|x| x.digest())
            .collect();*/

        let genesis = Certificate::genesis_cert(&committee);


        tokio::spawn(async move {
            Self {
                name,
                committee,
                signature_service,
                header_size,
                max_header_delay,
                rx_core,
                rx_workers,
                rx_instance,
                tx_core,
                height: 0,
                last_parent: Some(genesis),
                consensus_instances: HashMap::new(),
                digests: Vec::with_capacity(2 * header_size),
                payload_size: 0,
                num_active_instances: 0,
                use_special_rule: false,
                is_special: false,
            }
            .run()
            .await;
        });
    }
    
    async fn make_header(&mut self) {
        // Bound the number of digests per header to prevent runaway growth.
        // When the proposer is blocked waiting for a parent certificate, digests
        // accumulate. Without a cap, a single header can contain tens of thousands
        // of digests that peers cannot sync in time. Remaining digests stay in
        // self.digests and are included in subsequent headers.
        let take = self.digests.len().min(MAX_HEADER_DIGESTS);
        let included: BTreeMap<_, _> = self.digests.drain(..take).collect();
        let num_batches = included.len();
        let remaining = self.digests.len();
        let num_consensus_msgs = self.consensus_instances.len();

        if remaining > 0 {
            warn!("PROPOSER: Header at height {} capped at {} digests ({} remaining for next header)",
                  self.height, num_batches, remaining);
        }

        debug!("Creating header at height {} with {} batches, {} consensus instances",
               self.height, num_batches, num_consensus_msgs);

        let mut header = Header::new(
                self.name,
                self.height,
                included,
                self.last_parent.clone().unwrap(),
                &mut self.signature_service,
                self.consensus_instances.clone(),
                self.num_active_instances,
            ).await;

        DISSEMINATION_HEADERS_CREATED_TOTAL.inc();
        record_header_created(&header.id, now_ms());

        if self.is_special {
            header.special = true;
            debug!("Header at height {} is SPECIAL", self.height);
        }

        #[cfg(feature = "benchmark")]
        for digest in header.payload.keys() {
            // NOTE: This log entry is used to compute performance.
            info!("Created {} -> {:?}", header, digest);
        }

        // Reset last parent
        self.last_parent = None;
        // Reset proposed consensus instances
        self.consensus_instances.clear();
        self.num_active_instances = 0;
      
        // Send the new header to the `Core` that will broadcast and process it.
        // Propose time tracking removed - now using slot-level latency

        match self.tx_core.try_send(header) {
            Ok(_) => {},
            Err(tokio::sync::mpsc::error::TrySendError::Full(h)) => {
                warn!("PROPOSER: tx_core channel FULL at height {}! Core may be overloaded processing headers", self.height);
                if let Err(e) = self.tx_core.send(h).await {
                    error!("PROPOSER: CRITICAL - Failed to send header at height {}: {}", self.height, e);
                }
            },
            Err(e) => {
                error!("PROPOSER: CRITICAL - Channel closed at height {}: {}", self.height, e);
            }
        }
    }

    // Main loop listening to incoming messages.
    pub async fn run(&mut self) {
        debug!("Proposer starting at height {}", self.height);

        let timer = sleep(Duration::from_millis(self.max_header_delay));
        tokio::pin!(timer);
        let mut current_time = Instant::now();
        
        // Stats tracking
        let mut headers_proposed: u64 = 0;
        let mut batches_included: u64 = 0;
        let mut last_stats_log = Instant::now();
        let mut waiting_for_parent_since: Option<Instant> = None;
        let mut waiting_for_batches_since: Option<Instant> = None;

        loop {
            // Check if we can propose a new header. We propose a new header when one of the following
            // conditions is met:
            // 1. We have a quorum of certificates from the previous round and enough batches' digests;
            // 2. We have a quorum of certificates from the previous round and the specified maximum
            // inter-header delay has passed.
            // 3. If it is a special block opportunity. That is when either a QC or TC from the previous view forms,
            // we have a ticket to propose a new block
            let enough_parent = self.last_parent.is_some();
            let enough_digests = self.payload_size >= self.header_size;
            let timer_expired = timer.is_elapsed();

            if (timer_expired || enough_digests) && (enough_parent || self.is_special) {
                if timer_expired {
                    debug!("Timer expired for height {} - proposing with {} batches ({} bytes)", 
                           self.height, self.digests.len(), self.payload_size);
                }

                let elapsed = current_time.elapsed().as_millis();
                debug!("Proposing header at height {} after {:?} ms (special={})", 
                       self.height, elapsed, self.is_special);
                current_time = Instant::now();
                
                // Make a new header (may only include up to MAX_HEADER_DIGESTS).
                let pre_drain_count = self.digests.len();
                self.make_header().await;
                let included_count = pre_drain_count - self.digests.len();
                batches_included += included_count as u64;
                headers_proposed += 1;
                // Recompute payload_size from remaining digests (each is 32 bytes).
                self.payload_size = self.digests.iter().map(|(d, _)| d.size()).sum();
                
                // Reset wait timers
                waiting_for_parent_since = None;
                waiting_for_batches_since = None;

                // Reschedule the timer.
                let deadline = Instant::now() + Duration::from_millis(self.max_header_delay);
                timer.as_mut().reset(deadline);
            } else {
                // Track how long we're waiting for resources
                if !enough_parent && waiting_for_parent_since.is_none() {
                    waiting_for_parent_since = Some(Instant::now());
                }
                if enough_parent && !enough_digests && waiting_for_batches_since.is_none() {
                    waiting_for_batches_since = Some(Instant::now());
                }
                
                // Warn if stuck waiting too long
                if let Some(wait_start) = waiting_for_parent_since {
                    if wait_start.elapsed().as_secs() >= 3 {
                        warn!("PROPOSER: Waiting for parent certificate for {} seconds at height {}", 
                              wait_start.elapsed().as_secs(), self.height);
                        waiting_for_parent_since = Some(Instant::now()); // Reset to avoid spam
                    }
                }
                if let Some(wait_start) = waiting_for_batches_since {
                    if wait_start.elapsed().as_secs() >= 3 {
                        warn!("PROPOSER: Waiting for batches for {} seconds at height {} (have {} bytes of {} needed)", 
                              wait_start.elapsed().as_secs(), self.height, self.payload_size, self.header_size);
                        waiting_for_batches_since = Some(Instant::now()); // Reset to avoid spam
                    }
                }
            }

    
            tokio::select! {
                // Received info from consensus
                Some(info) = self.rx_instance.recv() => {
                    match &info {
                        ConsensusMessage::Prepare { slot, view: _, tc: _, qc_ticket: _, proposals: _} => {
                            if self.use_special_rule {
                                self.is_special = true;
                            }
                            self.num_active_instances +=1;
                            debug!("Received Prepare for slot {} (active instances: {})", slot, self.num_active_instances);
                        },
                        ConsensusMessage::Confirm { slot, view: _, qc: _, proposals: _} => {
                            if self.use_special_rule {
                                self.is_special = true;
                            }
                            self.num_active_instances +=1;
                            debug!("Received Confirm for slot {} (active instances: {})", slot, self.num_active_instances);
                        },
                        _ => {},
                    }

                    self.consensus_instances.insert(info.digest(), info);
                }

                // Receive own certificate from core (we are the author)
                Some(parent) = self.rx_core.recv() => {
                    if parent.height < self.height {
                        debug!("Ignoring stale parent from height {} (current: {})", parent.height, self.height);
                        continue;
                    }

                    // Advance to the next height.
                    self.height += 1;
                    debug!("Chain advanced to height {} (parent from height {})", self.height, parent.height);

                    // Signal that we have a parent certificates to propose a new header.
                    self.last_parent = Some(parent.clone());
                    waiting_for_parent_since = None;
                }

                Some((digest, worker_id)) = self.rx_workers.recv() => {
                    self.payload_size += digest.size();
                    self.digests.push((digest, worker_id));
                    
                    if waiting_for_batches_since.is_some() && self.payload_size >= self.header_size {
                        debug!("Batch threshold reached: {} bytes from {} batches", 
                               self.payload_size, self.digests.len());
                        waiting_for_batches_since = None;
                    }
                }
                () = &mut timer => {
                    // Nothing to do - handled above
                }
            }
            
            // Log aggregate stats every 10 seconds
            if last_stats_log.elapsed().as_secs() >= 10 {
                let header_rate = headers_proposed as f64 / last_stats_log.elapsed().as_secs_f64();
                let batch_rate = batches_included as f64 / last_stats_log.elapsed().as_secs_f64();
                info!("PROPOSER: Proposed {} headers ({:.2} hdr/s) with {} batches ({:.1} batch/s) in last {:.1}s - Current height: {}", 
                      headers_proposed, header_rate, batches_included, batch_rate, 
                      last_stats_log.elapsed().as_secs_f64(), self.height);
                
                // Channel capacity monitoring (only RX channels - Sender doesn't expose len())
                let rx_workers_remaining = self.rx_workers.capacity() - self.rx_workers.len();
                let rx_workers_pct = (self.rx_workers.len() as f64 / self.rx_workers.capacity() as f64) * 100.0;
                let rx_core_remaining = self.rx_core.capacity() - self.rx_core.len();
                let rx_core_pct = (self.rx_core.len() as f64 / self.rx_core.capacity() as f64) * 100.0;
                
                info!("PROPOSER CHANNELS: rx_workers {}/{} ({:.1}% full, {} remaining), rx_core {}/{} ({:.1}% full, {} remaining)",
                      self.rx_workers.len(), self.rx_workers.capacity(), rx_workers_pct, rx_workers_remaining,
                      self.rx_core.len(), self.rx_core.capacity(), rx_core_pct, rx_core_remaining);
                
                // Warn if channels getting full
                if rx_workers_pct > 80.0 {
                    warn!("PROPOSER: rx_workers channel {:.1}% full - workers sending batches faster than we can propose!",
                          rx_workers_pct);
                }
                if rx_core_pct > 80.0 {
                    warn!("PROPOSER: rx_core channel {:.1}% full - certificates backing up!",
                          rx_core_pct);
                }
                
                headers_proposed = 0;
                batches_included = 0;
                last_stats_log = Instant::now();
            }
        }
    }
}

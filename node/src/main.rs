#![allow(dead_code)]
#![allow(unused_variables)]
#![allow(unused_imports)]
// Copyright(C) Facebook, Inc. and its affiliates.
use anyhow::{Context, Result};
use clap::{crate_name, crate_version, App, AppSettings, ArgMatches, SubCommand};
use config::Export as _;
use config::Import as _;
use config::{Committee, KeyPair, Parameters, WorkerId};
use crypto::SignatureService;
use env_logger::Env;
use primary::Header;
use primary::Primary;
use primary::metrics::flush_interval_metrics;
use store::Store;
use tokio::sync::mpsc::{channel, Receiver};
use worker::Worker;
use std::net::SocketAddr;
use std::time::Duration;

use hyper::service::{make_service_fn, service_fn};
use hyper::{Body, Method, Request, Response, Server, StatusCode};
use prometheus::{default_registry, Encoder, TextEncoder, Registry, register_gauge, Gauge};
use std::sync::Arc;

/// The default channel capacity.
pub const CHANNEL_CAPACITY: usize = 1_000;

#[tokio::main]
async fn main() -> Result<()> {
    //std::env::set_var("RUST_BACKTRACE", "1");
    
    let matches = App::new(crate_name!())
        .version(crate_version!())
        .about("A research implementation of Sailfish.")
        .args_from_usage("-v... 'Sets the level of verbosity'")
        .subcommand(
            SubCommand::with_name("generate_keys")
                .about("Print a fresh key pair to file")
                .args_from_usage("--filename=<FILE> 'The file where to print the new key pair'"),
        )
        .subcommand(
            SubCommand::with_name("run")
                .about("Run a node")
                .args_from_usage("--keys=<FILE> 'The file containing the node keys'")
                .args_from_usage("--committee=<FILE> 'The file containing committee information'")
                .args_from_usage("--parameters=[FILE] 'The file containing the node parameters'")
                .args_from_usage("--store=<PATH> 'The path where to create the data store'")
                .args_from_usage("--metrics-address=[ADDR] 'HTTP address to expose Prometheus metrics, e.g. 0.0.0.0:9100'")
                .args_from_usage("--metrics-file=[FILE] 'If set, periodically flush /metrics to timestamped files using this as base name'" )
                .args_from_usage("--metrics-flush-interval-ms=[INT] 'Flush period to write metrics to file (default 5000 ms)'")
                .subcommand(SubCommand::with_name("primary").about("Run a single primary"))
                .subcommand(
                    SubCommand::with_name("worker")
                        .about("Run a single worker")
                        .args_from_usage("--id=<INT> 'The worker id'"),
                )
                .setting(AppSettings::SubcommandRequiredElseHelp),
        )
        .setting(AppSettings::SubcommandRequiredElseHelp)
        .get_matches();

    let log_level = match matches.occurrences_of("v") {
        0 => "error",
        1 => "warn",
        2 => "info",
        3 => "debug",
        _ => "trace",
    };
    let mut logger = env_logger::Builder::from_env(Env::default().default_filter_or(log_level));
    #[cfg(feature = "benchmark")]
    logger.format_timestamp_millis();
    logger.init();

    match matches.subcommand() {
        ("generate_keys", Some(sub_matches)) => KeyPair::new()
            .export(sub_matches.value_of("filename").unwrap())
            .context("Failed to generate key pair")?,
        ("run", Some(sub_matches)) => run(sub_matches).await?,
        _ => unreachable!(),
    }
    Ok(())
}

// Runs either a worker or a primary.
async fn run(matches: &ArgMatches<'_>) -> Result<()> {
    let key_file = matches.value_of("keys").unwrap();
    let committee_file = matches.value_of("committee").unwrap();
    let parameters_file = matches.value_of("parameters");
    let store_path = matches.value_of("store").unwrap();

    // Metrics: setup exporter and optional file flusher
    let metrics_addr: Option<SocketAddr> = matches
        .value_of("metrics-address")
        .and_then(|s| s.parse().ok());
    let metrics_file: Option<String> = matches.value_of("metrics-file").map(|s| s.to_string());
    let flush_interval_ms: u64 = matches
        .value_of("metrics-flush-interval-ms")
        .and_then(|s| s.parse().ok())
        .unwrap_or(5000);

    // Use the global default Prometheus registry for the process so other crates can register easily.
    let registry: &Registry = default_registry();
    let registry = Arc::new(registry.clone());
    start_metrics_http_exporter(metrics_addr, registry.clone()).await?;
    start_metrics_file_flusher(metrics_file, flush_interval_ms, registry.clone()).await?;

    // Read the committee and node's keypair from file.
    let keypair = KeyPair::import(key_file).context("Failed to load the node's keypair")?;
    let name = keypair.name;
    let committee =
        Committee::import(committee_file).context("Failed to load the committee information")?;

    // Load default parameters if none are specified.
    let parameters = match parameters_file {
        Some(filename) => {
            Parameters::import(filename).context("Failed to load the node's parameters")?
        }
        None => Parameters::default(),
    };

    // The `SignatureService` provides signatures on input digests.
    let signature_service = SignatureService::new(keypair.secret);

    // Make the data store.
    let store = Store::new(store_path).context("Failed to create a store")?;

    // Channels the sequence of certificates.
    let (tx_output, rx_output) = channel(CHANNEL_CAPACITY);

    // Channel for sending headers between DAG and Consensus
    let (tx_sailfish, rx_sailfish) = channel(CHANNEL_CAPACITY);

    // Channel for sending loopback headerds that completed validation between DAG and Consensus
    //let (tx_validation, rx_validation) = channel(CHANNEL_CAPACITY);

    // Channel for indicating commit and that new header should be proposed
    //let (tx_ticket, rx_ticket) = channel(CHANNEL_CAPACITY);

    // Check whether to run a primary, a worker, or an entire authority.
    //Note: Each node has at most one worker. Workers that don't include a primary (e.g. are not an entire authority) use PrimaryConnector to connect to a designated primary.
    match matches.subcommand() {
        // Spawn the primary and consensus core.
        ("primary", _) => {
            let (tx_new_certificates, rx_new_certificates) = channel(CHANNEL_CAPACITY);
            let (tx_feedback, rx_feedback) = channel(CHANNEL_CAPACITY);
            let (tx_committer, rx_committer) = channel(CHANNEL_CAPACITY);
            let (tx_pushdown_cert, rx_pushdown_cert) = channel(CHANNEL_CAPACITY);
            let(tx_request_header_sync, rx_request_header_sync) = channel(CHANNEL_CAPACITY);

            Primary::spawn(
                name,
                committee.clone(),
                parameters.clone(),
                signature_service.clone(),
                store.clone(),
                /* tx_consensus */ tx_new_certificates,
                tx_committer,
                rx_committer,
                /* rx_consensus */ rx_feedback,
                tx_sailfish,
                //rx_ticket,
                rx_pushdown_cert,
                rx_request_header_sync,
                tx_output,
            );
            /*Consensus::spawn(
                name,
                committee,
                parameters,
                signature_service,
                store,
                /* rx_consensus */ rx_new_certificates,
                rx_committer,
                /* tx_mempool */ tx_feedback,
                tx_output,
                tx_ticket,
                tx_validation,
                rx_sailfish,
                tx_pushdown_cert,
                tx_request_header_sync,
            );*/
        }

        // Spawn a single worker.
        ("worker", Some(sub_matches)) => {
            let id = sub_matches
                .value_of("id")
                .unwrap()
                .parse::<WorkerId>()
                .context("The worker id must be a positive integer")?;
            Worker::spawn(keypair.name, id, committee, parameters, store);
        }
        _ => unreachable!(),
    }

    // Analyze the consensus' output.
    analyze(rx_output).await;

    // If this expression is reached, the program ends and all other tasks terminate.
    unreachable!();
}

/// Receives an ordered list of certificates and apply any application-specific logic.
async fn analyze(mut rx_output: Receiver<Header>) {
    while let Some(_header) = rx_output.recv().await {
        // NOTE: Here goes the application logic.
    }
}

async fn start_metrics_http_exporter(
    addr: Option<SocketAddr>,
    registry: Arc<Registry>,
) -> Result<()> {
    if addr.is_none() {
        return Ok(());
    }
    let bind_addr = addr.unwrap();

    let make_svc = make_service_fn(move |_| {
        let registry = registry.clone();
        async move {
            Ok::<_, hyper::Error>(service_fn(move |req: Request<Body>| {
                let registry = registry.clone();
                async move {
                    match (req.method(), req.uri().path()) {
                        (&Method::GET, "/metrics") => {
                            let encoder = TextEncoder::new();
                            let metric_families = registry.gather();
                            let mut buffer = Vec::new();
                            encoder.encode(&metric_families, &mut buffer).unwrap();
                            Ok::<_, hyper::Error>(Response::new(Body::from(buffer)))
                        }
                        _ => {
                            let mut not_found = Response::default();
                            *not_found.status_mut() = StatusCode::NOT_FOUND;
                            Ok::<_, hyper::Error>(not_found)
                        }
                    }
                }
            }))
        }
    });

    tokio::spawn(async move {
        if let Err(e) = Server::bind(&bind_addr).serve(make_svc).await {
            log::error!("metrics HTTP server error: {}", e);
        }
    });
    Ok(())
}

async fn start_metrics_file_flusher(
    metrics_file: Option<String>,
    flush_interval_ms: u64,
    registry: Arc<Registry>,
) -> Result<()> {
    if metrics_file.is_none() {
        return Ok(());
    }
    let base_path = metrics_file.unwrap();
    // Register a gauge once that we will update on every flush with the current timestamp (ms).
    let flush_ts_gauge: Gauge = register_gauge!(
        "node_metrics_flush_timestamp_ms",
        "Timestamp (ms since epoch) of this process' last metrics flush"
    ).expect("failed to register node_metrics_flush_timestamp_ms");
    tokio::spawn(async move {
        let encoder = TextEncoder::new();
        loop {
            // Calculate averages and reset flush interval metrics before gathering
            flush_interval_metrics();
            
            let metric_families = registry.gather();
            let mut buffer = Vec::new();
            if encoder.encode(&metric_families, &mut buffer).is_ok() {
                // Build timestamped filename: <stem>-<unix_ms>.<ext> in the same directory
                use std::path::{Path, PathBuf};
                use std::time::{SystemTime, UNIX_EPOCH};
                let base: &Path = Path::new(&base_path);
                let dir: &Path = base.parent().unwrap_or(Path::new("."));
                let stem: &str = base.file_stem().and_then(|s| s.to_str()).unwrap_or("metrics");
                let ext: &str = base.extension().and_then(|e| e.to_str()).unwrap_or("prom");
                let ts_ms = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis()).unwrap_or(0);
                // Update the flush timestamp gauge
                flush_ts_gauge.set(ts_ms as f64);
                let filename = format!("{}-{}.{}", stem, ts_ms, ext);
                let path_out: PathBuf = dir.join(filename);
                let _ = tokio::fs::write(path_out, buffer).await; // best-effort
            }
            tokio::time::sleep(Duration::from_millis(flush_interval_ms)).await;
        }
    });
    Ok(())
}

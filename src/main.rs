mod config;
mod target_store;

use crate::target_store::TargetStore;
use anyhow::{Context, Error};
use config::AppConfig;
use futures_util::stream::FuturesUnordered;
use socket2::{Domain, Type};
use std::time::{Duration, Instant};
use std::{net::SocketAddr, sync::Arc};
use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tracing::{debug, info, trace, warn};

const CONFIG_PATH: &str = "config.json";
const SO_BACKLOG: i32 = 10;
const TARGET_RETRY_AFTER_MILLIS: u64 = 1000; // TODO: make configurable
const TARGET_SELECTION_TIMEOUT_MILLIS: u64 = 6000; // TODO: make configurable
const TARGET_CONNECT_TIMEOUT_MILLIS: u64 = 2000; // TODO: make configurable

fn init_logging() {
    let filter =
        tracing_subscriber::filter::EnvFilter::try_from_env("RUST_LOG").unwrap_or_default();
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_file(false)
        .with_line_number(false)
        .with_target(false)
        .init();
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    init_logging();
    let config = config::AppsContainer::load_config(CONFIG_PATH).await?;

    let apps = config
        .apps
        .into_iter()
        .map(|app| Ok::<_, Error>((bind(&app)?, app)))
        .collect::<Result<Vec<_>, Error>>()?;
    info!("bind OK");

    let handlers: Vec<JoinHandle<Result<(), Error>>> = apps
        .into_iter()
        .map(|(listeners, app)| tokio::spawn(execute_app(listeners, app.targets)))
        .collect();
    info!("Listening");

    // TODO: graceful shutdown
    for handler in handlers {
        handler.await??;
    }
    Ok(())
}

fn ports_to_ipv6_socket_addrs(ports: &[u16]) -> Vec<SocketAddr> {
    use std::net::{IpAddr, Ipv6Addr};
    ports
        .iter()
        .map(|port| SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 0)), *port))
        .collect()
}

fn bind(app: &AppConfig) -> Result<Vec<tokio::net::TcpListener>, Error> {
    let socket_addrs = ports_to_ipv6_socket_addrs(&app.ports);
    debug!("Binding to: {socket_addrs:?}");
    socket_addrs
        .into_iter()
        .map(|socket_addr| {
            trace!("Binding to {socket_addr}");
            bind_one_pair(socket_addr)
        })
        .collect()
}

/// Bind to one port, both IPv4 and IPv6
fn bind_one_pair(socket_addr: SocketAddr) -> Result<tokio::net::TcpListener, Error> {
    let socket = socket2::Socket::new(Domain::IPV6, Type::STREAM, None)
        .context("error configuring the socket")?;
    socket
        .set_only_v6(false)
        .context("error turning off set_only_v6")?;
    socket
        .set_reuse_address(true)
        .context("error setting SO_REUSEADDR")?;
    socket
        .bind(&socket_addr.into())
        .with_context(|| format!("error binding to {socket_addr}"))?;

    socket.listen(SO_BACKLOG).context("error while listening")?;

    let std_listener: std::net::TcpListener = socket.into();
    std_listener
        .set_nonblocking(true)
        .context("error while set_nonblocking")?;

    let tokio_listener = tokio::net::TcpListener::from_std(std_listener)
        .context("cannot wrap with tokio listener")?;
    Ok(tokio_listener)
}

/// Task representing single app.
async fn execute_app(
    listeners: Vec<tokio::net::TcpListener>,
    targets: Vec<String>,
) -> Result<(), Error> {
    use futures_util::StreamExt;

    let targets = Arc::new(TargetStore::new(
        targets,
        Duration::from_millis(TARGET_RETRY_AFTER_MILLIS),
    ));
    let mut futures = FuturesUnordered::new();
    for listener in listeners {
        futures.push(accept_spawn(listener, targets.clone()));
    }
    while let Some(listener) = futures.next().await {
        // loop forever
        futures.push(accept_spawn(listener, targets.clone()));
    }
    unreachable!("app must have at least one listener")
}

// Task that represents one listener
async fn accept_spawn(
    listener: tokio::net::TcpListener,
    targets: Arc<TargetStore>,
) -> tokio::net::TcpListener {
    trace!("Accepting connection on {listener:?}");
    let (inbound, client_addr) = match listener.accept().await {
        Ok((inbound, client_addr)) => (inbound, client_addr),
        Err(err) => {
            warn!("Accept failed on {listener:?}: {err:?}");
            return listener;
        }
    };
    debug!("[{client_addr}] Client connected to {listener:?}");
    {
        let targets = targets.clone();
        tokio::spawn(async move {
            match timeout(
                Duration::from_millis(TARGET_SELECTION_TIMEOUT_MILLIS),
                pick_target(targets, client_addr),
            )
            .await
            {
                Ok((outbound, target_addr)) => {
                    debug!("[{client_addr}] Targetting {target_addr}");
                    transfer(inbound, outbound, client_addr).await;
                }
                Err(_) => {
                    warn!("[{client_addr}] Target selection timeout reached");
                }
            }
        });
    }
    // Backpressure: do not listen again until a target is picked and connected
    let before_wait = Instant::now();
    while !targets.has_next() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let wait_secs = (Instant::now() - before_wait).as_secs();
    if wait_secs > 0 {
        info!(
            "Target is available, listening resumed after {}s",
            wait_secs
        );
    }
    return listener;
}

// This function can be cancelled because of a timeout.
async fn pick_target(targets: Arc<TargetStore>, client_addr: SocketAddr) -> (TcpStream, String) {
    loop {
        // pick target
        let handler = loop {
            match targets.next() {
                Some(handler) => break handler,
                None => {
                    debug!("[{client_addr}] Waiting for a target to become available");
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        };
        // connect
        let target_addr = handler.address().to_string();
        let outbound = match timeout(
            Duration::from_millis(TARGET_CONNECT_TIMEOUT_MILLIS),
            TcpStream::connect(&target_addr),
        )
        .await
        {
            Ok(Ok(outbound)) => Ok(outbound),
            Ok(Err(source)) => Err(source.to_string()),
            Err(source) => Err(source.to_string()),
        };
        match outbound {
            Ok(outbound) => {
                handler.mark_success();
                return (outbound, target_addr);
            }
            Err(err) => {
                warn!("[{client_addr}] Temporarily disabling {target_addr} for {TARGET_RETRY_AFTER_MILLIS}ms; {err}");
                handler.invalidate();
                // retry
            }
        }
    }
}

/// Task that processes a single connection.
async fn transfer(mut inbound: TcpStream, mut outbound: TcpStream, client_addr: SocketAddr) {
    let (mut ri, mut wi) = inbound.split();
    let (mut ro, mut wo) = outbound.split();
    let client_to_server = async {
        tokio::io::copy(&mut ri, &mut wo).await?;
        wo.shutdown().await
    };
    let server_to_client = async {
        tokio::io::copy(&mut ro, &mut wi).await?;
        wi.shutdown().await
    };
    if let Err(err) = tokio::try_join!(client_to_server, server_to_client) {
        warn!("[{client_addr:?}] Failed to transfer; {err:?}");
    };
}

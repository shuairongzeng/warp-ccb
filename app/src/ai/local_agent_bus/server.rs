//! JSONL Socket Server — background thread that accepts connections from CCB clients
//!
//! Phase 1: TCP loopback on all platforms (simplifies cross-platform support).

use std::sync::Arc;

use crate::ai::local_agent_bus::protocol::{BusCommand, BusRequest, BusResponse};

/// Run the JSONL socket server.
///
/// Listens on TCP loopback, validates auth tokens,
/// and forwards commands to the main thread via mpsc channel.
pub struct LocalAgentBusServer;

impl LocalAgentBusServer {
    pub async fn run(
        expected_token: &str,
        cmd_tx: tokio::sync::mpsc::Sender<(BusCommand, tokio::sync::oneshot::Sender<BusResponse>)>,
        addr_tx: std::sync::mpsc::Sender<String>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        // Phase 1: TCP loopback with random port on all platforms
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let local_addr = listener.local_addr()?;
        let actual_addr = format!("{}", local_addr);
        log::info!("LocalAgentBus listening on {}", local_addr);

        // Report actual address back to caller so bus-address.json gets the real port
        let _ = addr_tx.send(actual_addr);

        let expected_token = Arc::new(expected_token.to_string());

        loop {
            match listener.accept().await {
                Ok((stream, _addr)) => {
                    let tx = cmd_tx.clone();
                    let token = expected_token.clone();
                    tokio::spawn(async move {
                        if let Err(e) = Self::handle_connection(stream, &token, &tx).await {
                            log::warn!("Bus client connection error: {}", e);
                        }
                    });
                }
                Err(e) => {
                    log::error!("Bus accept error: {}", e);
                }
            }
        }
    }

    async fn handle_connection(
        mut stream: tokio::net::TcpStream,
        expected_token: &str,
        cmd_tx: &tokio::sync::mpsc::Sender<(BusCommand, tokio::sync::oneshot::Sender<BusResponse>)>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

        let (reader, mut writer) = stream.split();
        let mut reader = BufReader::new(reader);
        let mut line = String::new();

        loop {
            line.clear();
            match reader.read_line(&mut line).await {
                Ok(0) => break, // EOF
                Ok(_) => {
                    let trimmed = line.trim();
                    if trimmed.is_empty() {
                        continue;
                    }

                    let response = match serde_json::from_str::<BusRequest>(trimmed) {
                        Ok(req) => {
                            if req.token != expected_token {
                                BusResponse::error("invalid auth token")
                            } else if req.v
                                != crate::ai::local_agent_bus::protocol::PROTOCOL_VERSION
                            {
                                BusResponse::error(format!(
                                    "unsupported protocol version: {} (expected {})",
                                    req.v,
                                    crate::ai::local_agent_bus::protocol::PROTOCOL_VERSION
                                ))
                            } else {
                                let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
                                let timeout_duration = Self::command_timeout(&req.command);
                                if cmd_tx.send((req.command, reply_tx)).await.is_err() {
                                    BusResponse::error("bus model not available")
                                } else {
                                    match tokio::time::timeout(timeout_duration, reply_rx).await {
                                        Ok(Ok(resp)) => resp,
                                        Ok(Err(_)) => {
                                            BusResponse::error("response channel dropped")
                                        }
                                        Err(_) => BusResponse::error(format!(
                                            "request timed out ({}s)",
                                            timeout_duration.as_secs()
                                        )),
                                    }
                                }
                            }
                        }
                        Err(e) => BusResponse::error(format!("invalid JSON: {}", e)),
                    };

                    let mut resp_json = serde_json::to_string(&response)?;
                    resp_json.push('\n');
                    writer.write_all(resp_json.as_bytes()).await?;
                    writer.flush().await?;
                }
                Err(e) => {
                    log::debug!("Bus read error: {}", e);
                    break;
                }
            }
        }

        Ok(())
    }

    /// Determine timeout per command type. Wait gets a longer timeout, others 10s.
    fn command_timeout(cmd: &BusCommand) -> std::time::Duration {
        match cmd {
            BusCommand::Wait { timeout_ms, .. } => {
                let ms = timeout_ms.unwrap_or(300_000);
                std::time::Duration::from_millis(ms)
            }
            _ => std::time::Duration::from_secs(10),
        }
    }
}

//! LocalAgentBus — CCB-Warp multi-agent communication bridge
//!
//! Provides a JSONL-over-socket API that allows external CCB clients to:
//! - Ask: inject prompts into CLI agent panes
//! - Pend: query completion status and replies
//! - Ping: check agent session liveness
//! - ListSessions: enumerate active CLI agent sessions
//! - Cancel: mark in-flight requests as cancelled

pub mod completion;
pub mod protocol;
mod raw_output;
pub mod registry;
pub mod server;
pub mod store;

use std::collections::{HashMap, HashSet};

use completion::CompletionTracker;
use protocol::{
    BusAddressInfo, BusCommand, BusResponse, BusResponseData, ChainProgress, ChainStepResult,
    RequestStatus, SessionInfo, PROTOCOL_VERSION,
};
use raw_output::RawOutputCapture;
use registry::RequestRegistry;
use server::LocalAgentBusServer;
use store::ResponseStore;

use warpui::r#async::Timer;
use warpui::{Entity, EntityId, ModelContext, SingletonEntity, WeakViewHandle};

use crate::pane_group::PaneGroup;
use crate::terminal::cli_agent::CLIAgent;
use crate::terminal::cli_agent_sessions::listener::is_agent_supported;
use crate::terminal::cli_agent_sessions::CLIAgentSessionsModel;
use crate::terminal::view::TerminalView;

const CCB_OUTPUT_SETTLE_DELAY_MS: u64 = 1000;
const DEFAULT_BUS_INJECT_ENTER_DELAY_MS: u64 = 1000;
const EXPLICIT_REPLY_MAX_CONTENT_BYTES: usize = 4 * 1024 * 1024;

/// Events emitted by LocalAgentBusModel.
#[derive(Debug, Clone)]
pub enum LocalAgentBusEvent {
    ServerStarted {
        socket_path: String,
    },
    RequestCreated {
        req_id: String,
        provider: String,
    },
    RequestStatusChanged {
        req_id: String,
        old_status: RequestStatus,
        new_status: RequestStatus,
    },
    ServerStopped,
}

/// The singleton model that owns the LocalAgentBus lifecycle.
///
/// Runs on the Warp main thread. Receives commands via a bounded mpsc channel
/// from the background JSONL server thread.
pub struct LocalAgentBusModel {
    socket_path: String,
    registry: RequestRegistry,
    store: ResponseStore,
    completion: CompletionTracker,
    raw_output_capture: RawOutputCapture,
    terminal_handles: HashMap<EntityId, WeakViewHandle<TerminalView>>,
    /// Agent panes launched by LocalAgentBus, including agents without Warp session listeners.
    bus_launched_sessions: HashMap<EntityId, CLIAgent>,
    /// FIFO queue of requests waiting for a busy session to free up.
    request_queue: Vec<QueuedRequest>,
    /// Active chain executions.
    active_chains: HashMap<String, ChainState>,
    /// Handle to PaneGroup for creating new terminal panes.
    pane_group_handle: Option<WeakViewHandle<PaneGroup>>,
    /// Launch requests waiting for a new pane to be created.
    pending_launches: Vec<PendingLaunch>,
    /// Whether a pane creation via PaneGroup is currently in flight.
    pane_creation_in_progress: bool,
    /// Alias → view_id mapping (e.g. "writer" → pane EntityId).
    alias_to_view_id: HashMap<String, EntityId>,
    /// Consecutive pane creation attempts without success (bounds retries).
    pane_creation_attempts: u32,
    /// Wait clients: req_id -> list of oneshot senders waiting for completion.
    waiting_clients: HashMap<String, Vec<tokio::sync::oneshot::Sender<BusResponse>>>,
    cmd_rx: tokio::sync::mpsc::Receiver<(BusCommand, tokio::sync::oneshot::Sender<BusResponse>)>,
}

impl Entity for LocalAgentBusModel {
    type Event = LocalAgentBusEvent;
}

impl SingletonEntity for LocalAgentBusModel {}

impl LocalAgentBusModel {
    /// Create and initialize the LocalAgentBus.
    pub fn new(base_dir: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        use rand::Rng;

        let auth_token: String = {
            let mut rng = rand::thread_rng();
            (0..32)
                .map(|_| format!("{:02x}", rng.gen::<u8>()))
                .collect()
        };

        // Create base directory
        let response_dir = base_dir.join("responses");
        std::fs::create_dir_all(&response_dir)?;

        // Bounded channel: server thread -> main thread
        let (cmd_tx, cmd_rx) = tokio::sync::mpsc::channel(16);

        // Channel for server to report its actual bound address
        let (addr_tx, addr_rx) = std::sync::mpsc::channel::<String>();

        // Start JSONL server in background
        let server_token = auth_token.clone();
        let base_dir_for_addr = base_dir.to_path_buf();
        std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("failed to create tokio runtime for bus server");
            rt.block_on(async move {
                if let Err(e) = LocalAgentBusServer::run(&server_token, cmd_tx, addr_tx).await {
                    log::error!("LocalAgentBus server error: {}", e);
                }
            });
        });

        // Wait for the server to report its actual address (should be near-instant)
        let actual_socket_path = addr_rx
            .recv_timeout(std::time::Duration::from_secs(5))
            .map_err(|e| format!("LocalAgentBus server failed to start: {}", e))?;

        // Write bus-address.json with the real address
        let pid = std::process::id();
        let created_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let addr_info = BusAddressInfo {
            protocol_version: PROTOCOL_VERSION,
            pid,
            socket_path: actual_socket_path.clone(),
            auth_token: auth_token.clone(),
            created_at_ms,
        };

        let addr_json = serde_json::to_string_pretty(&addr_info)?;
        // Write the default bus-address.json for backward compatibility
        let default_path = base_dir_for_addr.join("bus-address.json");
        let _ = std::fs::write(&default_path, &addr_json);
        // Also write a per-PID file for multi-instance support
        let pid_path = base_dir_for_addr.join(format!("bus-address-{}.json", pid));
        std::fs::write(&pid_path, addr_json)?;

        log::info!(
            "LocalAgentBus initialized at {} (pid={})",
            actual_socket_path,
            pid
        );

        Ok(Self {
            socket_path: actual_socket_path,
            registry: RequestRegistry::new(),
            store: ResponseStore::new(response_dir),
            completion: CompletionTracker::new(),
            raw_output_capture: RawOutputCapture::new(),
            terminal_handles: HashMap::new(),
            bus_launched_sessions: HashMap::new(),
            request_queue: Vec::new(),
            active_chains: HashMap::new(),
            pane_group_handle: None,
            pending_launches: Vec::new(),
            pane_creation_in_progress: false,
        alias_to_view_id: HashMap::new(),
            pane_creation_attempts: 0,
            waiting_clients: HashMap::new(),
            cmd_rx,
        })
    }

    /// Register a weak reference to a TerminalView for later prompt injection.
    /// Called from TerminalView when a CLI agent session starts.
    pub fn register_terminal_handle(
        &mut self,
        view_id: EntityId,
        handle: WeakViewHandle<TerminalView>,
    ) {
        log::info!(
            "LocalAgentBus: registered terminal handle for view {:?}",
            view_id
        );
        self.terminal_handles.insert(view_id, handle);
        self.raw_output_capture.register_pane(view_id);
    }

    /// Remove a terminal handle when the session ends.
    pub fn deregister_terminal_handle(&mut self, view_id: EntityId) {
        self.terminal_handles.remove(&view_id);
        // Remove alias mappings pointing to this view_id
        self.alias_to_view_id.retain(|_alias, &mut vid| vid != view_id);
        let removed_agent = self.bus_launched_sessions.remove(&view_id);
        log::info!(
            "CCB会话注销: pane={} removed_bus_provider={:?}",
            view_id,
            removed_agent.map(|agent| agent.command_prefix())
        );
        self.raw_output_capture.deregister_pane(view_id);
    }

    /// Append raw PTY bytes for a terminal pane.
    pub fn append_raw_output(&mut self, view_id: EntityId, bytes: &[u8]) {
        self.raw_output_capture.append(view_id, bytes);
    }

    /// Register the PaneGroup handle for creating new terminal panes.
    pub fn register_pane_group_handle(&mut self, handle: WeakViewHandle<PaneGroup>) {
        log::info!("LocalAgentBus: registered PaneGroup handle");
        self.pane_group_handle = Some(handle);
    }

    /// Called when a block completes — scan output for CCB_DONE markers.
    pub fn check_block_output_for_done(
        &mut self,
        view_id: EntityId,
        output: &str,
        ctx: &mut ModelContext<Self>,
    ) {
        for req_id in self.completion.pending_for_terminal(view_id) {
            let raw_output = self.raw_output_capture.snapshot(&req_id);
            let selected = select_reply_capture(
                &req_id,
                raw_output.as_deref(),
                Some(output),
                CaptureSource::BlockCompleted,
            );
            let Some(selected) = selected else {
                continue;
            };

            if !self.is_capture_output_stable(
                &req_id,
                selected.source,
                selected.output,
                "block_completed",
            ) {
                continue;
            }

            log::info!("LocalAgentBus: CCB_DONE detected for req {}", req_id);
            let will_finalize = !selected.reply.trim().is_empty();
            log_reply_capture_attempt(
                &req_id,
                selected.source,
                view_id,
                selected.output,
                selected.reply.len(),
                will_finalize,
            );
            if !will_finalize {
                write_reply_capture_debug_file(
                    &req_id,
                    selected.source,
                    selected.output,
                    &selected.reply,
                );
                log::info!(
                    "CCB诊断: block_completed 检测到完成标记但回复为空，保持 Running req_id={}",
                    req_id
                );
                continue;
            }
            let source = selected.source;
            let reply = selected.reply;
            let meta = ReplyCaptureMeta::from_source_and_reply(source, &reply);
            self.finalize_request_with_reply(&req_id, reply, meta, ctx);
        }
    }

    /// Start periodic polling of the command channel.
    pub fn start_command_poll(&mut self, ctx: &mut ModelContext<Self>) {
        self.schedule_command_poll(ctx);
    }

    fn schedule_command_poll(&self, ctx: &mut ModelContext<Self>) {
        let poll_interval = std::time::Duration::from_millis(100);
        ctx.spawn(
            async move { Timer::after(poll_interval).await },
            |model, _, ctx| {
                model.process_commands(ctx);
                model.schedule_command_poll(ctx);
            },
        );
    }

    /// Process pending commands from the server thread.
    pub fn process_commands(&mut self, ctx: &mut ModelContext<Self>) -> usize {
        let mut count = 0;
        while count < 32 {
            match self.cmd_rx.try_recv() {
                Ok((cmd, reply_tx)) => {
                    // Wait command uses deferred response — don't send immediately
                    if matches!(cmd, BusCommand::Wait { .. }) {
                        self.handle_wait(cmd, reply_tx, ctx);
                    } else {
                        let response = self.handle_command(cmd, ctx);
                        let _ = reply_tx.send(response);
                    }
                    count += 1;
                }
                Err(_) => break,
            }
        }

        // Check for completed sessions and mark corresponding requests as done.
        self.check_session_completions(ctx);

        // Scan terminal output for CCB_DONE markers.
        self.scan_terminal_outputs(ctx);

        // Advance chain executions and start next steps.
        self.advance_chains_and_continue(ctx);

        count
    }

    /// Scan terminal output grids for CCB_DONE markers and capture reply content.
    fn scan_terminal_outputs(&mut self, ctx: &mut ModelContext<Self>) {
        let pending_terminals = self.completion.pending_terminals();
        if pending_terminals.is_empty() {
            return;
        }

        for view_id in pending_terminals {
            let req_ids = self.completion.pending_for_terminal(view_id);
            if req_ids.is_empty() {
                continue;
            }

            let weak_handle = match self.terminal_handles.get(&view_id) {
                Some(h) => h.clone(),
                None => continue,
            };

            let handle = match weak_handle.upgrade(ctx) {
                Some(h) => h,
                None => continue,
            };

            // Read the full output grid (bypass display filters, all rows)
            // to ensure START/END markers are captured regardless of scroll
            // position or active filters.
            let output: Option<String> = handle.update(ctx, |view, _| {
                let model = view.model.lock();
                let block = model.block_list().active_block();
                Some(block.output_grid().contents_to_string(false, None))
            });

            if let Some(ref grid_output) = output {
                for req_id in &req_ids {
                    let raw_output = self.raw_output_capture.snapshot(req_id);
                    log_scan_tick_diagnostics(req_id, raw_output.as_deref(), Some(&grid_output));
                }

                for req_id in &req_ids {
                    let raw_output = self.raw_output_capture.snapshot(req_id);
                    let selected = select_reply_capture_for_scan(
                        req_id,
                        raw_output.as_deref(),
                        Some(&grid_output),
                    );
                    let Some(selected) = selected else {
                        continue;
                    };

                    if !self.is_capture_output_stable(
                        req_id,
                        selected.source,
                        selected.output,
                        "output_scan",
                    ) {
                        continue;
                    }

                    log::info!(
                        "LocalAgentBus: CCB_DONE detected via output scan for req {} source={:?}",
                        req_id,
                        selected.source
                    );
                    let will_finalize = !selected.reply.trim().is_empty();
                    log_reply_capture_attempt(
                        req_id,
                        selected.source,
                        view_id,
                        selected.output,
                        selected.reply.len(),
                        will_finalize,
                    );
                    log::info!(
                        "LocalAgentBus: captured reply ({} chars) for req {}",
                        selected.reply.len(),
                        req_id
                    );
                    let debug_path = std::env::temp_dir().join(format!("ccb_scan_{}.txt", req_id));
                    let _ = std::fs::write(
                        &debug_path,
                        format!(
                            "SOURCE={:?}\nREPLY_LEN={}\n---OUTPUT---\n{}\n---END---",
                            selected.source,
                            selected.reply.len(),
                            selected.output
                        ),
                    );
                    if !will_finalize {
                        write_reply_capture_debug_file(
                            req_id,
                            selected.source,
                            selected.output,
                            &selected.reply,
                        );
                        log::info!(
                            "LocalAgentBus: scan found done marker but empty reply for req {}, skipping",
                            req_id
                        );
                        continue;
                    }
                    let source = selected.source;
                    let reply = selected.reply;
                    let meta = ReplyCaptureMeta::from_source_and_reply(source, &reply);
                    self.finalize_request_with_reply(req_id, reply, meta, ctx);
                }
            }

            // Passive capture fallback: for requests where no markers were found,
            // check if raw output has stabilized long enough to treat as a reply.
            {
                for req_id in &req_ids {
                    // Skip if already finalized (marker scan succeeded).
                    if self
                        .registry
                        .get(req_id)
                        .map_or(true, |e| e.status != RequestStatus::Running)
                    {
                        continue;
                    }

                    let raw_output = self.raw_output_capture.snapshot(req_id);
                    if raw_output.as_ref().map_or(true, |s| s.is_empty()) {
                        continue;
                    }

                    // Adaptive completion: use CompletionTracker to decide whether
                    // the output has been stable long enough, based on session type.
                    let stable_duration = self
                        .completion
                        .output_stable_duration(req_id, raw_output.as_deref().unwrap_or(""));
                    let Some(stable_for) = stable_duration else {
                        continue;
                    };

                    if !self.completion.should_finalize_passive(req_id, stable_for) {
                        continue;
                    }

                    // Look up provider for provider-specific cleaning.
                    let provider = self
                        .registry
                        .get(req_id)
                        .map(|e| e.provider.clone())
                        .unwrap_or_default();

                    let grid_snap: Option<&str> = None; // Grid output was already tried above; raw is preferred for passive.
                    let candidate = build_passive_candidate(
                        req_id,
                        raw_output.as_deref(),
                        grid_snap.as_deref(),
                        &provider,
                    );
                    let Some((reply, meta)) = candidate else {
                        continue;
                    };

                    log::info!(
                        "CCB_PASSIVE: finalizing passive capture req_id={} source={:?} confidence={} reply_len={} stable_for={:?}",
                        req_id,
                        meta.source,
                        meta.confidence,
                        reply.len(),
                        stable_for,
                    );
                    let debug_path =
                        std::env::temp_dir().join(format!("ccb_passive_{}.txt", req_id));
                    let raw_snap = raw_output.unwrap_or_default();
                    let _ = std::fs::write(
                        &debug_path,
                        format!(
                            "SOURCE={:?}\nCONFIDENCE={}\nRAW_LEN={}\nFILTERED_LEN={}\nWARNINGS={:?}\n---RAW---\n{}\n---REPLY---\n{}\n---END---",
                            meta.source,
                            meta.confidence,
                            meta.raw_len,
                            meta.filtered_len,
                            meta.warnings,
                            &raw_snap[..raw_snap.len().min(2000)],
                            reply,
                        ),
                    );
                    self.finalize_request_with_reply(req_id, reply, meta, ctx);
                }
            }
        }

        // Process any queued requests now that terminals may be free.
        self.process_queued_requests(ctx);
    }

    /// Scan active requests and check if their sessions have completed.
    fn check_session_completions(&mut self, ctx: &mut ModelContext<Self>) {
        use crate::terminal::cli_agent_sessions::CLIAgentSessionStatus;

        let sessions_model = CLIAgentSessionsModel::as_ref(ctx);

        // Collect request IDs that should be marked as done.
        let mut completed = Vec::new();
        let mut errored = Vec::new();

        // Also clean up launch- placeholder entries once a CLIAgent session is detected.
        let mut launches_resolved = Vec::new();

        for (req_id, req) in self.registry.iter() {
            // Clean up launch- placeholders: once the CLIAgent session is detected,
            // the launch is successful and the terminal is ready for asks.
            if req_id.starts_with("launch-") && req.status == RequestStatus::Injecting {
                let view_id = req.terminal_view_id;
                if sessions_model.session(view_id).is_some()
                    || (self.bus_launched_sessions.contains_key(&view_id)
                        && self.terminal_handles.contains_key(&view_id))
                {
                    launches_resolved.push(req_id.clone());
                }
                continue;
            }

            if req.status != RequestStatus::Running {
                continue;
            }
            let view_id = req.terminal_view_id;
            if let Some(session) = sessions_model.session(view_id) {
                match session.status {
                    CLIAgentSessionStatus::Success => {
                        completed.push(req_id.clone());
                    }
                    CLIAgentSessionStatus::Blocked { .. } => {
                        // Agent is blocked — request still running, just waiting
                    }
                    CLIAgentSessionStatus::InProgress => {}
                }
            } else if self.bus_launched_sessions.contains_key(&view_id)
                && self.terminal_handles.contains_key(&view_id)
            {
                log::info!(
                    "LocalAgentBus: req {} is running in bus-launched terminal {}, no Warp session listener; keep scanning output",
                    req_id,
                    view_id
                );
            } else {
                // Session disappeared — mark as error
                errored.push(req_id.clone());
            }
        }

        for req_id in completed {
            let mut reply_text = String::new();
            let mut capture_source = CaptureSource::SessionCompletion;
            if let Some((reply, source)) = self.capture_reply_for_request(&req_id, ctx) {
                if !reply.is_empty() {
                    log::info!(
                        "LocalAgentBus: captured reply ({} chars) via session completion for req {}",
                        reply.len(),
                        req_id
                    );
                    self.registry.set_reply_content(&req_id, reply.clone());
                    reply_text = reply;
                    capture_source = source;
                }
            }
            // If reply is still empty, terminal output may not be fully rendered yet.
            // Don't finalize — let the next tick retry capture.
            if reply_text.is_empty() {
                log::info!(
                    "LocalAgentBus: reply still empty for req {}, will retry next tick",
                    req_id
                );
                continue;
            }
            let meta = ReplyCaptureMeta::from_source_and_reply(capture_source, &reply_text);
            self.finalize_request_with_reply(&req_id, reply_text, meta, ctx);
        }
        for req_id in errored {
            self.registry.update_status(&req_id, RequestStatus::Error);
            self.completion.deregister(&req_id);
            self.raw_output_capture.deregister_request(&req_id);
            self.notify_waiters(&req_id);
        }

        // Remove resolved launch- entries so the terminal is no longer marked busy.
        for req_id in launches_resolved {
            log::info!("LocalAgentBus: launch resolved, removing entry {}", req_id);
            self.registry.remove(&req_id);
        }

        self.process_queued_requests(ctx);
    }

    /// Attempt to capture reply content from the terminal output for a given request.
    /// Uses the full output grid and [CCB_START:xxx] ... [CCB_END:xxx] markers.
    fn capture_reply_for_request(
        &mut self,
        req_id: &str,
        ctx: &mut ModelContext<Self>,
    ) -> Option<(String, CaptureSource)> {
        log::info!("CCB_DEBUG capture_reply_for_request: req_id={}", req_id);
        let view_id = match self.registry.get(req_id) {
            Some(e) => e.terminal_view_id,
            None => {
                log::info!("CCB_DEBUG capture: req_id {} not in registry", req_id);
                return None;
            }
        };
        let weak_handle = match self.terminal_handles.get(&view_id) {
            Some(h) => h.clone(),
            None => {
                log::info!("CCB_DEBUG capture: no handle for view {:?}", view_id);
                return None;
            }
        };
        let handle = match weak_handle.upgrade(ctx) {
            Some(h) => h,
            None => return None,
        };

        let output: Option<String> = handle.update(ctx, |view, _| {
            let model = view.model.lock();
            let block = model.block_list().active_block();
            // Read all rows (None) to ensure START marker hasn't scrolled out of window.
            let output = block.output_grid().contents_to_string(false, None);

            Some(output)
        });

        let Some(grid_output) = output else {
            return None;
        };

        let raw_output = self.raw_output_capture.snapshot(req_id);
        log_scan_tick_diagnostics(req_id, raw_output.as_deref(), Some(&grid_output));

        // Debug: write terminal output to file for diagnosis
        let debug_path = std::env::temp_dir().join(format!("ccb_capture_{}.txt", req_id));
        let _ = std::fs::write(
            &debug_path,
            format!(
                "---RAW---\n{}\n---GRID---\n{}",
                raw_output.as_deref().unwrap_or(""),
                grid_output
            ),
        );
        log::info!(
            "CCB_DEBUG: wrote raw_len={} grid_len={} chars to {:?}",
            raw_output.as_deref().map(str::len).unwrap_or(0),
            grid_output.len(),
            debug_path
        );

        let selected = select_reply_capture(
            req_id,
            raw_output.as_deref(),
            Some(&grid_output),
            CaptureSource::SessionCompletion,
        )?;

        if !self.is_capture_output_stable(
            req_id,
            selected.source,
            selected.output,
            "session_completion",
        ) {
            return None;
        }

        if !selected.reply.is_empty() {
            return Some((selected.reply, selected.source));
        }

        None
    }

    fn is_capture_output_stable(
        &mut self,
        req_id: &str,
        source: CaptureSource,
        output: &str,
        stage: &str,
    ) -> bool {
        let stable_for = std::time::Duration::from_millis(CCB_OUTPUT_SETTLE_DELAY_MS);
        let stable = match source {
            CaptureSource::RawOutputScan | CaptureSource::PassiveRawOutput => self
                .raw_output_capture
                .is_request_stable(req_id, stable_for),
            _ => self
                .completion
                .is_output_length_stable(req_id, output, stable_for),
        };

        if !stable {
            log::info!(
                "CCB诊断: {} 检测到完成标记但输出长度尚未稳定，延迟确认 req_id={} source={:?} output_len={}",
                stage,
                req_id,
                source,
                output.len()
            );
        }

        stable
    }

    fn finalize_request_with_reply(
        &mut self,
        req_id: &str,
        reply: String,
        meta: ReplyCaptureMeta,
        ctx: &mut ModelContext<Self>,
    ) -> bool {
        if reply.trim().is_empty() {
            log::error!(
                "CCB诊断: 拒绝将空回复标记为 Success req_id={} source={:?}",
                req_id,
                meta.source
            );
            return false;
        }

        let (from_provider, receiver_terminal_view_id, callback_provider, caller_terminal_view_id) =
            match self.registry.get(req_id) {
                Some(entry) => (
                    entry.provider.clone(),
                    entry.terminal_view_id,
                    entry.callback_provider.clone(),
                    entry.caller_terminal_view_id,
                ),
                None => {
                    log::warn!("CCB诊断: finalize 时 registry 中找不到 req_id={}", req_id);
                    return false;
                }
            };

        log::info!(
            "CCB路由: 完成请求 req_id={} source={:?} confidence={} from={}#{} callback={:?}#{:?} reply_len={}",
            req_id,
            meta.source,
            meta.confidence,
            from_provider,
            receiver_terminal_view_id,
            callback_provider,
            caller_terminal_view_id,
            reply.len()
        );

        let finalized_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        self.registry.set_reply_content(req_id, reply.clone());
        self.registry.set_reply_metadata(
            req_id,
            Some(meta.source.to_string()),
            Some(meta.confidence),
            meta.warnings.clone(),
        );
        self.registry.update_status(req_id, RequestStatus::Success);
        self.persist_response(req_id, &meta, finalized_at_ms);
        self.completion.deregister(req_id);
        self.raw_output_capture.deregister_request(req_id);
        self.notify_waiters(req_id);
        self.deliver_callback(req_id, &from_provider, &reply, ctx);
        true
    }

    fn handle_command(&mut self, command: BusCommand, ctx: &mut ModelContext<Self>) -> BusResponse {
        match command {
            BusCommand::Ask {
                provider,
                prompt,
                session_id,
                cwd,
                req_id,
                caller,
                caller_terminal_view_id,
                caller_session_id,
                caller_cwd,
                queue,
            } => self.handle_ask(
                provider,
                prompt,
                session_id,
                cwd,
                req_id,
                caller,
                caller_terminal_view_id.and_then(entity_id_from_u64),
                caller_session_id,
                caller_cwd,
                queue,
                ctx,
            ),

            BusCommand::Pend {
                provider,
                session_id: _,
                cwd: _,
                count,
                req_id,
                chain_id,
            } => self.handle_pend(&provider, count, req_id.as_deref(), chain_id.as_deref()),

            BusCommand::Ping {
                provider,
                session_id,
                cwd,
            } => self.handle_ping(&provider, session_id.as_deref(), cwd.as_deref(), ctx),

            BusCommand::ListSessions { cwd } => self.handle_list_sessions(cwd.as_deref(), ctx),

            BusCommand::Cancel { req_id } => self.handle_cancel(&req_id, ctx),

            BusCommand::Launch {
                provider,
                alias,
                prompt,
                cwd,
                ..
            } => self.handle_launch(provider, alias, prompt, cwd, ctx),

            BusCommand::Chain { steps, caller } => self.handle_chain(steps, caller, ctx),

            BusCommand::Reply {
                req_id,
                content,
                caller,
                caller_terminal_view_id,
                cwd,
            } => self.handle_reply(req_id, content, caller, caller_terminal_view_id, cwd, ctx),

            BusCommand::Wait { .. } => {
                // Handled in process_commands before reaching here
                BusResponse::error("wait command not handled in dispatch")
            }

            BusCommand::ClosePane { view_id } => {
                self.handle_close_pane(view_id, ctx)
            }

            BusCommand::CloseSession { provider, view_id } => {
                self.handle_close_session(provider, view_id, ctx)
            }
        }
    }

    fn handle_reply(
        &mut self,
        req_id: String,
        content: String,
        _caller: Option<String>,
        caller_terminal_view_id: Option<u64>,
        _cwd: Option<String>,
        ctx: &mut ModelContext<Self>,
    ) -> BusResponse {
        let (status, terminal_view_id) = match self.registry.get(&req_id) {
            Some(entry) => (entry.status, entry.terminal_view_id),
            None => {
                return BusResponse::error(format!("reply request not found: {}", req_id));
            }
        };

        match validate_explicit_reply(status, terminal_view_id, caller_terminal_view_id, &content) {
            Ok(ExplicitReplyValidation::AlreadyFinalized) => {
                return BusResponse::ok(BusResponseData::ReplyAccepted {
                    req_id,
                    already_finalized: true,
                });
            }
            Ok(ExplicitReplyValidation::Finalize) => {}
            Err(message) => return BusResponse::error(message),
        }

        let meta = ReplyCaptureMeta::from_source_and_reply(CaptureSource::ExplicitReply, &content);
        self.finalize_request_with_reply(&req_id, content, meta, ctx);
        self.process_queued_requests(ctx);

        BusResponse::ok(BusResponseData::ReplyAccepted {
            req_id,
            already_finalized: false,
        })
    }

    fn handle_ask(
        &mut self,
        provider: String,
        prompt: String,
        session_id: Option<String>,
        cwd: Option<String>,
        req_id: String,
        caller: String,
        caller_terminal_view_id: Option<EntityId>,
        caller_session_id: Option<String>,
        caller_cwd: Option<String>,
        queue: bool,
        ctx: &mut ModelContext<Self>,
    ) -> BusResponse {
        if self.registry.get(&req_id).is_some() {
            return BusResponse::error(format!("duplicate req_id: {}", req_id));
        }

        let matched = self.find_session(&provider, session_id.as_deref(), cwd.as_deref(), ctx);

        match matched {
            FindSessionResult::NotFound => BusResponse::error(format!(
                "no active session for provider '{}' matching criteria",
                provider
            )),
            FindSessionResult::Ambiguous(sessions) => BusResponse::error(format!(
                "multiple sessions match for '{}'; specify session_id. Matching: {:?}",
                provider, sessions
            )),
            FindSessionResult::Found(entity_id) => {
                if Self::is_self_ask(&provider, &caller, caller_terminal_view_id, entity_id) {
                    log::info!(
                        "CCB路由: 跳过 self-ask req_id={} provider={} caller={} caller_pane={:?} target_pane={}",
                        req_id,
                        provider,
                        caller,
                        caller_terminal_view_id,
                        entity_id
                    );
                    return BusResponse::ok(BusResponseData::AskSkipped {
                        req_id,
                        provider,
                        reason: "self_request_skipped".to_string(),
                    });
                }

                if self.registry.has_active_for_terminal(entity_id) {
                    if queue {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        let callback_provider = Self::extract_callback(&caller, &provider);
                        let caller_terminal_view_id = match caller_terminal_view_id {
                            Some(id) => Some(id),
                            None => {
                                // Lightweight lookup only (no find_session to avoid re-entrancy)
                                self.alias_to_view_id.get(&caller)
                                    .filter(|id| self.terminal_handles.contains_key(id))
                                    .copied()
                                    .or_else(|| {
                                        resolve_agent(&caller).and_then(|agent| {
                                            self.bus_launched_sessions.iter()
                                                .find(|(_, a)| **a == agent)
                                                .filter(|(id, _)| self.terminal_handles.contains_key(id))
                                                .map(|(id, _)| *id)
                                        })
                                    })
                            }
                        };
                        self.registry.insert(registry::RequestEntry {
                            req_id: req_id.clone(),
                            provider: provider.clone(),
                            caller: caller.clone(),
                            terminal_view_id: entity_id,
                            session_id: session_id.clone(),
                            cwd: cwd.clone(),
                            status: RequestStatus::Queued,
                            created_at_ms: now_ms,
                            updated_at_ms: now_ms,
                            error_message: None,
                            reply_content: None,
                            reply_source: None,
                            reply_confidence: None,
                            reply_warnings: Vec::new(),
                            callback_provider,
                            caller_terminal_view_id,
                            caller_session_id,
                            caller_cwd,
                        });
                        self.request_queue.push(QueuedRequest {
                            req_id: req_id.clone(),
                            provider: provider.clone(),
                            prompt,
                            terminal_view_id: entity_id,
                        });
                        log::info!(
                            "CCB路由: 请求排队 req_id={} from={}#{:?} to={}#{}",
                            req_id,
                            caller,
                            caller_terminal_view_id,
                            provider,
                            entity_id
                        );
                        return BusResponse::ok(BusResponseData::AskAccepted {
                            req_id,
                            session_id,
                            provider,
                        });
                    }
                    return BusResponse::error("session_busy");
                }

                self.inject_and_register(
                    req_id,
                    provider,
                    caller,
                    caller_terminal_view_id,
                    caller_session_id,
                    caller_cwd,
                    prompt,
                    session_id,
                    cwd,
                    entity_id,
                    ctx,
                )
            }
        }
    }

    /// Common path: register request, inject prompt into PTY.
    fn inject_and_register(
        &mut self,
        req_id: String,
        provider: String,
        caller: String,
        caller_terminal_view_id: Option<EntityId>,
        caller_session_id: Option<String>,
        caller_cwd: Option<String>,
        prompt: String,
        session_id: Option<String>,
        cwd: Option<String>,
        entity_id: EntityId,
        ctx: &mut ModelContext<Self>,
    ) -> BusResponse {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let callback_provider = Self::extract_callback(&caller, &provider);

        // Auto-resolve caller_terminal_view_id when not provided by the caller.
        // This happens when the caller's pane doesn't have WARP_CCB_TERMINAL_VIEW_ID set
        // (e.g. user's own Claude pane, not launched by warp-ccb).
        // NOTE: We only do lightweight HashMap lookups here (alias_to_view_id, terminal_handles)
        // because find_session() calls refresh_bus_launched_sessions_from_terminal_outputs()
        // which does weak_handle.upgrade(ctx) -> handle.update(ctx) and can cause re-entrancy
        // panics when called from within inject_and_register (already in model update path).
        let caller_terminal_view_id = match caller_terminal_view_id {
            Some(id) => Some(id),
            None => {
                // Check alias_to_view_id for alias names like "qa", "writer"
                if let Some(&alias_id) = self.alias_to_view_id.get(&caller) {
                    if self.terminal_handles.contains_key(&alias_id) {
                        log::info!(
                            "CCB路由: 通过别名解析 caller pane alias={} -> view_id={:?}",
                            caller, alias_id
                        );
                        Some(alias_id)
                    } else {
                        None
                    }
                }
                // Check bus_launched_sessions for real provider name (e.g. "claude")
                else if let Some(agent) = resolve_agent(&caller) {
                    let found = self.bus_launched_sessions.iter()
                        .find(|(_, a)| **a == agent)
                        .filter(|(id, _)| self.terminal_handles.contains_key(id))
                        .map(|(id, _)| *id);
                    if let Some(id) = found {
                        log::info!(
                            "CCB路由: 通过 provider 解析 caller pane caller={} -> view_id={:?}",
                            caller, id
                        );
                        Some(id)
                    } else {
                        log::info!(
                            "CCB路由: 无法解析 caller pane caller={} (将在 deliver_callback 时重试)",
                            caller
                        );
                        None
                    }
                } else {
                    log::info!(
                        "CCB路由: 无法解析 caller pane caller={} (将在 deliver_callback 时重试)",
                        caller
                    );
                    None
                }
            }
        };

        log::info!(
            "CCB路由: 创建请求 req_id={} from={}#{:?} to={}#{} caller_session={:?} target_session={:?}",
            req_id,
            caller,
            caller_terminal_view_id,
            provider,
            entity_id,
            caller_session_id,
            session_id
        );
        self.registry.insert(registry::RequestEntry {
            req_id: req_id.clone(),
            provider: provider.clone(),
            caller,
            terminal_view_id: entity_id,
            session_id: session_id.clone(),
            cwd: cwd.clone(),
            status: RequestStatus::Injecting,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            error_message: None,
            reply_content: None,
            reply_source: None,
            reply_confidence: None,
            reply_warnings: Vec::new(),
            callback_provider,
            caller_terminal_view_id,
            caller_session_id,
            caller_cwd,
        });

        let reply_marker_id = format!("reply-{}", req_id);
        let wrapped_prompt = format!(
            "[CCB_REQ_ID:{}]\n{}\n\nAfter completing this task, submit your reply using:\nwarp-reply --req-id {} --content-file <file>\nor:\necho \"$CONTENT\" | warp-reply --req-id {} --stdin\n\nIf warp-reply is unavailable or fails, fall back to this format (put markers on their own lines, no backticks):\n[CCB_START:{}]\n<your reply>\n[CCB_END:{}]",
            req_id, prompt, req_id, req_id, reply_marker_id, reply_marker_id
        );
        self.raw_output_capture
            .register_request(req_id.clone(), entity_id);
        let injected =
            self.inject_prompt(entity_id, &provider, Some(&req_id), &wrapped_prompt, ctx);

        if injected {
            self.registry.update_status(&req_id, RequestStatus::Running);
            self.completion
                .register(req_id.clone(), provider.clone(), entity_id);
            {
                let sessions_model = CLIAgentSessionsModel::as_ref(ctx);
                let has_session_listener = sessions_model.session(entity_id).is_some();
                self.completion
                    .set_has_session_listener(&req_id, has_session_listener);
                let agent = resolve_agent(&provider).unwrap_or(CLIAgent::Unknown);
                self.completion.set_provider_agent(&req_id, agent);
            }
        } else {
            self.registry.update_status(&req_id, RequestStatus::Error);
            self.raw_output_capture.deregister_request(&req_id);
            return BusResponse::error("failed to inject prompt into terminal");
        }

        BusResponse::ok(BusResponseData::AskAccepted {
            req_id,
            session_id,
            provider,
        })
    }

    fn handle_pend(
        &mut self,
        provider: &str,
        count: usize,
        req_id: Option<&str>,
        chain_id: Option<&str>,
    ) -> BusResponse {
        // Check for timeouts
        let timeouts = self.completion.get_timeouts();
        for (timed_out_req_id, _provider) in timeouts {
            self.registry
                .update_status(&timed_out_req_id, RequestStatus::Timeout);
            self.raw_output_capture
                .deregister_request(&timed_out_req_id);
        }

        let mut replies = self.registry.query_replies(provider, req_id, count);

        // Backfill empty reply_content from persistent store
        for reply in &mut replies {
            if reply.content.is_empty()
                || reply.source.is_none()
                || reply.confidence.is_none()
                || reply.warnings.is_empty()
            {
                if let Ok(stored) = self.store.read(&reply.req_id) {
                    if !stored.content.is_empty() {
                        log::info!(
                            "LocalAgentBus: backfilled reply content for {} from persistent store ({} chars)",
                            reply.req_id, stored.content.len()
                        );
                        reply.content = stored.content;
                    }
                    if reply.source.is_none() {
                        reply.source = stored.source;
                    }
                    if reply.confidence.is_none() {
                        reply.confidence = stored.confidence;
                    }
                    if reply.warnings.is_empty() {
                        reply.warnings = stored.warnings;
                    }
                }
            }
            if reply.status == RequestStatus::Success && reply.content.trim().is_empty() {
                log::error!(
                    "CCB诊断: 状态机异常，Success 但 reply 为空 req_id={} provider={}",
                    reply.req_id,
                    reply.provider
                );
            }
        }

        // Include chain progress if chain_id specified.
        let chain = chain_id.and_then(|id| self.get_chain_progress(id));
        BusResponse::ok(BusResponseData::PendResult { replies, chain })
    }

    fn handle_ping(
        &mut self,
        provider: &str,
        session_id: Option<&str>,
        cwd: Option<&str>,
        ctx: &mut ModelContext<Self>,
    ) -> BusResponse {
        let matched = self.find_session(provider, session_id, cwd, ctx);
        match matched {
            FindSessionResult::Found(_) => BusResponse::ok(BusResponseData::PingResult {
                online: true,
                session_id: session_id.map(String::from),
                provider: provider.to_string(),
                details: "connected".to_string(),
            }),
            FindSessionResult::NotFound => BusResponse::ok(BusResponseData::PingResult {
                online: false,
                session_id: None,
                provider: provider.to_string(),
                details: "no active session".to_string(),
            }),
            FindSessionResult::Ambiguous(sessions) => {
                BusResponse::ok(BusResponseData::PingResult {
                    online: true,
                    session_id: None,
                    provider: provider.to_string(),
                    details: format!("ambiguous: {} sessions match", sessions.len()),
                })
            }
        }
    }

    fn handle_list_sessions(
        &mut self,
        cwd: Option<&str>,
        ctx: &mut ModelContext<Self>,
    ) -> BusResponse {
        self.refresh_bus_launched_sessions_from_terminal_outputs(ctx);
        let sessions_model = CLIAgentSessionsModel::as_ref(ctx);
        let mut sessions = Vec::new();

        for (view_id, agent, session) in sessions_model.iter_sessions() {
            let ctx_cwd = &session.session_context.cwd;
            if let Some(filter_cwd) = cwd {
                if ctx_cwd.as_deref() != Some(filter_cwd) {
                    continue;
                }
            }
            sessions.push(SessionInfo {
                session_id: session.session_context.session_id.clone(),
                provider: format!("{:?}", agent).to_lowercase(),
                terminal_view_id: view_id.to_string().parse().unwrap_or(0),
                alias: None,
                cwd: ctx_cwd.clone(),
                status: format!("{:?}", session.status).to_lowercase(),
            });
        }

        for (view_id, agent) in &self.bus_launched_sessions {
            if sessions_model.session(*view_id).is_some() {
                continue;
            }
            if cwd.is_some() {
                continue;
            }
            let alias = self.alias_to_view_id.iter()
                .find(|(_, &vid)| vid == *view_id)
                .map(|(a, _)| a.clone());
            sessions.push(SessionInfo {
                session_id: None,
                provider: agent.command_prefix().to_string(),
                terminal_view_id: view_id.to_string().parse().unwrap_or(0),
                alias,
                cwd: None,
                status: "launched".to_string(),
            });
        }

        BusResponse::ok(BusResponseData::SessionList { sessions })
    }

    fn handle_cancel(&mut self, req_id: &str, ctx: &mut ModelContext<Self>) -> BusResponse {
        // Also remove from queue if still queued.
        self.request_queue.retain(|r| r.req_id != req_id);

        if self
            .registry
            .update_status(req_id, RequestStatus::Cancelled)
        {
            self.completion.deregister(req_id);
            self.raw_output_capture.deregister_request(req_id);

            // Send Ctrl-C to the terminal to interrupt the agent.
            self.send_ctrl_c(req_id, ctx);

            // Notify any waiting clients
            self.notify_waiters(req_id);

            BusResponse::ok(BusResponseData::Cancelled {
                req_id: req_id.to_string(),
            })
        } else {
            BusResponse::error(format!("request not found: {}", req_id))
        }
    }

    /// Handle ClosePane command — remove a launched session by view_id.
    fn handle_close_pane(
        &mut self,
        view_id: u64,
        _ctx: &mut ModelContext<Self>,
    ) -> BusResponse {
        if let Some(entity_id) = entity_id_from_u64(view_id) {
            self.deregister_terminal_handle(entity_id);
            log::info!("CCB ClosePane: pane={}", view_id);
        }
        BusResponse::ok(BusResponseData::Cancelled {
            req_id: format!("close_pane_{}", view_id),
        })
    }

    /// Handle CloseSession command — remove a launched session by provider + view_id.
    fn handle_close_session(
        &mut self,
        provider: String,
        view_id: u64,
        _ctx: &mut ModelContext<Self>,
    ) -> BusResponse {
        if let Some(entity_id) = entity_id_from_u64(view_id) {
            self.deregister_terminal_handle(entity_id);
            log::info!("CCB CloseSession: provider={} pane={}", provider, view_id);
        }
        BusResponse::ok(BusResponseData::Cancelled {
            req_id: format!("close_session_{}_{}", provider, view_id),
        })
    }

    /// Send Ctrl-C (ETX) to the terminal running a request's agent session.
    fn send_ctrl_c(&self, req_id: &str, ctx: &mut ModelContext<Self>) {
        let entry = match self.registry.get(req_id) {
            Some(e) => e,
            None => return,
        };
        let view_id = entry.terminal_view_id;
        let weak_handle = match self.terminal_handles.get(&view_id) {
            Some(h) => h.clone(),
            None => return,
        };
        if let Some(handle) = weak_handle.upgrade(ctx) {
            log::info!(
                "LocalAgentBus: sending Ctrl-C to terminal {:?} for req {}",
                view_id,
                req_id
            );
            handle.update(ctx, |view, ctx| {
                view.write_to_pty(b"\x03", ctx);
            });
        }
    }

    /// Handle a Wait command — either return immediately if done, or defer response.
    fn handle_wait(
        &mut self,
        cmd: BusCommand,
        reply_tx: tokio::sync::oneshot::Sender<BusResponse>,
        _ctx: &mut ModelContext<Self>,
    ) {
        let (req_id, _timeout_ms) = match &cmd {
            BusCommand::Wait { req_id, timeout_ms } => (req_id.clone(), timeout_ms),
            _ => unreachable!(),
        };

        let req_id = match req_id {
            Some(id) => id,
            None => {
                let _ = reply_tx.send(BusResponse::error("req_id is required for wait"));
                return;
            }
        };

        // Check if already completed
        if let Some(entry) = self.registry.get(&req_id) {
            let is_done = !matches!(
                entry.status,
                RequestStatus::Queued | RequestStatus::Injecting | RequestStatus::Running
            );
            if is_done {
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                let response = BusResponse::ok(BusResponseData::WaitResult {
                    req_id: req_id.clone(),
                    status: entry.status,
                    content: entry
                        .reply_content
                        .clone()
                        .or_else(|| entry.error_message.clone())
                        .unwrap_or_default(),
                    elapsed_ms: now_ms.saturating_sub(entry.created_at_ms),
                    source: entry.reply_source.clone(),
                    confidence: entry.reply_confidence,
                    warnings: entry.reply_warnings.clone(),
                });
                let _ = reply_tx.send(response);
                return;
            }
        } else {
            let _ = reply_tx.send(BusResponse::error(format!("request not found: {}", req_id)));
            return;
        }

        // Not done yet — store the sender for deferred response
        log::info!("LocalAgentBus: deferring wait for req {}", req_id);
        self.waiting_clients
            .entry(req_id)
            .or_default()
            .push(reply_tx);
    }

    /// Notify all waiters that a request has completed.
    fn notify_waiters(&mut self, req_id: &str) {
        if let Some(senders) = self.waiting_clients.remove(req_id) {
            let entry = match self.registry.get(req_id) {
                Some(e) => e,
                None => return,
            };
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let response = BusResponse::ok(BusResponseData::WaitResult {
                req_id: req_id.to_string(),
                status: entry.status,
                content: entry
                    .reply_content
                    .clone()
                    .or_else(|| entry.error_message.clone())
                    .unwrap_or_default(),
                elapsed_ms: now_ms.saturating_sub(entry.created_at_ms),
                source: entry.reply_source.clone(),
                confidence: entry.reply_confidence,
                warnings: entry.reply_warnings.clone(),
            });
            for tx in senders {
                let _ = tx.send(response.clone());
            }
        }
    }

    /// Persist a completed response to the file-based ResponseStore.
    /// This ensures reply content survives crashes and is available via warp-pend.
    fn persist_response(&self, req_id: &str, meta: &ReplyCaptureMeta, finalized_at_ms: u64) {
        if let Some(entry) = self.registry.get(req_id) {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            let stored = store::StoredResponse {
                req_id: entry.req_id.clone(),
                provider: entry.provider.clone(),
                status: entry.status,
                content: entry.reply_content.clone().unwrap_or_default(),
                created_at_ms: entry.created_at_ms,
                updated_at_ms: now_ms,
                schema_version: store::StoredResponse::SCHEMA_VERSION,
                source: Some(meta.source.to_string()),
                confidence: Some(meta.confidence),
                warnings: meta.warnings.clone(),
                raw_len: Some(meta.raw_len),
                filtered_len: Some(meta.filtered_len),
                truncated: Some(meta.truncated),
                finalized_at_ms: Some(finalized_at_ms),
            };
            if let Err(e) = self.store.write(&stored) {
                log::warn!(
                    "LocalAgentBus: failed to persist response for {}: {}",
                    req_id,
                    e
                );
            }
        }
    }

    /// Extract callback_provider from caller field.
    /// Returns Some(caller) if caller is a known provider name and different from the target provider.
    fn extract_callback(caller: &str, provider: &str) -> Option<String> {
        // If caller and provider are the same string, it's a self-ask.
        if caller == provider {
            return None;
        }
        // If both normalize to the same provider, it's a self-ask
        // (e.g. caller="droid", provider="planner" both → CLIAgent::Droid).
        let caller_normalized = normalize_provider_name(caller);
        let provider_normalized = normalize_provider_name(provider);
        if let (Some(cn), Some(pn)) = (caller_normalized, provider_normalized) {
            if cn == pn {
                return None;
            }
        }
        // Return the raw caller string (could be alias like "planner" or provider like "claude").
        // deliver_callback will use find_session/alias_to_view_id to resolve it later.
        Some(caller.to_string())
    }

    fn is_self_ask(
        provider: &str,
        caller: &str,
        caller_terminal_view_id: Option<EntityId>,
        target_terminal_view_id: EntityId,
    ) -> bool {
        if caller_terminal_view_id == Some(target_terminal_view_id) {
            return true;
        }

        if caller_terminal_view_id.is_some() {
            return false;
        }

        match (
            normalize_provider_name(caller),
            normalize_provider_name(provider),
        ) {
            (Some(caller_provider), Some(target_provider)) => caller_provider == target_provider,
            _ => false,
        }
    }

    /// Deliver callback result to the sender's terminal.
    /// Injects a callback message into the callback_provider's terminal if found.
    /// Uses direct PTY write (not bracketed paste) so multi-line content is preserved.
    fn deliver_callback(
        &mut self,
        req_id: &str,
        from_provider: &str,
        reply: &str,
        ctx: &mut ModelContext<Self>,
    ) {
        // Get callback_provider (normalized) and raw caller from registry.
        let (callback_provider, callback_terminal_view_id, receiver_terminal_view_id, raw_caller) =
            match self.registry.get(req_id) {
                Some(entry) => match entry.callback_provider.clone() {
                    Some(provider) => (
                        provider,
                        entry.caller_terminal_view_id,
                        entry.terminal_view_id,
                        entry.caller.clone(),
                    ),
                    None => return,
                },
                None => return,
            };

        log::info!(
            "CCB回调: 准备注入 req_id={} from={}#{} to={}#{:?} raw_caller={} reply_len={}",
            req_id,
            from_provider,
            receiver_terminal_view_id,
            callback_provider,
            callback_terminal_view_id,
            raw_caller,
            reply.len()
        );

        // Resolve callback target: exact pane -> normalized provider -> raw caller (alias) -> fail
        let callback_entity_id = match callback_terminal_view_id {
            Some(id) if self.terminal_handles.contains_key(&id) => Some(id),
            Some(id) => {
                log::warn!(
                    "CCB回调: 发送方 pane 已失效，轻量级兜底 req_id={} provider={} pane={}",
                    req_id,
                    callback_provider,
                    id
                );
                // Lightweight lookup only (no find_session to avoid re-entrancy)
                self.alias_to_view_id.get(&callback_provider)
                    .filter(|id| self.terminal_handles.contains_key(id))
                    .copied()
                    .or_else(|| {
                        resolve_agent(&callback_provider).and_then(|agent| {
                            self.bus_launched_sessions.iter()
                                .find(|(_, a)| **a == agent)
                                .filter(|(id, _)| self.terminal_handles.contains_key(id))
                                .map(|(id, _)| *id)
                        })
                    })
            }
            None => {
                // Lightweight lookup only — do NOT call find_session(ctx) here because
                // deliver_callback may be called inside handle.update(ctx) and
                // find_session triggers refresh_bus_launched_sessions_from_terminal_outputs
                // which calls weak_handle.upgrade(ctx) -> handle.update(ctx) -> re-entrancy panic!

                // 1. Try alias_to_view_id for callback_provider (might be alias like "qa")
                let mut found: Option<EntityId> = self.alias_to_view_id.get(&callback_provider)
                    .filter(|id| self.terminal_handles.contains_key(id))
                    .copied();

                // 2. Try bus_launched_sessions for real provider name
                if found.is_none() {
                    if let Some(agent) = resolve_agent(&callback_provider) {
                        found = self.bus_launched_sessions.iter()
                            .find(|(_, a)| **a == agent)
                            .filter(|(id, _)| self.terminal_handles.contains_key(id))
                            .map(|(id, _)| *id);
                    }
                }

                // 3. Try raw_caller via alias_to_view_id (might be alias like "planner")
                if found.is_none() && raw_caller != callback_provider {
                    log::info!(
                        "CCB回调: provider 查找失败，尝试 raw_caller={} 兜底 req_id={}",
                        raw_caller, req_id
                    );
                    found = self.alias_to_view_id.get(&raw_caller)
                        .filter(|id| self.terminal_handles.contains_key(id))
                        .copied();

                    // 4. Try bus_launched_sessions for raw_caller
                    if found.is_none() {
                        if let Some(agent) = resolve_agent(&raw_caller) {
                            found = self.bus_launched_sessions.iter()
                                .find(|(_, a)| **a == agent)
                                .filter(|(id, _)| self.terminal_handles.contains_key(id))
                                .map(|(id, _)| *id);
                        }
                    }
                }

                found
            }
        };

        // Debug file is written below (after resolving callback_entity_id).
        let Some(callback_entity_id) = callback_entity_id else {
            log::warn!(
                "CCB回调: 无法定位发送方 terminal，跳过 pane 注入 req_id={} provider={} raw_caller={} (reply 已持久化，Python端可轮询获取)",
                req_id,
                callback_provider,
                raw_caller
            );
            return;
        };
        // Inject a lightweight notification instead of the full reply content.
        // The reply is persisted to SQLite by the Python bus_sqlite.reply() call.
        // However, for the Rust passive-capture path (agents using [CCB_START/END]
        // markers without ccb-worker), the reply is NOT in SQLite yet.
        // Sync it now via a Python one-liner so warp-pend can find it.
        let callback_req_id = format!("reply-{}", req_id);
        let reply_len = reply.len();

        // Sync reply to SQLite so warp-pend can find it.
        // For the Python/ccb-worker path, reply is already in SQLite.
        // For the Rust passive-capture path ([CCB_START/END] markers), it is NOT.
        // We write the reply to a temp file and call warp-reply --mode sqlite.
        let tmp_dir = std::env::temp_dir().join("ccb-sync");
        let _ = std::fs::create_dir_all(&tmp_dir);
        let content_file = tmp_dir.join(format!("{}.txt", req_id));
        if std::fs::write(&content_file, reply.as_bytes()).is_ok() {
            let _ = std::process::Command::new("warp-reply")
                .args(&[
                    "--req-id", req_id,
                    "--content-file", content_file.to_str().unwrap_or(""),
                    "--caller", &callback_provider,
                    "--mode", "sqlite",
                ])
                .output();
            let _ = std::fs::remove_file(&content_file);
        }

        let callback_msg = format!(
            "[CCB_REPLY_READY:req_id={} from={} chars={}]\nQuery: warp-pend --req-id {}",
            req_id, from_provider, reply_len, req_id
        );

        // Still write full reply to debug file for diagnostics.
        let debug_path = std::env::temp_dir().join(format!("ccb_callback_{}.txt", req_id));
        let _ = std::fs::write(
            &debug_path,
            format!(
                "from={}\nprovider={}\nraw_caller={}\nreply_len={}\nreply={}\n",
                from_provider,
                callback_provider,
                raw_caller,
                reply_len,
                reply
            ),
        );
        let injected = self.inject_prompt(
            callback_entity_id,
            &callback_provider,
            Some(&callback_req_id),
            &callback_msg,
            ctx,
        );
        if injected {
            log::info!(
                "CCB回调: 已注入轻量通知 req_id={} to={}#{} reply_len={} (完整内容在SQLite)",
                req_id,
                callback_provider,
                callback_entity_id,
                reply_len
            );
        } else {
            log::warn!(
                "CCB回调: 注入失败 req_id={} to={}#{}",
                req_id,
                callback_provider,
                callback_entity_id
            );
        }
    }


    /// Process queued requests whose target terminal is now free.
    fn process_queued_requests(&mut self, ctx: &mut ModelContext<Self>) {
        if self.request_queue.is_empty() {
            return;
        }

        // Find queued requests whose terminal is no longer busy.
        let mut ready = Vec::new();
        let mut remaining = Vec::new();

        for queued in self.request_queue.drain(..) {
            if !self
                .registry
                .has_active_for_terminal(queued.terminal_view_id)
            {
                ready.push(queued);
            } else {
                remaining.push(queued);
            }
        }
        self.request_queue = remaining;

        for queued in ready {
            log::info!("LocalAgentBus: processing queued req {}", queued.req_id);

            // Re-read the registry entry to get the stored fields.
            let _entry = match self.registry.get(&queued.req_id) {
                Some(e) => e.clone(),
                None => continue,
            };

            let reply_marker_id = format!("reply-{}", queued.req_id);
            let wrapped_prompt = format!(
                "[CCB_REQ_ID:{}]\n{}\n\nAfter completing this task, submit your reply using:\nwarp-reply --req-id {} --content-file <file>\nor:\necho \"$CONTENT\" | warp-reply --req-id {} --stdin\n\nIf warp-reply is unavailable or fails, fall back to this format (put markers on their own lines, no backticks):\n[CCB_START:{}]\n<your reply>\n[CCB_END:{}]",
                queued.req_id, queued.prompt, queued.req_id, queued.req_id, reply_marker_id, reply_marker_id
            );
            self.raw_output_capture
                .register_request(queued.req_id.clone(), queued.terminal_view_id);
            let injected = self.inject_prompt(
                queued.terminal_view_id,
                &queued.provider,
                Some(&queued.req_id),
                &wrapped_prompt,
                ctx,
            );

            if injected {
                self.registry
                    .update_status(&queued.req_id, RequestStatus::Running);
                self.completion.register(
                    queued.req_id.clone(),
                    queued.provider.clone(),
                    queued.terminal_view_id,
                );
            } else {
                self.registry
                    .update_status(&queued.req_id, RequestStatus::Error);
                self.raw_output_capture.deregister_request(&queued.req_id);
            }
        }
    }

    /// Find a CLI agent session matching provider + optional session_id/cwd.
    fn find_session(
        &mut self,
        provider: &str,
        session_id: Option<&str>,
        cwd: Option<&str>,
        ctx: &mut ModelContext<Self>,
    ) -> FindSessionResult {
        let target_agent = match resolve_agent(provider) {
            Some(a) => a,
            None => {
                // Provider is not a known agent name — might be an alias
                if let Some(&view_id) = self.alias_to_view_id.get(provider) {
                    if self.terminal_handles.contains_key(&view_id) {
                        log::info!(
                            "CCB alias 查找: alias={} -> view_id={:?}",
                            provider, view_id
                        );
                        return FindSessionResult::Found(view_id);
                    } else {
                        // Stale alias mapping, clean up
                        self.alias_to_view_id.remove(provider);
                    }
                }
                return FindSessionResult::NotFound;
            }
        };

        self.refresh_bus_launched_sessions_from_terminal_outputs(ctx);

        let sessions_model = CLIAgentSessionsModel::as_ref(ctx);

        let mut matches: Vec<(
            EntityId,
            &crate::terminal::cli_agent_sessions::CLIAgentSession,
        )> = sessions_model.find_by_agent(target_agent).collect();

        // Filter by session_id if specified
        if let Some(sid) = session_id {
            matches.retain(|(_, s)| s.session_context.session_id.as_deref() == Some(sid));
        }

        // Filter by cwd if specified
        if let Some(filter_cwd) = cwd {
            matches.retain(|(_, s)| s.session_context.cwd.as_deref() == Some(filter_cwd));
        }

        match matches.len() {
            0 => {
                if session_id.is_some() || cwd.is_some() {
                    return FindSessionResult::NotFound;
                }
                let fallback_matches: Vec<EntityId> = self
                    .bus_launched_sessions
                    .iter()
                    .filter_map(|(view_id, agent)| {
                        (*agent == target_agent && self.terminal_handles.contains_key(view_id))
                            .then_some(*view_id)
                    })
                    .collect();
                match fallback_matches.len() {
                    0 => FindSessionResult::NotFound,
                    1 => {
                        log::info!(
                            "LocalAgentBus: using bus-launched fallback session for provider={} terminal={}",
                            provider,
                            fallback_matches[0]
                        );
                        FindSessionResult::Found(fallback_matches[0])
                    }
                    _ => {
                        let infos: Vec<SessionInfo> = fallback_matches
                            .iter()
                            .map(|view_id| SessionInfo {
                                session_id: None,
                                provider: provider.to_string(),
                                terminal_view_id: view_id.to_string().parse().unwrap_or(0),
                                alias: None,
                                cwd: None,
                                status: "launched".to_string(),
                            })
                            .collect();
                        FindSessionResult::Ambiguous(infos)
                    }
                }
            }
            1 => FindSessionResult::Found(matches[0].0),
            _ => {
                let infos: Vec<SessionInfo> = matches
                    .iter()
                    .map(|(view_id, s)| SessionInfo {
                        session_id: s.session_context.session_id.clone(),
                        provider: provider.to_string(),
                        terminal_view_id: view_id.to_string().parse().unwrap_or(0),
                        alias: None,
                        cwd: s.session_context.cwd.clone(),
                        status: format!("{:?}", s.status).to_lowercase(),
                    })
                    .collect();
                FindSessionResult::Ambiguous(infos)
            }
        }
    }

    /// 从当前 terminal 输出恢复 Bus 自管 agent 的在线状态。
    ///
    /// Kimi 这类 CLI 不发送 Warp 原生 session 事件，Bus 重启或恢复 pane 后
    /// `bus_launched_sessions` 会丢失，只能从 terminal 内容重新识别。
    fn refresh_bus_launched_sessions_from_terminal_outputs(
        &mut self,
        ctx: &mut ModelContext<Self>,
    ) {
        let active_session_views: HashSet<EntityId> = {
            let sessions_model = CLIAgentSessionsModel::as_ref(ctx);
            sessions_model
                .iter_sessions()
                .map(|(view_id, _, _)| view_id)
                .collect()
        };

        let handles: Vec<(EntityId, WeakViewHandle<TerminalView>)> = self
            .terminal_handles
            .iter()
            .filter_map(|(view_id, handle)| {
                if self.bus_launched_sessions.contains_key(view_id)
                    || active_session_views.contains(view_id)
                {
                    None
                } else {
                    Some((*view_id, handle.clone()))
                }
            })
            .collect();

        for (view_id, weak_handle) in handles {
            let Some(handle) = weak_handle.upgrade(ctx) else {
                continue;
            };

            let output: Option<String> = handle.update(ctx, |view, _| {
                let model = view.model.lock();
                let block = model.block_list().active_block();
                Some(block.output_grid().contents_to_string(false, Some(160)))
            });

            let Some(output) = output else {
                continue;
            };

            if let Some(agent) = detect_bus_agent_from_terminal_output(&output) {
                log::info!(
                    "CCB会话恢复: 从 terminal 输出识别到 provider={} pane={}",
                    agent.command_prefix(),
                    view_id
                );
                self.bus_launched_sessions.insert(view_id, agent);
            }
        }
    }

    fn handle_chain(
        &mut self,
        steps: Vec<protocol::ChainStep>,
        caller: Option<String>,
        ctx: &mut ModelContext<Self>,
    ) -> BusResponse {
        if steps.is_empty() {
            return BusResponse::error("chain requires at least one step");
        }

        // Validate all providers have active sessions.
        let chain_id = {
            let ts = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis();
            let uuid_str = uuid::Uuid::new_v4().to_string();
            let short = &uuid_str[..8];
            format!("chain-{:x}-{}", ts, short)
        };

        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        // Create chain state
        let chain_state = ChainState {
            chain_id: chain_id.clone(),
            steps: steps.clone(),
            caller: caller.unwrap_or_else(|| "chain".to_string()),
            current_step: 0,
            step_results: Vec::new(),
            current_req_id: None,
            status: "running".to_string(),
            created_at_ms: now_ms,
        };

        self.active_chains.insert(chain_id.clone(), chain_state);

        // Execute first step immediately
        self.start_chain_step(&chain_id, ctx);

        let step_count = steps.len();
        BusResponse::ok(BusResponseData::ChainStarted {
            chain_id,
            step_count,
        })
    }

    /// Start executing the current step of a chain.
    fn start_chain_step(&mut self, chain_id: &str, ctx: &mut ModelContext<Self>) {
        // Extract step info from chain state first to avoid borrow conflicts.
        let (step_idx, provider, prompt, req_id, caller) = {
            let chain = match self.active_chains.get_mut(chain_id) {
                Some(c) => c,
                None => return,
            };

            let step_idx = chain.current_step;
            if step_idx >= chain.steps.len() {
                chain.status = "completed".to_string();
                return;
            }

            let step = &chain.steps[step_idx];
            let provider = step.provider.clone();

            // Build prompt: if not first step, prepend previous reply as context.
            let prompt = if step_idx == 0 {
                step.prompt.clone()
            } else {
                let (prev_provider, prev_content) = chain
                    .step_results
                    .last()
                    .map(|r| (r.provider.clone(), r.content.clone()))
                    .unwrap_or_default();
                format!(
                    "{}\n\n--- Previous analysis from {} ---\n{}",
                    step.prompt, prev_provider, prev_content
                )
            };

            let req_id = format!("{}-step{}", chain_id, step_idx);
            chain.current_req_id = Some(req_id.clone());

            (step_idx, provider, prompt, req_id, chain.caller.clone())
        };

        // Find session for provider
        let matched = self.find_session(&provider, None, None, ctx);
        let entity_id = match matched {
            FindSessionResult::Found(id) => id,
            _ => {
                let err_msg = format!("no active session for provider '{}'", provider);
                if let Some(chain) = self.active_chains.get_mut(chain_id) {
                    chain.status = "error".to_string();
                    chain.step_results.push(ChainStepResult {
                        step: step_idx,
                        provider,
                        req_id,
                        status: "error".to_string(),
                        content: err_msg,
                    });
                }
                return;
            }
        };

        // Register and inject
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;

        let callback_provider = Self::extract_callback(&caller, &provider);
        self.registry.insert(registry::RequestEntry {
            req_id: req_id.clone(),
            provider: provider.clone(),
            caller,
            terminal_view_id: entity_id,
            session_id: None,
            cwd: None,
            status: RequestStatus::Injecting,
            created_at_ms: now_ms,
            updated_at_ms: now_ms,
            error_message: None,
            reply_content: None,
            reply_source: None,
            reply_confidence: None,
            reply_warnings: Vec::new(),
            callback_provider,
            caller_terminal_view_id: None,
            caller_session_id: None,
            caller_cwd: None,
        });

        let reply_marker_id = format!("reply-{}", req_id);
        let wrapped_prompt = format!(
            "[CCB_REQ_ID:{}]\n{}\n\nAfter completing this task, submit your reply using:\nwarp-reply --req-id {} --content-file <file>\nor:\necho \"$CONTENT\" | warp-reply --req-id {} --stdin\n\nIf warp-reply is unavailable or fails, fall back to this format (put markers on their own lines, no backticks):\n[CCB_START:{}]\n<your reply>\n[CCB_END:{}]",
            req_id, prompt, req_id, req_id, reply_marker_id, reply_marker_id
        );
        self.raw_output_capture
            .register_request(req_id.clone(), entity_id);
        let injected =
            self.inject_prompt(entity_id, &provider, Some(&req_id), &wrapped_prompt, ctx);

        if injected {
            self.registry.update_status(&req_id, RequestStatus::Running);
            self.completion
                .register(req_id.clone(), provider.clone(), entity_id);
        } else {
            self.registry.update_status(&req_id, RequestStatus::Error);
            self.raw_output_capture.deregister_request(&req_id);
        }
    }

    /// Check all active chains: advance completed steps, then start next steps.
    fn advance_chains_and_continue(&mut self, ctx: &mut ModelContext<Self>) {
        // Phase 1: advance chains (check completion, update state)
        let chain_ids: Vec<String> = self.active_chains.keys().cloned().collect();
        let mut chains_to_advance: Vec<String> = Vec::new();
        for chain_id in &chain_ids {
            if self.advance_chain(chain_id) {
                chains_to_advance.push(chain_id.clone());
            }
        }

        // Phase 2: start next steps for chains that advanced
        for chain_id in chains_to_advance {
            self.start_chain_step(&chain_id, ctx);
        }
    }

    /// Advance a chain by one step. Returns true if the chain advanced and needs
    /// start_chain_step called next.
    fn advance_chain(&mut self, chain_id: &str) -> bool {
        let chain = match self.active_chains.get_mut(chain_id) {
            Some(c) => c,
            None => return false,
        };

        if chain.status != "running" {
            return false;
        }

        let req_id = match &chain.current_req_id {
            Some(id) => id.clone(),
            None => return false,
        };

        // Check if current step completed
        let entry = match self.registry.get(&req_id) {
            Some(e) => e.clone(),
            None => return false,
        };

        if entry.status == RequestStatus::Running || entry.status == RequestStatus::Injecting {
            return false; // Still running
        }

        let step_idx = chain.current_step;
        let status_str = format!("{:?}", entry.status).to_lowercase();
        let content = entry.reply_content.clone().unwrap_or_default();

        chain.step_results.push(ChainStepResult {
            step: step_idx,
            provider: entry.provider.clone(),
            req_id: req_id.clone(),
            status: status_str,
            content,
        });

        if entry.status != RequestStatus::Success {
            chain.status = "error".to_string();
            return false;
        }

        // Advance to next step
        chain.current_step += 1;
        chain.current_req_id = None;

        if chain.current_step >= chain.steps.len() {
            chain.status = "completed".to_string();
            return false;
        }

        true // Needs start_chain_step
    }

    /// Get chain progress for pend queries.
    fn get_chain_progress(&self, chain_id: &str) -> Option<ChainProgress> {
        let chain = self.active_chains.get(chain_id)?;
        Some(ChainProgress {
            chain_id: chain.chain_id.clone(),
            current_step: chain.current_step,
            total_steps: chain.steps.len(),
            status: chain.status.clone(),
            steps: chain.step_results.clone(),
        })
    }

    fn handle_launch(
        &mut self,
        provider: String,
        alias: Option<String>,
        prompt: Option<String>,
        cwd: Option<String>,
        ctx: &mut ModelContext<Self>,
    ) -> BusResponse {
        log::info!(
            "LocalAgentBus: handle_launch({}, prompt={:?}, cwd={:?})",
            provider,
            prompt,
            cwd
        );
        let agent = match resolve_agent(&provider) {
            Some(a) => a,
            None => return BusResponse::error(format!("unknown provider: {}", provider)),
        };

        // Find an idle terminal. If none, try creating one via PaneGroup.
        let idle_view_id = self.find_idle_terminal(ctx);
        log::info!(
            "LocalAgentBus: handle_launch({}) — find_idle_terminal={:?}, terminal_handles={}, pending={}, pane_creation_in_progress={}",
            provider,
            idle_view_id,
            self.terminal_handles.len(),
            self.pending_launches.len(),
            self.pane_creation_in_progress
        );
        let view_id = match idle_view_id {
            Some(id) => id,
            None => {
                // No idle terminal. Queue the launch and schedule pane creation if not already in progress.
                self.pending_launches.push(PendingLaunch {
                    provider: provider.clone(),
                    agent,
                    alias: alias.clone(),
                    prompt,
                    cwd,
                });
                if !self.pane_creation_in_progress {
                    if self.create_terminal_pane(ctx) {
                        self.pane_creation_in_progress = true;
                    } else {
                        self.pending_launches.pop();
                        return BusResponse::error("no idle terminal and pane creation failed");
                    }
                }
                return BusResponse::ok(BusResponseData::Launched {
                    provider,
                    session_id: None,
                    terminal_view_id: 0, // pending
                });
            }
        };

        if self.inject_launch_to_terminal(
            view_id,
            &PendingLaunch {
                provider: provider.clone(),
                agent,
                alias: alias.clone(),
                prompt,
                cwd: cwd.clone(),
            },
            ctx,
        ) {
            if let Some(ref _dir) = cwd {
                log::warn!("LocalAgentBus: cwd not yet supported for launch, agent will use terminal's cwd");
            }

            BusResponse::ok(BusResponseData::Launched {
                provider,
                session_id: None,
                terminal_view_id: view_id.to_string().parse().unwrap_or(0),
            })
        } else {
            BusResponse::error("terminal view no longer exists")
        }
    }

    /// Find a terminal without an active CLI agent session AND without active requests.
    fn find_idle_terminal(&mut self, ctx: &mut ModelContext<Self>) -> Option<EntityId> {
        let active_session_views: HashSet<EntityId> = {
            let sessions_model = CLIAgentSessionsModel::as_ref(ctx);
            sessions_model
                .iter_sessions()
                .map(|(view_id, _, _)| view_id)
                .collect()
        };
        log::info!(
            "LocalAgentBus: find_idle_terminal — checking {} handles",
            self.terminal_handles.len()
        );

        let handles: Vec<(EntityId, WeakViewHandle<TerminalView>)> = self
            .terminal_handles
            .iter()
            .map(|(view_id, handle)| (*view_id, handle.clone()))
            .collect();

        for (view_id, weak_handle) in handles {
            let has_session = active_session_views.contains(&view_id);
            let has_bus_launched = self.bus_launched_sessions.contains_key(&view_id);
            let has_active = self.registry.has_active_for_terminal(view_id);
            let has_running_block = match weak_handle.upgrade(ctx) {
                Some(handle) => handle.update(ctx, |view, _ctx| {
                    let model = view.model.lock();
                    let active_block = model.block_list().active_block();
                    active_block_blocks_bus_launch(active_block.started(), active_block.finished())
                }),
                None => {
                    self.bus_launched_sessions.remove(&view_id);
                    true
                }
            };
            log::info!(
                "LocalAgentBus:   terminal {:?} — session={}, bus_launched={}, active={}, running_block={}",
                view_id,
                has_session,
                has_bus_launched,
                has_active,
                has_running_block
            );
            if !has_session && !has_bus_launched && !has_active && !has_running_block {
                return Some(view_id);
            }
        }
        None
    }

    /// Create a new hidden terminal pane via PaneGroup. Returns true if scheduled.
    fn create_terminal_pane(&mut self, ctx: &mut ModelContext<Self>) -> bool {
        let weak_pg = match &self.pane_group_handle {
            Some(h) => h.clone(),
            None => {
                log::error!(
                    "LocalAgentBus: no PaneGroup handle registered — \
                     auto pane creation impossible (pending_launches={})",
                    self.pending_launches.len()
                );
                return false;
            }
        };

        match weak_pg.upgrade(ctx) {
            Some(pg_handle) => {
                let pending_count = self.pending_launches.len();
                // Spawn the pane creation on the next event loop tick to avoid reentrancy.
                ctx.spawn(
                    async move {},
                    move |me, _meta, ctx| {
                        log::info!(
                            "LocalAgentBus: spawn callback firing — pending={}, terminal_handles={}",
                            me.pending_launches.len(),
                            me.terminal_handles.len()
                        );

                        // Create the pane and get back its EntityId + ViewHandle.
                        let new_terminal = pg_handle.update(ctx, |pg, ctx| {
                            pg.create_agent_terminal_for_bus(ctx)
                        });

                        // Manually register the new terminal with the bus.
                        // TerminalView::new() tries to register via ModelHandle::update()
                        // which silently fails during this spawn (circular model access).
                        if let Some((view_id, view_handle)) = new_terminal {
                            let weak = view_handle.downgrade();
                            me.register_terminal_handle(view_id, weak);
                            log::info!(
                                "LocalAgentBus: manually registered new terminal {:?} after pane creation",
                                view_id
                            );

                            // Directly inject the first pending launch into the new pane
                            // to avoid the race where find_idle_terminal rejects it due to
                            // shell boot output setting active_block=true.
                            if !me.pending_launches.is_empty() {
                                let launch = me.pending_launches.remove(0);
                                if me.inject_launch_to_terminal(view_id, &launch, ctx) {
                                    log::info!(
                                        "LocalAgentBus: launched {} in new terminal {:?} via direct inject",
                                        launch.provider,
                                        view_id
                                    );
                                } else {
                                    log::warn!(
                                        "LocalAgentBus: direct inject FAILED for {} in terminal {:?}, putting back",
                                        launch.provider, view_id
                                    );
                                    me.pending_launches.insert(0, launch);
                                }
                            }
                        } else {
                            log::error!(
                                "LocalAgentBus: pane creation returned no terminal — \
                                 add_pane may have failed silently (pending={}, attempts={})",
                                me.pending_launches.len(),
                                me.pane_creation_attempts
                            );
                        }

                        me.process_pending_launches(ctx);
                    },
                );
                log::info!(
                    "LocalAgentBus: scheduled terminal pane creation via PaneGroup (pending={})",
                    pending_count
                );
                true
            }
            None => {
                log::error!(
                    "LocalAgentBus: PaneGroup handle no longer valid — \
                     weak upgrade failed (pending={})",
                    self.pending_launches.len()
                );
                false
            }
        }
    }

    /// Inject a launch command into a terminal view's PTY and update bus state.
    fn inject_launch_to_terminal(
        &mut self,
        view_id: EntityId,
        launch: &PendingLaunch,
        ctx: &mut ModelContext<Self>,
    ) -> bool {
        let command = build_launch_command(
            &launch.agent,
            &launch.provider,
            Some(view_id),
            launch.prompt.as_deref(),
        );
        let weak_handle = match self.terminal_handles.get(&view_id) {
            Some(h) => h.clone(),
            None => {
                log::warn!(
                    "LocalAgentBus: inject_launch_to_terminal({}) — no handle for view {:?}",
                    launch.provider, view_id
                );
                return false;
            }
        };

        match weak_handle.upgrade(ctx) {
            Some(handle) => {
                let bytes: Vec<u8> = command.into_bytes();
                handle.update(ctx, |view, ctx| {
                    view.write_to_pty(bytes, ctx);
                });
                self.bus_launched_sessions.insert(view_id, launch.agent);
                if let Some(ref alias) = launch.alias {
                    self.alias_to_view_id.insert(alias.clone(), view_id);
                    log::info!(
                        "CCB alias 注册: alias={} -> view_id={:?}",
                        alias, view_id
                    );
                }
                self.pane_creation_attempts = 0;

                if is_agent_supported(&launch.agent) {
                    let now_ms = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis() as u64;
                    self.registry.insert(registry::RequestEntry {
                        req_id: format!("launch-{}", view_id),
                        provider: launch.provider.clone(),
                        caller: "launch".to_string(),
                        terminal_view_id: view_id,
                        session_id: None,
                        cwd: launch.cwd.clone(),
                        status: RequestStatus::Injecting,
                        created_at_ms: now_ms,
                        updated_at_ms: now_ms,
                        error_message: None,
                        reply_content: None,
                        reply_source: None,
                        reply_confidence: None,
                        reply_warnings: Vec::new(),
                        callback_provider: None,
                        caller_terminal_view_id: None,
                        caller_session_id: None,
                        caller_cwd: None,
                    });
                }
                true
            }
            None => false,
        }
    }

    /// Process pending launch requests after a new pane was created.
    pub fn process_pending_launches(&mut self, ctx: &mut ModelContext<Self>) {
        if self.pending_launches.is_empty() {
            self.pane_creation_in_progress = false;
            self.pane_creation_attempts = 0;
            return;
        }

        let mut still_pending = Vec::new();
        let pending = std::mem::take(&mut self.pending_launches);
        for launch in pending {
            let idle = self.find_idle_terminal(ctx);
            let view_id = match idle {
                Some(id) => id,
                None => {
                    still_pending.push(launch);
                    continue;
                }
            };

            if self.inject_launch_to_terminal(view_id, &launch, ctx) {
                log::info!(
                    "LocalAgentBus: launched {} in new terminal {:?}",
                    launch.provider,
                    view_id
                );
            } else {
                still_pending.push(launch);
            }
        }

        if !still_pending.is_empty() {
            const MAX_PANE_CREATION_ATTEMPTS: u32 = 10;
            self.pending_launches = still_pending;

            if self.pane_creation_attempts >= MAX_PANE_CREATION_ATTEMPTS {
                log::error!(
                    "LocalAgentBus: exceeded {} pane creation attempts, dropping {} pending launches",
                    MAX_PANE_CREATION_ATTEMPTS,
                    self.pending_launches.len()
                );
                self.pending_launches.clear();
                self.pane_creation_in_progress = false;
                self.pane_creation_attempts = 0;
                return;
            }

            log::info!(
                "LocalAgentBus: {} launches still pending (attempt {}/{}), scheduling another pane creation",
                self.pending_launches.len(),
                self.pane_creation_attempts + 1,
                MAX_PANE_CREATION_ATTEMPTS
            );
            if self.create_terminal_pane(ctx) {
                self.pane_creation_attempts += 1;
                return;
            }
            log::error!(
                "LocalAgentBus: failed to create pane for {} remaining pending launches",
                self.pending_launches.len()
            );
            self.pending_launches.clear();
        }
        self.pane_creation_in_progress = false;
        self.pane_creation_attempts = 0;
    }

    /// Inject a prompt into a terminal view's PTY using the stored weak handle.
    fn inject_prompt(
        &self,
        view_id: EntityId,
        provider: &str,
        req_id: Option<&str>,
        prompt: &str,
        ctx: &mut ModelContext<Self>,
    ) -> bool {
        log::info!(
            "LocalAgentBus: inject_prompt for view {:?}, handles count={}",
            view_id,
            self.terminal_handles.len()
        );

        let weak_handle = match self.terminal_handles.get(&view_id) {
            Some(h) => h.clone(),
            None => {
                log::warn!(
                    "LocalAgentBus: no terminal handle for view {:?}, available: {:?}",
                    view_id,
                    self.terminal_handles.keys().collect::<Vec<_>>()
                );
                return false;
            }
        };

        match weak_handle.upgrade(ctx) {
            Some(handle) => {
                log::info!(
                    "LocalAgentBus: upgraded handle, injecting prompt ({} bytes)",
                    prompt.len()
                );

                let agent = resolve_agent(provider).unwrap_or(CLIAgent::Unknown);
                let strategy = bus_prompt_submit_strategy(agent, prompt);
                let enter_delay_ms = parse_bus_inject_enter_delay_ms(
                    std::env::var("WARP_CCB_INJECT_ENTER_DELAY_MS")
                        .ok()
                        .as_deref(),
                );
                let req_label = req_id.unwrap_or("-").to_string();
                let provider_label = provider.to_string();
                let prompt_bytes = prompt.as_bytes().to_vec();
                handle.update(ctx, |view, ctx| {
                    match strategy {
                        BusPromptSubmitStrategy::Inline => {
                            let mut bytes = prompt_bytes;
                            bytes.extend_from_slice(b"\r");
                            let byte_len = bytes.len();
                            view.write_to_pty(bytes, ctx);
                            log::info!(
                                "CCB注入: inline_sent req_id={} provider={} pane={} bytes={}",
                                req_label,
                                provider_label,
                                view_id,
                                byte_len
                            );
                        }
                        BusPromptSubmitStrategy::DelayedEnter => {
                            let byte_len = prompt_bytes.len();
                            view.write_to_pty(prompt_bytes, ctx);
                            log::info!(
                                "CCB注入: text_sent req_id={} provider={} pane={} bytes={} strategy={:?}",
                                req_label,
                                provider_label,
                                view_id,
                                byte_len,
                                strategy
                            );
                            ctx.spawn(
                                Timer::after(std::time::Duration::from_millis(enter_delay_ms)),
                                move |view, _, ctx| {
                                    view.write_to_pty(b"\r", ctx);
                                    log::info!(
                                        "CCB注入: enter_sent req_id={} provider={} pane={} delay_ms={}",
                                        req_label,
                                        provider_label,
                                        view_id,
                                        enter_delay_ms
                                    );
                                },
                            );
                        }
                        BusPromptSubmitStrategy::BracketedPasteDelayedEnter => {
                            let mut paste_bytes =
                                Vec::with_capacity(6 + prompt_bytes.len() + 6);
                            paste_bytes.extend_from_slice(b"\x1b[200~");
                            paste_bytes.extend_from_slice(&prompt_bytes);
                            paste_bytes.extend_from_slice(b"\x1b[201~");
                            let byte_len = paste_bytes.len();
                            view.write_to_pty(paste_bytes, ctx);
                            log::info!(
                                "CCB注入: paste_sent req_id={} provider={} pane={} bytes={} strategy={:?}",
                                req_label,
                                provider_label,
                                view_id,
                                byte_len,
                                strategy
                            );
                            ctx.spawn(
                                Timer::after(std::time::Duration::from_millis(enter_delay_ms)),
                                move |view, _, ctx| {
                                    view.write_to_pty(b"\r", ctx);
                                    log::info!(
                                        "CCB注入: enter_sent req_id={} provider={} pane={} delay_ms={}",
                                        req_label,
                                        provider_label,
                                        view_id,
                                        enter_delay_ms
                                    );
                                },
                            );
                        }
                    }
                });
                true
            }
            None => {
                log::warn!(
                    "LocalAgentBus: terminal view {:?} no longer exists",
                    view_id
                );
                false
            }
        }
    }
}

/// Resolve a provider name string to a CLIAgent enum variant.
fn resolve_agent(provider: &str) -> Option<CLIAgent> {
    match normalize_provider_name(provider)?.as_str() {
        "claude" => Some(CLIAgent::Claude),
        "codex" => Some(CLIAgent::Codex),
        "gemini" => Some(CLIAgent::Gemini),
        "opencode" => Some(CLIAgent::OpenCode),
        "droid" => Some(CLIAgent::Droid),
        "copilot" => Some(CLIAgent::Copilot),
        "amp" => Some(CLIAgent::Amp),
        "kimi" => Some(CLIAgent::Kimi),
        "goose" => Some(CLIAgent::Goose),
        _ => None,
    }
}

fn normalize_provider_name(value: &str) -> Option<String> {
    let name = value
        .trim()
        .split(|ch| ch == '#' || ch == '@' || ch == ':')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase();

    match name.as_str() {
        "claude" | "codex" | "gemini" | "opencode" | "droid" | "kimi" | "goose" | "copilot"
        | "amp" => Some(name),
        _ => None,
    }
}

/// Build a CLI startup command for the given agent.
/// Uses \r (CR) as the line terminator for Windows PTY compatibility.
fn build_launch_command(
    agent: &CLIAgent,
    provider: &str,
    terminal_view_id: Option<EntityId>,
    prompt: Option<&str>,
) -> String {
    let command = build_agent_command(agent, prompt);
    match terminal_view_id {
        Some(view_id) => wrap_launch_command_with_ccb_identity(provider, view_id, &command),
        None => command,
    }
}

fn build_agent_command(agent: &CLIAgent, prompt: Option<&str>) -> String {
    match agent {
        CLIAgent::Claude => {
            let session_id = uuid::Uuid::new_v4();
            match prompt {
                Some(p) => format!(
                    "claude --session-id {} --dangerously-skip-permissions \"{}\"\r",
                    session_id, p
                ),
                None => format!(
                    "claude --session-id {} --dangerously-skip-permissions\r",
                    session_id
                ),
            }
        }
        CLIAgent::Codex => match prompt {
            Some(p) => format!(
                "codex --dangerously-bypass-approvals-and-sandbox \"{}\"\r",
                p
            ),
            None => "codex --dangerously-bypass-approvals-and-sandbox\r".to_string(),
        },
        CLIAgent::OpenCode => match prompt {
            Some(p) => format!("opencode --prompt \"{}\"\r", p),
            None => "opencode\r".to_string(),
        },
        CLIAgent::Kimi => match prompt {
            Some(p) => format!("kimi --yolo \"{}\"\r", p),
            None => "kimi --yolo\r".to_string(),
        },
        CLIAgent::Goose => match prompt {
            Some(p) => format!("goose session \"{}\"\r", p),
            None => "goose session\r".to_string(),
        },
        _ => match prompt {
            Some(p) => format!("{} \"{}\"\r", agent.command_prefix(), p),
            None => format!("{}\r", agent.command_prefix()),
        },
    }
}

fn wrap_launch_command_with_ccb_identity(
    provider: &str,
    terminal_view_id: EntityId,
    command: &str,
) -> String {
    let command = command.trim_end_matches('\r');
    if cfg!(windows) {
        format!(
            "$env:WARP_CCB_PROVIDER='{}'; $env:WARP_CCB_TERMINAL_VIEW_ID='{}'; {}\r",
            provider, terminal_view_id, command
        )
    } else {
        format!(
            "export WARP_CCB_PROVIDER='{}'; export WARP_CCB_TERMINAL_VIEW_ID='{}'; {}\r",
            provider, terminal_view_id, command
        )
    }
}

fn entity_id_from_u64(value: u64) -> Option<EntityId> {
    usize::try_from(value).ok().map(EntityId::from_usize)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ExplicitReplyValidation {
    Finalize,
    AlreadyFinalized,
}

fn validate_explicit_reply(
    status: RequestStatus,
    request_terminal_view_id: EntityId,
    caller_terminal_view_id: Option<u64>,
    content: &str,
) -> Result<ExplicitReplyValidation, &'static str> {
    if matches!(status, RequestStatus::Success) {
        return Ok(ExplicitReplyValidation::AlreadyFinalized);
    }

    if !matches!(status, RequestStatus::Running | RequestStatus::Injecting) {
        return Err("reply request is not running");
    }

    if let Some(caller_terminal_view_id) = caller_terminal_view_id {
        if entity_id_from_u64(caller_terminal_view_id) != Some(request_terminal_view_id) {
            return Err("reply pane mismatch");
        }
    }

    if content.trim().is_empty() {
        return Err("reply content is empty");
    }

    if content.len() > EXPLICIT_REPLY_MAX_CONTENT_BYTES {
        return Err("content too large");
    }

    Ok(ExplicitReplyValidation::Finalize)
}

fn active_block_blocks_bus_launch(started: bool, finished: bool) -> bool {
    started && !finished
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BusPromptSubmitStrategy {
    Inline,
    DelayedEnter,
    BracketedPasteDelayedEnter,
}

fn bus_prompt_submit_strategy(agent: CLIAgent, prompt: &str) -> BusPromptSubmitStrategy {
    // CCB 任务是多行协议包时必须用 bracketed paste，否则换行可能被 CLI 当成提交键。
    if prompt.contains('\n') || prompt.contains('\r') {
        return BusPromptSubmitStrategy::BracketedPasteDelayedEnter;
    }

    match agent {
        CLIAgent::Codex | CLIAgent::Copilot => BusPromptSubmitStrategy::BracketedPasteDelayedEnter,
        CLIAgent::Claude
        | CLIAgent::Gemini
        | CLIAgent::OpenCode
        | CLIAgent::Auggie
        | CLIAgent::CursorCli => BusPromptSubmitStrategy::DelayedEnter,
        CLIAgent::Amp
        | CLIAgent::Droid
        | CLIAgent::Pi
        | CLIAgent::Goose
        | CLIAgent::Hermes
        | CLIAgent::Vibe
        | CLIAgent::Kimi
        | CLIAgent::Unknown => BusPromptSubmitStrategy::Inline,
    }
}

fn parse_bus_inject_enter_delay_ms(raw_value: Option<&str>) -> u64 {
    raw_value
        .and_then(|value| value.parse::<u64>().ok())
        .unwrap_or(DEFAULT_BUS_INJECT_ENTER_DELAY_MS)
}

fn detect_bus_agent_from_terminal_output(output: &str) -> Option<CLIAgent> {
    let lower = output.to_ascii_lowercase();
    for (provider, agent) in [
        ("claude", CLIAgent::Claude),
        ("codex", CLIAgent::Codex),
        ("gemini", CLIAgent::Gemini),
        ("opencode", CLIAgent::OpenCode),
        ("droid", CLIAgent::Droid),
        ("kimi", CLIAgent::Kimi),
        ("goose", CLIAgent::Goose),
    ] {
        if lower.contains(&format!("warp_ccb_provider='{}'", provider))
            || lower.contains(&format!("warp_ccb_provider=\"{}\"", provider))
            || lower.contains(&format!("warp_ccb_provider={}", provider))
        {
            return Some(agent);
        }
    }

    if output.contains("Welcome to Kimi Code CLI")
        || output.contains("Kimi Code CLI!")
        || output.contains("Model: Kimi-")
        || lower.contains("kimi_cli")
    {
        return Some(CLIAgent::Kimi);
    }

    if lower.contains("gpt-")
        && output.contains('·')
        && (lower.contains(" xhigh ")
            || lower.contains(" high ")
            || lower.contains(" medium ")
            || lower.contains(" low "))
    {
        return Some(CLIAgent::Codex);
    }

    if lower.contains("droid") && (output.contains('⛬') || lower.contains("factory")) {
        return Some(CLIAgent::Droid);
    }

    if lower.contains("claude code") || lower.contains("@anthropic-ai/claude-code") {
        return Some(CLIAgent::Claude);
    }

    None
}

enum FindSessionResult {
    NotFound,
    Ambiguous(Vec<SessionInfo>),
    Found(EntityId),
}

/// A request waiting in the FIFO queue for a busy session to free up.
struct QueuedRequest {
    req_id: String,
    provider: String,
    prompt: String,
    terminal_view_id: EntityId,
}

/// A launch request waiting for a new pane to be created.
struct PendingLaunch {
    provider: String,
    agent: CLIAgent,
    alias: Option<String>,
    prompt: Option<String>,
    cwd: Option<String>,
}

/// State tracking for an active chain execution.
struct ChainState {
    chain_id: String,
    steps: Vec<protocol::ChainStep>,
    caller: String,
    current_step: usize,
    step_results: Vec<ChainStepResult>,
    current_req_id: Option<String>,
    status: String,
    created_at_ms: u64,
}

#[derive(Debug, Clone, Copy)]
enum CaptureSource {
    BlockCompleted,
    RawOutputScan,
    OutputScan,
    SessionCompletion,
    ExplicitReply,
    PassiveRawOutput,
    PassiveGridOutput,
    PassiveTimeoutPartial,
}

impl CaptureSource {
    fn as_str(&self) -> &'static str {
        match self {
            CaptureSource::BlockCompleted => "block_completed",
            CaptureSource::RawOutputScan => "raw_output_scan",
            CaptureSource::OutputScan => "output_scan",
            CaptureSource::SessionCompletion => "session_completion",
            CaptureSource::ExplicitReply => "explicit_reply",
            CaptureSource::PassiveRawOutput => "passive_raw_output",
            CaptureSource::PassiveGridOutput => "passive_grid_output",
            CaptureSource::PassiveTimeoutPartial => "passive_timeout_partial",
        }
    }

    fn to_string(&self) -> String {
        self.as_str().to_string()
    }

    fn default_confidence(&self) -> f64 {
        match self {
            CaptureSource::ExplicitReply => 1.0,
            CaptureSource::BlockCompleted
            | CaptureSource::RawOutputScan
            | CaptureSource::OutputScan
            | CaptureSource::SessionCompletion => 0.95,
            CaptureSource::PassiveRawOutput => 0.7,
            CaptureSource::PassiveGridOutput => 0.55,
            CaptureSource::PassiveTimeoutPartial => 0.3,
        }
    }
}

#[derive(Debug, Clone)]
struct ReplyCaptureMeta {
    source: CaptureSource,
    confidence: f64,
    warnings: Vec<String>,
    raw_len: usize,
    filtered_len: usize,
    truncated: bool,
}

impl ReplyCaptureMeta {
    fn from_source_and_reply(source: CaptureSource, reply: &str) -> Self {
        Self {
            source,
            confidence: source.default_confidence(),
            warnings: Vec::new(),
            raw_len: reply.len(),
            filtered_len: reply.len(),
            truncated: false,
        }
    }
}

#[derive(Debug)]
struct ReplyCaptureDiagnostics {
    output_len: usize,
    req_marker_pos: Option<usize>,
    start_tag_count: usize,
    last_start_tag_pos: Option<usize>,
    end_tag_count: usize,
    last_end_tag_pos: Option<usize>,
    done_marker_pos: Option<usize>,
    contains_instruction_text: bool,
}

fn collect_reply_capture_diagnostics(req_id: &str, output: &str) -> ReplyCaptureDiagnostics {
    let req_marker = format!("[CCB_REQ_ID:{}]", req_id);
    let start_tag = format!("[CCB_START:{}]", req_id);
    let end_tag = format!("[CCB_END:{}]", req_id);
    let done_marker = format!("CCB_DONE:{}", req_id);

    let start_positions: Vec<usize> = output
        .match_indices(&start_tag)
        .map(|(pos, _)| pos)
        .collect();
    let end_positions: Vec<usize> = output.match_indices(&end_tag).map(|(pos, _)| pos).collect();

    ReplyCaptureDiagnostics {
        output_len: output.len(),
        req_marker_pos: output.rfind(&req_marker),
        start_tag_count: start_positions.len(),
        last_start_tag_pos: start_positions.last().copied(),
        end_tag_count: end_positions.len(),
        last_end_tag_pos: end_positions.last().copied(),
        done_marker_pos: output.rfind(&done_marker),
        contains_instruction_text: output.contains("Before your final reply")
            || output.contains("After your final reply")
            || output.contains("on its own line")
            || output.contains("Reply using exactly this format"),
    }
}

fn log_reply_capture_attempt(
    req_id: &str,
    source: CaptureSource,
    view_id: EntityId,
    output: &str,
    reply_len: usize,
    will_finalize: bool,
) {
    let diag = collect_reply_capture_diagnostics(req_id, output);
    log::info!(
        "CCB诊断: req_id={} source={:?} view_id={} output_len={} req_marker_pos={:?} \
         start_count={} last_start={:?} end_count={} last_end={:?} done_pos={:?} \
         has_instruction={} reply_len={} will_finalize={}",
        req_id,
        source,
        view_id,
        diag.output_len,
        diag.req_marker_pos,
        diag.start_tag_count,
        diag.last_start_tag_pos,
        diag.end_tag_count,
        diag.last_end_tag_pos,
        diag.done_marker_pos,
        diag.contains_instruction_text,
        reply_len,
        will_finalize,
    );
}

fn write_reply_capture_debug_file(req_id: &str, source: CaptureSource, output: &str, reply: &str) {
    let diag = collect_reply_capture_diagnostics(req_id, output);
    let debug_path = std::env::temp_dir().join(format!(
        "ccb_capture_{}_{}.txt",
        req_id,
        capture_source_name(source)
    ));
    let content = format!(
        "req_id={}\nsource={:?}\nreply_len={}\ndiag={:#?}\n\n---OUTPUT---\n{}\n---END---\n",
        req_id,
        source,
        reply.len(),
        diag,
        output
    );

    if let Err(err) = std::fs::write(&debug_path, content) {
        log::warn!(
            "CCB诊断: 写入 capture debug 文件失败 req_id={} err={}",
            req_id,
            err
        );
    } else {
        log::info!(
            "CCB诊断: 已写入 capture debug 文件 req_id={} path={:?}",
            req_id,
            debug_path
        );
    }
}

fn capture_source_name(source: CaptureSource) -> &'static str {
    source.as_str()
}

fn debug_prefix(input: &str, max_chars: usize) -> String {
    input.chars().take(max_chars).collect()
}

#[derive(Debug)]
struct SelectedReplyCapture<'a> {
    source: CaptureSource,
    output: &'a str,
    reply: String,
}

#[derive(Debug)]
struct MarkerDiagnostics {
    len: usize,
    has_start: bool,
    has_end: bool,
    has_reply_closure: bool,
}

fn select_reply_capture_for_scan<'a>(
    req_id: &str,
    raw_output: Option<&'a str>,
    grid_output: Option<&'a str>,
) -> Option<SelectedReplyCapture<'a>> {
    select_reply_capture(req_id, raw_output, grid_output, CaptureSource::OutputScan)
}

fn select_reply_capture<'a>(
    req_id: &str,
    raw_output: Option<&'a str>,
    grid_output: Option<&'a str>,
    grid_source: CaptureSource,
) -> Option<SelectedReplyCapture<'a>> {
    if let Some(raw_output) = raw_output.filter(|output| !output.is_empty()) {
        let reply = extract_reply(req_id, raw_output);
        if !reply.trim().is_empty() {
            return Some(SelectedReplyCapture {
                source: CaptureSource::RawOutputScan,
                output: raw_output,
                reply,
            });
        }

        let raw_diag = collect_marker_diagnostics(req_id, Some(raw_output));
        if raw_diag.has_start && !raw_diag.has_end {
            return None;
        }
    }

    let grid_output = grid_output?;
    let reply = extract_reply(req_id, grid_output);
    if reply.trim().is_empty() {
        return None;
    }

    Some(SelectedReplyCapture {
        source: grid_source,
        output: grid_output,
        reply,
    })
}

fn collect_marker_diagnostics(req_id: &str, output: Option<&str>) -> MarkerDiagnostics {
    let Some(output) = output else {
        return MarkerDiagnostics {
            len: 0,
            has_start: false,
            has_end: false,
            has_reply_closure: false,
        };
    };

    let marker_ids = completion::reply_marker_ids(req_id);
    MarkerDiagnostics {
        len: output.len(),
        has_start: marker_ids.iter().any(|marker_id| {
            !completion::find_unwrapped_ccb_tag_ranges(output, "CCB_START", marker_id).is_empty()
        }),
        has_end: marker_ids.iter().any(|marker_id| {
            !completion::find_unwrapped_ccb_tag_ranges(output, "CCB_END", marker_id).is_empty()
        }),
        has_reply_closure: marker_ids.iter().any(|marker_id| {
            completion::find_last_complete_reply_span(output, marker_id).is_some()
        }),
    }
}

fn log_scan_tick_diagnostics(req_id: &str, raw_output: Option<&str>, grid_output: Option<&str>) {
    let raw_diag = collect_marker_diagnostics(req_id, raw_output);
    let grid_diag = collect_marker_diagnostics(req_id, grid_output);

    log::info!(
        "CCB_SCAN_TICK: req={} RAW_LEN={} GRID_LEN={} RAW_HAS_REPLY_CLOSURE={} GRID_HAS_REPLY_CLOSURE={} RAW_HAS_START={} RAW_HAS_END={} GRID_HAS_START={} GRID_HAS_END={}",
        req_id,
        raw_diag.len,
        grid_diag.len,
        raw_diag.has_reply_closure,
        grid_diag.has_reply_closure,
        raw_diag.has_start,
        raw_diag.has_end,
        grid_diag.has_start,
        grid_diag.has_end,
    );

    let tick_debug = std::env::temp_dir().join(format!("ccb_tick_{}.txt", req_id));
    let raw_debug = debug_output_for_tick(raw_output, &raw_diag);
    let grid_debug = debug_output_for_tick(grid_output, &grid_diag);
    let _ = std::fs::write(
        &tick_debug,
        format!(
            "RAW_LEN={}\nGRID_LEN={}\nRAW_HAS_REPLY_CLOSURE={}\nGRID_HAS_REPLY_CLOSURE={}\nRAW_HAS_START={}\nRAW_HAS_END={}\nGRID_HAS_START={}\nGRID_HAS_END={}\n---RAW---\n{}\n---GRID---\n{}\n---",
            raw_diag.len,
            grid_diag.len,
            raw_diag.has_reply_closure,
            grid_diag.has_reply_closure,
            raw_diag.has_start,
            raw_diag.has_end,
            grid_diag.has_start,
            grid_diag.has_end,
            raw_debug,
            grid_debug,
        ),
    );
}

fn debug_output_for_tick(output: Option<&str>, diag: &MarkerDiagnostics) -> String {
    let Some(output) = output else {
        return String::new();
    };
    if diag.has_start || diag.has_end {
        output.to_string()
    } else {
        debug_prefix(output, 3000)
    }
}

/// Minimum character count for a passive capture to be considered a valid reply.
const PASSIVE_CAPTURE_MIN_CHARS: usize = 50;

/// Clean passive-captured raw output into a reply candidate.
///
/// Returns (cleaned_content, warnings). Steps:
/// 1. Strip everything up to and including the [CCB_REQ_ID:xxx] line (prompt echo).
/// 2. Remove instruction text (warp-reply commands, marker instructions).
/// 3. Remove leftover [CCB_START:...]/[CCB_END:...] markers that never closed properly.
/// 4. Compress consecutive blank lines to at most 2.
/// 5. Remove known provider chrome (Kimi shell tags, Codex status lines, etc.).
/// 6. Trim leading/trailing whitespace.
fn clean_passive_output(raw_output: &str, provider: &str) -> (String, Vec<String>) {
    let mut warnings = Vec::new();
    let mut text = raw_output;

    // Step 1: Remove prompt echo — everything up to and including [CCB_REQ_ID:xxx] line.
    if let Some(pos) = text.find("[CCB_REQ_ID:") {
        // Find end of this line
        let line_end = text[pos..]
            .find('\n')
            .map(|p| pos + p + 1)
            .unwrap_or(text.len());
        text = &text[line_end..];
    }

    // Step 2: Remove instruction text lines.
    let instruction_patterns = [
        "After completing this task",
        "After completing this task, submit your reply using:",
        "warp-reply --req-id",
        "If warp-reply is unavailable or fails",
        "echo \"$CONTENT\" | warp-reply",
        "--content-file <file>",
        "--stdin",
        "put markers on their own lines",
        "no backticks",
        "<your reply>",
    ];
    let mut lines: Vec<&str> = text.lines().collect();
    lines.retain(|line| {
        let trimmed = line.trim();
        if trimmed.is_empty() {
            return true; // keep blank lines for now; compress later
        }
        for pattern in &instruction_patterns {
            if trimmed.contains(pattern) {
                return false;
            }
        }
        true
    });

    // Step 3: Remove leftover CCB markers that never formed a valid closure.
    let marker_re = regex_lazy();
    let filtered_lines: Vec<String> = lines
        .iter()
        .map(|l| marker_re.replace_all(l, "").to_string())
        .collect();

    // Step 4: Compress consecutive blank lines to at most 2.
    let mut compressed = String::new();
    let mut consecutive_blank = 0usize;
    for line in &filtered_lines {
        if line.trim().is_empty() {
            consecutive_blank += 1;
            if consecutive_blank <= 2 {
                compressed.push('\n');
            }
        } else {
            consecutive_blank = 0;
            compressed.push_str(line);
            compressed.push('\n');
        }
    }

    // Step 5: Remove known provider chrome.
    let provider_lower = provider.to_ascii_lowercase();
    let cleaned = match provider_lower.as_str() {
        "kimi" => clean_kimi_chrome(&compressed, &mut warnings),
        "codex" => clean_codex_chrome(&compressed),
        "droid" => clean_droid_chrome(&compressed),
        _ => compressed,
    };

    // Step 6: Trim.
    let result = cleaned.trim().to_string();

    // Warn if result seems to contain thinking/intermediate output.
    if result.contains("让我思考") || result.contains("让我分析") || result.contains("Let me think")
    {
        warnings.push("passive_capture_contains_thinking".to_string());
    }

    (result, warnings)
}

fn regex_lazy() -> regex::Regex {
    use std::sync::OnceLock;
    static RE: OnceLock<regex::Regex> = OnceLock::new();
    RE.get_or_init(|| {
        regex::Regex::new(
            r"\[CCB_START:reply-[^\]]*\]|\[CCB_END:reply-[^\]]*\]|\[CCB_DONE:[^\]]*\]",
        )
        .unwrap()
    })
    .clone()
}

/// Remove Kimi-specific chrome: function call markers like functions.Shell:N, <system>...</system> tags.
fn clean_kimi_chrome(text: &str, warnings: &mut Vec<String>) -> String {
    let re = regex::Regex::new(r"functions\.\w+:\d+").unwrap();
    let mut result = re.replace_all(text, "").to_string();

    // Remove <system>...</system> blocks.
    let sys_re = regex::Regex::new(r"(?s)<system>.*?</system>").unwrap();
    if sys_re.is_match(&result) {
        warnings.push("passive_capture_contains_system_tags".to_string());
    }
    result = sys_re.replace_all(&result, "").to_string();

    // Remove lines that are just standalone markers like ╭─...╮ boxes from Kimi UI.
    let mut cleaned_lines = Vec::new();
    for line in result.lines() {
        let trimmed = line.trim();
        if trimmed.starts_with('╭') || trimmed.starts_with('╰') || trimmed.starts_with('│') {
            continue;
        }
        cleaned_lines.push(line);
    }
    cleaned_lines.join("\n")
}

/// Remove Codex-specific chrome: status/reasoning lines starting with ›.
fn clean_codex_chrome(text: &str) -> String {
    text.lines()
        .filter(|line| !line.trim().starts_with('›'))
        .collect::<Vec<&str>>()
        .join("\n")
}

/// Droid-specific chrome removal. Currently a no-op; add patterns as needed.
fn clean_droid_chrome(text: &str) -> String {
    text.to_string()
}

/// Build a passive capture candidate from raw/grid output when all marker strategies failed.
///
/// Returns a `ReplyCaptureMeta` if the cleaned output is substantial enough (>= PASSIVE_CAPTURE_MIN_CHARS).
fn build_passive_candidate(
    req_id: &str,
    raw_output: Option<&str>,
    grid_output: Option<&str>,
    provider: &str,
) -> Option<(String, ReplyCaptureMeta)> {
    // Prefer raw output (higher fidelity) over grid output.
    if let Some(raw) = raw_output.filter(|s| !s.is_empty()) {
        let (cleaned, warnings) = clean_passive_output(raw, provider);
        if cleaned.len() >= PASSIVE_CAPTURE_MIN_CHARS {
            let raw_len = raw.len();
            let filtered_len = cleaned.len();
            log::info!(
                "CCB_PASSIVE: built PassiveRawOutput candidate req_id={} raw_len={} filtered_len={} warnings={:?}",
                req_id,
                raw_len,
                filtered_len,
                warnings,
            );
            return Some((
                cleaned,
                ReplyCaptureMeta {
                    source: CaptureSource::PassiveRawOutput,
                    confidence: 0.7,
                    warnings,
                    raw_len,
                    filtered_len,
                    truncated: false,
                },
            ));
        }
        log::info!(
            "CCB_PASSIVE: raw output too short after cleaning ({} chars), trying grid req_id={}",
            cleaned.len(),
            req_id,
        );
    }

    // Fallback to grid output.
    if let Some(grid) = grid_output.filter(|s| !s.is_empty()) {
        let (cleaned, warnings) = clean_passive_output(grid, provider);
        if cleaned.len() >= PASSIVE_CAPTURE_MIN_CHARS {
            let raw_len = grid.len();
            let filtered_len = cleaned.len();
            log::info!(
                "CCB_PASSIVE: built PassiveGridOutput candidate req_id={} raw_len={} filtered_len={} warnings={:?}",
                req_id,
                raw_len,
                filtered_len,
                warnings,
            );
            return Some((
                cleaned,
                ReplyCaptureMeta {
                    source: CaptureSource::PassiveGridOutput,
                    confidence: 0.55,
                    warnings,
                    raw_len,
                    filtered_len,
                    truncated: false,
                },
            ));
        }
        log::info!(
            "CCB_PASSIVE: grid output also too short after cleaning ({} chars) req_id={}",
            cleaned.len(),
            req_id,
        );
    }

    None
}

/// Extract reply content from terminal output.
/// Strategy 1: find the terminal [CCB_START:xxx]...[CCB_END:xxx] pair.
/// Strategy 2: find CCB_DONE:xxx fallback.
fn extract_reply(req_id: &str, output: &str) -> String {
    log::info!(
        "CCB_DEBUG extract_reply: req_id={}, output_len={}",
        req_id,
        output.len()
    );

    let start_tag = format!("[CCB_START:{}]", req_id);
    let end_tag = format!("[CCB_END:{}]", req_id);
    let marker_ids = completion::reply_marker_ids(req_id);

    log::info!(
        "CCB_DEBUG extract_reply: searching marker_ids={:?}, legacy_start={:?}, legacy_end={:?}",
        marker_ids,
        start_tag,
        end_tag
    );

    // Strategy 0: 取最后一个有效 START/END 闭环。流式输出中，提示词和思考文本
    // 都可能复述 marker，必须枚举所有候选区间，不能命中第一个就结束。
    let mut saw_new_end_marker = false;
    let mut saw_new_start_marker = false;
    for marker_id in &marker_ids {
        if !completion::find_unwrapped_ccb_tag_ranges(output, "CCB_START", marker_id).is_empty() {
            saw_new_start_marker = true;
        }

        let end_ranges = completion::find_unwrapped_ccb_tag_ranges(output, "CCB_END", marker_id);
        if !end_ranges.is_empty() {
            saw_new_end_marker = true;
        }

        if let Some((content_start, end_pos)) =
            completion::find_last_complete_reply_span(output, marker_id)
        {
            let reply = output[content_start..end_pos].trim();
            if completion::is_valid_reply_content(reply) {
                log::info!(
                    "CCB_DEBUG: complete tag scan extracted reply_len={} marker_id={}",
                    reply.len(),
                    marker_id
                );
                return reply.to_string();
            }
        }
    }
    if saw_new_end_marker && saw_new_start_marker {
        log::info!("CCB_DEBUG: saw CCB markers but no valid reply closure yet");
        return String::new();
    }

    // Strategy 1: find [CCB_START:xxx] occurrences, iterating from end.
    // Skip backtick-wrapped markers (instruction text) and instruction lines.
    let mut search_from = output.len();
    while let Some(start_pos) = output[..search_from].rfind(&start_tag) {
        // Skip if this marker is wrapped in backticks (instruction text)
        let preceded_by_backtick = start_pos > 0 && output.as_bytes()[start_pos - 1] == b'`';
        let after_start = start_pos + start_tag.len();
        let followed_by_backtick =
            after_start < output.len() && output.as_bytes()[after_start] == b'`';
        if preceded_by_backtick || followed_by_backtick {
            log::info!(
                "CCB_DEBUG: skipping backtick-wrapped start_tag at pos {}",
                start_pos
            );
            search_from = start_pos;
            continue;
        }

        // Find the line containing this match
        let line_start = output[..start_pos].rfind('\n').map(|p| p + 1).unwrap_or(0);
        let line_end = output[start_pos..]
            .find('\n')
            .map(|p| start_pos + p)
            .unwrap_or(output.len());
        let line = &output[line_start..line_end];

        log::info!(
            "CCB_DEBUG: found start_tag at pos {}, line: {:?}",
            start_pos,
            line
        );

        // Skip instruction lines (old format)
        if line.contains("Before your final reply")
            || line.contains("After your final reply")
            || line.contains("Reply using exactly this format")
        {
            log::info!("CCB_DEBUG: skipping instruction line");
            search_from = line_start;
            continue;
        }

        // This is the agent's marker — find the matching end tag after it
        let content_start = start_pos + start_tag.len();
        if content_start < output.len() {
            let remaining = &output[content_start..];
            // Find end tag that is NOT wrapped in backticks
            let mut end_search = 0;
            let end_pos = loop {
                if let Some(pos) = remaining[end_search..].find(&end_tag) {
                    let abs = end_search + pos;
                    let pre_backtick = abs > 0 && remaining.as_bytes()[abs - 1] == b'`';
                    let after_end = abs + end_tag.len();
                    let post_backtick =
                        after_end < remaining.len() && remaining.as_bytes()[after_end] == b'`';
                    if !pre_backtick && !post_backtick {
                        break Some(abs);
                    }
                    end_search = after_end;
                } else {
                    break None;
                }
            };
            if let Some(end_pos) = end_pos {
                let reply = remaining[..end_pos].trim();
                log::info!("CCB_DEBUG: found end_tag, reply_len={}", reply.len());
                // Skip if content is instruction text
                if reply.contains("on its own line")
                    || reply.contains("After your final reply")
                    || reply.contains("Reply using exactly this format")
                    || reply.contains("without backticks")
                    || reply.contains("<your reply>")
                {
                    log::info!("CCB_DEBUG: skipping instruction content");
                    search_from = line_start;
                    continue;
                }
                if !reply.is_empty() {
                    return reply.to_string();
                }
            } else {
                log::info!("CCB_DEBUG: end_tag NOT found after start_tag");
            }
        }
        break;
    }

    // Strategy 2: Find CCB_DONE:xxx fallback
    let done_marker = format!("CCB_DONE:{}", req_id);
    if let Some(done_pos) = output.rfind(&done_marker) {
        log::info!("CCB_DEBUG: found CCB_DONE at pos {}", done_pos);
        let req_marker = format!("[CCB_REQ_ID:{}]", req_id);
        if let Some(req_pos) = output.rfind(&req_marker) {
            if req_pos < done_pos {
                let between = &output[req_pos..done_pos];
                let mut newline_count = 0;
                let mut reply_start = 0;
                for (i, c) in between.char_indices() {
                    if c == '\n' {
                        newline_count += 1;
                        if newline_count == 3 {
                            reply_start = i + 1;
                            break;
                        }
                    }
                }
                if reply_start > 0 && reply_start < between.len() {
                    let reply = between[reply_start..].trim();
                    if !reply.is_empty() {
                        return reply.to_string();
                    }
                }
            }
        }
    }

    // Strategy 3: END found but START not in captured output.
    // Find END marker not wrapped in backticks, extract content before it.
    {
        let mut end_search = 0;
        let naked_end = loop {
            if let Some(pos) = output[end_search..].find(&end_tag) {
                let abs = end_search + pos;
                let pre_bt = abs > 0 && output.as_bytes()[abs - 1] == b'`';
                let after_abs = abs + end_tag.len();
                let post_bt = after_abs < output.len() && output.as_bytes()[after_abs] == b'`';
                if !pre_bt && !post_bt {
                    break Some(abs);
                }
                end_search = after_abs;
            } else {
                break None;
            }
        };
        if let Some(end_pos) = naked_end {
            log::info!(
                "CCB_DEBUG: START not found but naked END found at pos {}, using fallback",
                end_pos
            );
            let before_end = &output[..end_pos];
            let mut candidate = before_end.trim_end();
            for _ in 0..10 {
                if let Some(last_newline) = candidate.rfind('\n') {
                    let last_line = candidate[last_newline + 1..].trim();
                    if last_line.contains("on its own line")
                        || last_line.contains("After your final reply")
                        || last_line.contains("Before your final reply")
                        || last_line.contains("Reply using exactly this format")
                        || last_line.contains("without backticks")
                        || last_line.contains("<your reply>")
                        || last_line.starts_with(&format!("[CCB_REQ_ID:{}]", req_id))
                        || last_line.contains(&start_tag)
                        || last_line.contains(&end_tag)
                        || last_line.contains('`')
                    {
                        candidate = candidate[..last_newline].trim_end();
                        continue;
                    }
                }
                break;
            }
            if !candidate.is_empty() {
                let lines: Vec<&str> = candidate.lines().collect();
                let start_idx = lines.len().saturating_sub(200);
                let reply = lines[start_idx..].join("\n").trim().to_string();
                if !reply.is_empty() {
                    log::info!("CCB_DEBUG: fallback extracted {} chars", reply.len());
                    return reply;
                }
            }
        }
    }

    log::info!("CCB_DEBUG: no reply extracted, returning empty");
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn test_extract_reply_new_format() {
        let output = "thinking...\ntool calls...\n[CCB_START:abc123]\nHello from CCB Bus!\n[CCB_END:abc123]\n";
        let reply = extract_reply("abc123", output);
        assert_eq!(reply, "Hello from CCB Bus!");
    }

    #[test]
    fn test_extract_reply_new_format_multiline() {
        let output =
            "lots of intermediate output\n[CCB_START:xyz]\nLine 1\nLine 2\nLine 3\n[CCB_END:xyz]\n";
        let reply = extract_reply("xyz", output);
        assert_eq!(reply, "Line 1\nLine 2\nLine 3");
    }

    #[test]
    fn test_extract_reply_old_format_fallback() {
        let output = "some earlier output\n[CCB_REQ_ID:abc123]\nSay hello\nReply with CCB_DONE:abc123 when done.\n● Hello from CCB Bus!\nCCB_DONE:abc123\n";
        let reply = extract_reply("abc123", output);
        assert_eq!(reply, "● Hello from CCB Bus!");
    }

    #[test]
    fn test_extract_reply_old_format_multiline() {
        let output = "[CCB_REQ_ID:xyz]\nExplain Rust\nReply with CCB_DONE:xyz when done.\nLine 1\nLine 2\nLine 3\nCCB_DONE:xyz\n";
        let reply = extract_reply("xyz", output);
        assert_eq!(reply, "Line 1\nLine 2\nLine 3");
    }

    #[test]
    fn test_extract_reply_no_markers() {
        let output = "some random output without markers";
        let reply = extract_reply("abc", output);
        assert_eq!(reply, "");
    }

    #[test]
    fn test_clean_passive_output_removes_instruction_echo() {
        let raw = "some prompt text\n[CCB_REQ_ID:abc-123]\nHello this is my reply\nAfter completing this task...\nwarp-reply --req-id";
        let (cleaned, _warnings) = clean_passive_output(raw, "kimi");
        assert!(cleaned.contains("Hello this is my reply"));
        assert!(!cleaned.contains("CCB_REQ_ID"));
        assert!(!cleaned.contains("warp-reply"));
        assert!(!cleaned.contains("After completing"));
    }

    #[test]
    fn test_clean_passive_output_preserves_valid_content() {
        let raw = "[CCB_REQ_ID:abc]\n这是我的详细回复，包含代码和解释。这是一段很长的内容。";
        let (cleaned, _warnings) = clean_passive_output(raw, "kimi");
        assert!(cleaned.contains("这是我的详细回复"));
    }

    #[test]
    fn test_clean_passive_output_removes_kimi_chrome() {
        let raw = "[CCB_REQ_ID:x]\nfunctions.Shell:6\n<system>Command executed</system>\nMy actual reply here with enough text to pass threshold";
        let (cleaned, _warnings) = clean_passive_output(raw, "kimi");
        assert!(!cleaned.contains("functions.Shell"));
        assert!(!cleaned.contains("<system>"));
        assert!(cleaned.contains("actual reply"));
    }

    #[test]
    fn test_build_passive_candidate_returns_none_for_short_output() {
        let result = build_passive_candidate("short-req", Some("short"), None, "kimi");
        assert!(result.is_none());
    }

    #[test]
    fn test_build_passive_candidate_returns_some_for_valid_output() {
        let raw = "[CCB_REQ_ID:x]\nThis is a valid reply that is long enough to pass the minimum character threshold for passive capture.";
        let result = build_passive_candidate("valid-req", Some(raw), None, "claude");
        assert!(result.is_some());
        let (content, meta) = result.unwrap();
        assert!(content.contains("valid reply"));
        assert_eq!(meta.source.to_string(), "passive_raw_output");
        assert!(meta.confidence > 0.0 && meta.confidence < 1.0);
    }

    #[test]
    fn test_capture_source_confidence_grading() {
        assert_eq!(CaptureSource::ExplicitReply.default_confidence(), 1.0);
        assert_eq!(CaptureSource::PassiveRawOutput.default_confidence(), 0.7);
        assert_eq!(CaptureSource::PassiveGridOutput.default_confidence(), 0.55);
        assert_eq!(
            CaptureSource::PassiveTimeoutPartial.default_confidence(),
            0.3
        );
    }

    #[test]
    fn test_completion_strategy_session_aware_faster() {
        let mut tracker = CompletionTracker::new();
        tracker.register(
            "session-aware".to_string(),
            "claude".to_string(),
            EntityId::from_usize(1),
        );
        tracker.register(
            "raw-only".to_string(),
            "kimi".to_string(),
            EntityId::from_usize(2),
        );
        tracker.set_has_session_listener("session-aware", true);

        std::thread::sleep(Duration::from_millis(3100));

        assert!(tracker.should_finalize_passive("session-aware", Duration::from_secs(2)));
        assert!(!tracker.should_finalize_passive("raw-only", Duration::from_secs(8)));
    }

    #[test]
    fn test_stored_response_v1_backward_compat() {
        let raw = r#"{
  "req_id": "legacy-req",
  "provider": "codex",
  "status": "success",
  "content": "legacy content",
  "created_at_ms": 1000,
  "updated_at_ms": 2000,
  "schema_version": 1
}"#;

        let response: store::StoredResponse = serde_json::from_str(raw).unwrap();
        assert_eq!(response.req_id, "legacy-req");
        assert_eq!(response.source, None);
        assert_eq!(response.confidence, None);
        assert!(response.warnings.is_empty());
        assert_eq!(response.raw_len, None);
        assert_eq!(response.filtered_len, None);
        assert_eq!(response.truncated, None);
        assert_eq!(response.finalized_at_ms, None);
    }

    #[test]
    fn test_debug_prefix_does_not_split_utf8_chars() {
        let output = format!("{}{}", "a".repeat(2998), "─tail");
        let truncated = debug_prefix(&output, 3000);
        assert!(truncated.is_char_boundary(truncated.len()));
        assert!(output.starts_with(&truncated));
    }

    #[test]
    fn test_select_reply_capture_prefers_raw_when_grid_is_damaged() {
        let req_id = "raw-r1";
        let raw = "\
[CCB_START:reply-raw-r1]
raw reply
[CCB_END:reply-raw-r1]
";
        let grid = "\
[CCB_START:reply-raw-r1]
grid reply
CCB_END:reply-raw-r1]
";

        let selected =
            select_reply_capture_for_scan(req_id, Some(raw), Some(grid)).expect("raw should win");

        assert!(matches!(selected.source, CaptureSource::RawOutputScan));
        assert_eq!(selected.reply, "raw reply");
    }

    #[test]
    fn test_select_reply_capture_does_not_fallback_to_grid_when_raw_is_available() {
        let req_id = "raw-r2";
        let raw = "[CCB_START:reply-raw-r2]\npartial reply\n";
        let grid = "\
[CCB_START:reply-raw-r2]
grid reply
[CCB_END:reply-raw-r2]
";

        assert!(
            select_reply_capture_for_scan(req_id, Some(raw), Some(grid)).is_none(),
            "raw 可用时，grid 只能作为诊断，不能提前完成请求"
        );
    }

    #[test]
    fn test_select_reply_capture_falls_back_to_grid_when_raw_has_no_valid_closure() {
        let req_id = "grid-fallback-r1";
        let raw = "\
[CCB_REQ_ID:grid-fallback-r1]
Reply using exactly this format:
[CCB_START:reply-grid-fallback-r1]
<your reply>
[CCB_END:reply-grid-fallback-r1]
\x08• [CCB_START:reply-grid-fallback-r1
  ] hello from agent [CCB_END:reply-
  grid-fallback-r1]
";
        let grid = "\
[CCB_START:reply-grid-fallback-r1]
hello from agent
[CCB_END:reply-grid-fallback-r1]
";

        let selected = select_reply_capture_for_scan(req_id, Some(raw), Some(grid))
            .expect("grid should be allowed when raw cannot form a valid closure");

        assert!(matches!(selected.source, CaptureSource::OutputScan));
        assert_eq!(selected.reply, "hello from agent");
    }

    #[test]
    fn test_select_reply_capture_uses_grid_when_raw_is_unavailable() {
        let req_id = "grid-r1";
        let grid = "\
[CCB_START:reply-grid-r1]
grid reply
[CCB_END:reply-grid-r1]
";

        let selected =
            select_reply_capture_for_scan(req_id, None, Some(grid)).expect("grid fallback works");

        assert!(matches!(selected.source, CaptureSource::OutputScan));
        assert_eq!(selected.reply, "grid reply");
    }

    #[test]
    fn test_extract_reply_real_kimi_output() {
        // Simulates actual Kimi terminal output with new format
        let output = "[CCB_REQ_ID:20260514-195447-8c242b0c]\n\
            请写一段100字以上的短文，主题是：为什么跨Agent通信对AI协作很重要？\n\
            Reply using exactly this format (output markers without backticks):\n\
            `[CCB_START:20260514-195447-8c242b0c]`\n\
            <your reply>\n\
            `[CCB_END:20260514-195447-8c242b0c]`\n\
            \n\
            • 用户要求写一段100字以上的短文\n\
            • 这是一个简单的创作任务\n\
            \n\
            [CCB_START:20260514-195447-8c242b0c]\n\
            \n\
            在复杂任务面前，单一AI Agent的能力往往受限于其训练数据、架构设计和专业领域。跨Agent通信正是打破这种局限的关键桥梁。\n\
            \n\
            [CCB_END:20260514-195447-8c242b0c]";
        let reply = extract_reply("20260514-195447-8c242b0c", output);
        assert!(
            reply.contains("在复杂任务面前"),
            "should extract actual reply, got: {:?}",
            reply
        );
        assert!(
            !reply.contains("backticks"),
            "should NOT contain instruction text, got: {:?}",
            reply
        );
    }

    #[test]
    fn test_extract_reply_agent_no_markers() {
        // Agent did NOT output markers — only instruction lines contain them
        // extract_reply should NOT extract instruction text
        let output = "[CCB_REQ_ID:abc123]\nSay hello\n\
            Reply using exactly this format (output markers without backticks):\n\
            `[CCB_START:abc123]`\n\
            <your reply>\n\
            `[CCB_END:abc123]`\n\
            \n\
            Hello, I am an AI assistant.\n\
            Nice to meet you!";
        let reply = extract_reply("abc123", output);
        assert_eq!(
            reply, "",
            "should return empty when agent didn't output markers, got: {:?}",
            reply
        );
    }

    #[test]
    fn test_extract_reply_terminal_wrapped_instruction() {
        // Simulates terminal wrapping with new format
        let output = "[CCB_REQ_ID:droid-test]\nSay hello\n\
            Reply using exactly this format (output markers\n\
            without backticks):\n\
            `[CCB_START:droid-test]`\n\
            <your reply>\n\
            `[CCB_END:droid-test]`\n\
            \n\
            [CCB_START:droid-test]\n\
            \n\
            你好！我是 Droid，一个 AI 软件工程代理。\n\
            \n\
            [CCB_END:droid-test]";
        let reply = extract_reply("droid-test", output);
        assert!(
            reply.contains("我是 Droid"),
            "should extract actual reply, got: {:?}",
            reply
        );
        assert!(
            !reply.contains("backticks"),
            "should NOT contain instruction text, got: {:?}",
            reply
        );
    }

    #[test]
    fn test_extract_reply_wrapped_end_marker_id() {
        let req_id = "20260515-130313-6aea5fe7";
        let output =
            "• [CCB_START:20260515-130313-6aea5fe7] kimi 你好 [CCB_END:20\n260515-130313-6aea5fe7]";
        let reply = extract_reply(req_id, output);
        assert_eq!(reply, "kimi 你好");
    }

    #[test]
    fn test_extract_reply_ignores_kimi_thinking_marker_mention() {
        let req_id = "20260515-155427-7d26c5be";
        let output = "\
• 用户要求我介绍一下自己，并使用特定的格式回复。格式要求是 …
  [CCB_START:20260515-155427-7d26c5be] 和 [CCB_END:20260515-
  155427-7d26c5be] 包裹回复内容，不带反引号。

  根据系统提示，我是 Kimi Code CLI。

⠙ Composing... <1s · 2 tokens";
        let reply = extract_reply(req_id, output);
        assert_eq!(reply, "");
    }

    #[test]
    fn test_extract_reply_prefers_reply_prefixed_terminal_marker() {
        let req_id = "20260515-155427-7d26c5be";
        let output = "\
[CCB_START:reply-20260515-155427-7d26c5be]
我是 Kimi Code CLI。
[CCB_END:reply-20260515-155427-
7d26c5be]

── input ──────────────────────────────────────────────────";
        let reply = extract_reply(req_id, output);
        assert_eq!(reply, "我是 Kimi Code CLI。");
    }

    #[test]
    fn test_extract_reply_allows_kimi_status_footer() {
        let req_id = "20260515-192826-f8fdad02";
        let output = "\
• [CCB_START:reply-20260515-192826-f8fdad02] 你好！我是 Kimi Code CLI。

  我的主要职责是协助你完成软件工程任务。
  [CCB_END:reply-20260515-192826-f8fdad02]



── input ──────────────────────────────────────────────────







───────────────────────────────────────────────────────────
yolo  agent (Kimi-k2.6 ●)  D:\\GitHub\\warp-ccb
                               context: 4.9% (12.8k/262.1k)";
        let reply = extract_reply(req_id, output);
        assert!(
            reply.contains("你好！我是 Kimi Code CLI。"),
            "got: {:?}",
            reply
        );
        assert!(reply.contains("协助你完成软件工程任务"), "got: {:?}", reply);
        assert!(!reply.contains("yolo  agent"), "got: {:?}", reply);
    }

    #[test]
    fn test_extract_reply_allows_droid_status_footer() {
        let req_id = "20260515-212919-2ba19670";
        let output = "\
⛬  [CCB_START:reply-20260515-212919-2ba1
   9670]
   你好！我是 Droid，一个由 Factory
   构建的 AI 软件工程代理。我可以帮助你
   完成各种软件工程任务，包括：

   •  阅读、编写和编辑代码
   •  搜索和探索代码库
   •  调试和修复问题

   我当前工作在 `D:\\GitHub\\warp-ccb`
   项目目录下。有什么我可以帮你的吗？
   [CCB_END:reply-20260515-212919-2ba196
   70]

GLM-5.1 [GLM Coding Plan China] - Openai […]

 >

[⏱ 21s] ✓ v0.126.0 ready (restart to apply)";
        let reply = extract_reply(req_id, output);
        assert!(reply.contains("你好！我是 Droid"), "got: {:?}", reply);
        assert!(reply.contains("完成各种软件工程任务"), "got: {:?}", reply);
        assert!(!reply.contains("GLM-5.1"), "got: {:?}", reply);
        assert!(
            !reply.contains("ready (restart to apply)"),
            "got: {:?}",
            reply
        );
    }

    #[test]
    fn test_extract_reply_allows_codex_status_footer() {
        let req_id = "20260515-223836-3484bb45";
        let output = "\
• [CCB_START:reply-20260515-223836-
  3484bb45]
  我是 Codex，一个在你当前工作区内协作的 AI
  编程助手。

  我的主要定位是架构协作者、代码实现者和质量把关者。
  [CCB_END:reply-20260515-223836-3484bb45]

───────────────────────────────────────────


› Explain this codebase

  gpt-5.5 xhigh · D:\\GitHub\\warp-ccb";
        let reply = extract_reply(req_id, output);
        assert!(reply.contains("我是 Codex"), "got: {:?}", reply);
        assert!(reply.contains("架构协作者"), "got: {:?}", reply);
        assert!(!reply.contains("Explain this codebase"), "got: {:?}", reply);
        assert!(!reply.contains("gpt-5.5 xhigh"), "got: {:?}", reply);
    }

    #[test]
    fn test_extract_reply_uses_last_valid_reply_closure() {
        let req_id = "20260515-225555-979c6156";
        let output = "\
[CCB_REQ_ID:20260515-225555-979c6156]
介绍一下自己
Reply using exactly this format. Put the markers on their own lines and do not wrap them in backticks:
[CCB_START:reply-20260515-225555-979c6156]
<your reply>
[CCB_END:reply-20260515-225555-979c6156]
• 用户要求我介绍自己，并使用特定的格式回复。格式要求：
  1. 使用 [CCB_START:reply-20260515-225555-979c6156] 标记开始
  2. 使用 [CCB_END:reply-20260515-225555-979c6156] 标记结束
• [CCB_START:reply-20260515-225555-979c6156]
你好！我是 Kimi Code CLI，一个由月之暗面开发的交互式 AI 代理。
[CCB_END:reply-20260515-225555-979c6156]
后面出现新的交互提示或任意终端内容";

        let reply = extract_reply(req_id, output);
        assert!(
            reply.contains("你好！我是 Kimi Code CLI"),
            "应提取最后一个真实回复闭环，got: {:?}",
            reply
        );
        assert!(!reply.contains("<your reply>"), "got: {:?}", reply);
        assert!(!reply.contains("标记开始"), "got: {:?}", reply);
        assert!(!reply.contains("交互提示"), "got: {:?}", reply);
    }

    #[test]
    fn test_extract_reply_allows_truncated_grid_end_prefix() {
        let req_id = "20260516-001753-d6940bd4";
        let output = "\
[CCB_REQ_ID:20260516-001753-d6940bd4]
Reply using exactly this format. Put the markers on their own lines and do not wrap them in backticks:
[CCB_START:reply-20260516-001753-d6940bd4]
<your reply>
[CCB_END:reply-20260516-001753-d6940bd4]

• [CCB_START:reply-20260516-001753-d6940bd4
  ]
  1. 容错与恢复：保障多智能体协作的可靠性。
  2. 智能路由：降低耦合并提升系统扩展性。
  3. 全链路可观测：缩短跨 Agent 故障定位时间。
     B_END:reply-20260516-001753-d6940bd4]

── input ─────────────────────────────────
yolo  agent (Kimi-k2.6 ●)  D:\\GitHub\\warp-ccb";

        let reply = extract_reply(req_id, output);
        assert!(reply.contains("容错与恢复"), "got: {:?}", reply);
        assert!(reply.contains("智能路由"), "got: {:?}", reply);
        assert!(reply.contains("全链路可观测"), "got: {:?}", reply);
        assert!(!reply.contains("<your reply>"), "got: {:?}", reply);
        assert!(!reply.contains("yolo  agent"), "got: {:?}", reply);
    }

    #[test]
    fn test_self_ask_detects_same_terminal() {
        assert!(LocalAgentBusModel::is_self_ask(
            "codex",
            "claude",
            Some(EntityId::from_usize(42)),
            EntityId::from_usize(42)
        ));
    }

    #[test]
    fn test_self_ask_detects_same_provider_without_terminal_id() {
        assert!(LocalAgentBusModel::is_self_ask(
            "claude",
            "claude",
            None,
            EntityId::from_usize(42)
        ));
    }

    #[test]
    fn test_self_ask_allows_same_provider_different_terminal_id() {
        assert!(!LocalAgentBusModel::is_self_ask(
            "claude",
            "claude",
            Some(EntityId::from_usize(7)),
            EntityId::from_usize(42)
        ));
    }

    #[test]
    fn test_validate_explicit_reply_rejects_pane_mismatch() {
        assert_eq!(
            validate_explicit_reply(
                RequestStatus::Running,
                EntityId::from_usize(42),
                Some(7),
                "reply"
            ),
            Err("reply pane mismatch")
        );
    }

    #[test]
    fn test_validate_explicit_reply_rejects_blank_content() {
        assert_eq!(
            validate_explicit_reply(
                RequestStatus::Running,
                EntityId::from_usize(42),
                Some(42),
                " \n\t"
            ),
            Err("reply content is empty")
        );
    }

    #[test]
    fn test_validate_explicit_reply_rejects_large_content() {
        let content = "a".repeat(EXPLICIT_REPLY_MAX_CONTENT_BYTES + 1);

        assert_eq!(
            validate_explicit_reply(
                RequestStatus::Running,
                EntityId::from_usize(42),
                Some(42),
                &content
            ),
            Err("content too large")
        );
    }

    #[test]
    fn test_validate_explicit_reply_keeps_finalized_requests_idempotent() {
        assert_eq!(
            validate_explicit_reply(
                RequestStatus::Success,
                EntityId::from_usize(42),
                Some(7),
                ""
            ),
            Ok(ExplicitReplyValidation::AlreadyFinalized)
        );
    }

    #[test]
    fn test_validate_explicit_reply_rejects_non_success_final_states() {
        for status in [
            RequestStatus::Queued,
            RequestStatus::Error,
            RequestStatus::Timeout,
            RequestStatus::Cancelled,
            RequestStatus::SessionBusy,
        ] {
            assert_eq!(
                validate_explicit_reply(status, EntityId::from_usize(42), Some(42), "reply"),
                Err("reply request is not running")
            );
        }
    }

    #[test]
    fn test_active_block_blocks_bus_launch_when_command_is_running() {
        assert!(active_block_blocks_bus_launch(true, false));
        assert!(!active_block_blocks_bus_launch(false, false));
        assert!(!active_block_blocks_bus_launch(true, true));
    }

    #[test]
    fn test_bus_prompt_submit_strategy_is_provider_specific() {
        assert_eq!(
            bus_prompt_submit_strategy(CLIAgent::Kimi, "hello"),
            BusPromptSubmitStrategy::Inline
        );
        assert_eq!(
            bus_prompt_submit_strategy(CLIAgent::Droid, "hello"),
            BusPromptSubmitStrategy::Inline
        );
        assert_eq!(
            bus_prompt_submit_strategy(CLIAgent::Claude, "hello"),
            BusPromptSubmitStrategy::DelayedEnter
        );
        assert_eq!(
            bus_prompt_submit_strategy(CLIAgent::Codex, "hello"),
            BusPromptSubmitStrategy::BracketedPasteDelayedEnter
        );
    }

    #[test]
    fn test_bus_prompt_submit_strategy_uses_bracketed_paste_for_multiline_prompt() {
        assert_eq!(
            bus_prompt_submit_strategy(CLIAgent::Kimi, "line 1\nline 2"),
            BusPromptSubmitStrategy::BracketedPasteDelayedEnter
        );
        assert_eq!(
            bus_prompt_submit_strategy(CLIAgent::Droid, "line 1\nline 2"),
            BusPromptSubmitStrategy::BracketedPasteDelayedEnter
        );
    }

    #[test]
    fn test_parse_bus_inject_enter_delay_ms() {
        assert_eq!(parse_bus_inject_enter_delay_ms(None), 1000);
        assert_eq!(parse_bus_inject_enter_delay_ms(Some("1500")), 1500);
        assert_eq!(parse_bus_inject_enter_delay_ms(Some("0")), 0);
        assert_eq!(parse_bus_inject_enter_delay_ms(Some("bad")), 1000);
    }

    #[test]
    fn test_detect_bus_agent_from_kimi_banner() {
        let output = "\
╭──────────────────────────────────────────────────────────╮
│   ▐█▛█▛█▌  Welcome to Kimi Code CLI!                     │
│  Model: Kimi-k2.6                                        │
╰──────────────────────────────────────────────────────────╯";
        assert_eq!(
            detect_bus_agent_from_terminal_output(output),
            Some(CLIAgent::Kimi)
        );
    }

    #[test]
    fn test_detect_bus_agent_from_codex_output() {
        let output = "\
gpt-5.5 xhigh · D:\\GitHub\\warp-ccb

› Ready for work";
        assert_eq!(
            detect_bus_agent_from_terminal_output(output),
            Some(CLIAgent::Codex)
        );
    }

    #[test]
    fn test_detect_bus_agent_from_droid_output() {
        let output = "\
⛬ Droid
D:\\GitHub\\warp-ccb";
        assert_eq!(
            detect_bus_agent_from_terminal_output(output),
            Some(CLIAgent::Droid)
        );
    }

    #[test]
    fn test_detect_bus_agent_ignores_plain_kimi_text() {
        let output = "用户说：Kimi 你好，但这里不是 Kimi CLI 的欢迎界面。";
        assert_eq!(detect_bus_agent_from_terminal_output(output), None);
    }

    #[test]
    fn test_build_launch_command_claude() {
        let cmd = build_launch_command(&CLIAgent::Claude, "claude", None, None);
        assert!(cmd.starts_with("claude --session-id "));
        assert!(cmd.contains("--dangerously-skip-permissions"));
        assert!(cmd.ends_with('\r'));
    }

    #[test]
    fn test_build_launch_command_codex_with_prompt() {
        let cmd = build_launch_command(&CLIAgent::Codex, "codex", None, Some("hello"));
        assert!(cmd.starts_with("codex --dangerously-bypass-approvals-and-sandbox"));
        assert!(cmd.contains("hello"));
    }

    #[test]
    fn test_build_launch_command_opencode() {
        let cmd = build_launch_command(&CLIAgent::OpenCode, "opencode", None, None);
        assert!(cmd.starts_with("opencode"));
    }

    #[test]
    fn test_build_launch_command_adds_ccb_identity() {
        let cmd = build_launch_command(
            &CLIAgent::OpenCode,
            "opencode",
            Some(EntityId::from_usize(2247)),
            None,
        );
        assert!(cmd.contains("WARP_CCB_PROVIDER"));
        assert!(cmd.contains("opencode"));
        assert!(cmd.contains("2247"));
    }
}

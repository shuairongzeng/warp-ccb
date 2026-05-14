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
pub mod registry;
pub mod server;
pub mod store;

use std::collections::HashMap;

use completion::CompletionTracker;
use protocol::{
    BusAddressInfo, BusCommand, BusResponse, BusResponseData, ChainProgress, ChainStepResult,
    RequestStatus, SessionInfo, PROTOCOL_VERSION,
};
use registry::RequestRegistry;
use server::LocalAgentBusServer;
use store::ResponseStore;

use warpui::r#async::Timer;
use warpui::{Entity, EntityId, ModelContext, SingletonEntity, ViewHandle, WeakViewHandle};

use crate::pane_group::PaneGroup;
use crate::terminal::cli_agent::CLIAgent;
use crate::terminal::cli_agent_sessions::CLIAgentSessionsModel;
use crate::terminal::view::TerminalView;

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
    terminal_handles: HashMap<EntityId, WeakViewHandle<TerminalView>>,
    /// FIFO queue of requests waiting for a busy session to free up.
    request_queue: Vec<QueuedRequest>,
    /// Active chain executions.
    active_chains: HashMap<String, ChainState>,
    /// Handle to PaneGroup for creating new terminal panes.
    pane_group_handle: Option<WeakViewHandle<PaneGroup>>,
    /// Launch requests waiting for a new pane to be created.
    pending_launches: Vec<PendingLaunch>,
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
            terminal_handles: HashMap::new(),
            request_queue: Vec::new(),
            active_chains: HashMap::new(),
            pane_group_handle: None,
            pending_launches: Vec::new(),
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
    }

    /// Remove a terminal handle when the session ends.
    pub fn deregister_terminal_handle(&mut self, view_id: EntityId) {
        self.terminal_handles.remove(&view_id);
    }

    /// Register the PaneGroup handle for creating new terminal panes.
    pub fn register_pane_group_handle(&mut self, handle: WeakViewHandle<PaneGroup>) {
        log::info!("LocalAgentBus: registered PaneGroup handle");
        self.pane_group_handle = Some(handle);
    }

    /// Called when a block completes — scan output for CCB_DONE markers.
    pub fn check_block_output_for_done(&mut self, view_id: EntityId, output: &str) {
        for req_id in self.completion.pending_for_terminal(view_id) {
            if self.completion.check_done_marker(&req_id, output) {
                log::info!("LocalAgentBus: CCB_DONE detected for req {}", req_id);
                let reply = extract_reply(&req_id, output);
                self.registry.update_status(&req_id, RequestStatus::Success);
                self.registry.set_reply_content(&req_id, reply);
                self.completion.deregister(&req_id);
            }
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

            // Read last 60 rows of the active block's output grid to capture
            // both the injected prompt and the reply content.
            let output: Option<String> = handle.update(ctx, |view, _| {
                let model = view.model.lock();
                let block = model.block_list().active_block();
                Some(block.output_grid().contents_to_string(false, Some(60)))
            });

            if let Some(output) = output {
                for req_id in &req_ids {
                    if self.completion.check_done_marker(req_id, &output) {
                        log::info!(
                            "LocalAgentBus: CCB_DONE detected via output scan for req {}",
                            req_id
                        );
                        let reply = extract_reply(req_id, &output);
                        log::info!(
                            "LocalAgentBus: captured reply ({} chars) for req {}",
                            reply.len(),
                            req_id
                        );
                        self.registry.update_status(req_id, RequestStatus::Success);
                        self.registry.set_reply_content(req_id, reply);
                        self.completion.deregister(req_id);
                        self.notify_waiters(req_id);
                    }
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
                if sessions_model.session(view_id).is_some() {
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
            } else {
                // Session disappeared — mark as error
                errored.push(req_id.clone());
            }
        }

        for req_id in completed {
            self.registry.update_status(&req_id, RequestStatus::Success);
            self.completion.deregister(&req_id);
            self.notify_waiters(&req_id);
        }
        for req_id in errored {
            self.registry.update_status(&req_id, RequestStatus::Error);
            self.completion.deregister(&req_id);
            self.notify_waiters(&req_id);
        }

        // Remove resolved launch- entries so the terminal is no longer marked busy.
        for req_id in launches_resolved {
            log::info!("LocalAgentBus: launch resolved, removing entry {}", req_id);
            self.registry.remove(&req_id);
        }

        self.process_queued_requests(ctx);
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
                queue,
            } => self.handle_ask(
                provider, prompt, session_id, cwd, req_id, caller, queue, ctx,
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
                prompt,
                cwd,
            } => self.handle_launch(provider, prompt, cwd, ctx),

            BusCommand::Chain { steps, caller } => self.handle_chain(steps, caller, ctx),

            BusCommand::Wait { .. } => {
                // Handled in process_commands before reaching here
                BusResponse::error("wait command not handled in dispatch")
            }
        }
    }

    fn handle_ask(
        &mut self,
        provider: String,
        prompt: String,
        session_id: Option<String>,
        cwd: Option<String>,
        req_id: String,
        caller: String,
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
                if self.registry.has_active_for_terminal(entity_id) {
                    if queue {
                        // Queue the request for later execution.
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
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
                        });
                        self.request_queue.push(QueuedRequest {
                            req_id: req_id.clone(),
                            provider: provider.clone(),
                            prompt,
                            terminal_view_id: entity_id,
                        });
                        log::info!(
                            "LocalAgentBus: queued req {} for terminal {:?}",
                            req_id,
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
                    req_id, provider, caller, prompt, session_id, cwd, entity_id, ctx,
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
        });

        let wrapped_prompt = format!(
            "[CCB_REQ_ID:{}]\n{}\nReply with CCB_DONE:{} when done.",
            req_id, prompt, req_id
        );
        let injected = self.inject_prompt(entity_id, &wrapped_prompt, ctx);

        if injected {
            self.registry.update_status(&req_id, RequestStatus::Running);
            self.completion
                .register(req_id.clone(), provider.clone(), entity_id);
        } else {
            self.registry.update_status(&req_id, RequestStatus::Error);
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
        }

        let replies = self.registry.query_replies(provider, req_id, count);

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
                cwd: ctx_cwd.clone(),
                status: format!("{:?}", session.status).to_lowercase(),
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
            });
            for tx in senders {
                let _ = tx.send(response.clone());
            }
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

            let wrapped_prompt = format!(
                "[CCB_REQ_ID:{}]\n{}\nReply with CCB_DONE:{} when done.",
                queued.req_id, queued.prompt, queued.req_id
            );
            let injected = self.inject_prompt(queued.terminal_view_id, &wrapped_prompt, ctx);

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
            }
        }
    }

    /// Find a CLI agent session matching provider + optional session_id/cwd.
    fn find_session(
        &self,
        provider: &str,
        session_id: Option<&str>,
        cwd: Option<&str>,
        ctx: &mut ModelContext<Self>,
    ) -> FindSessionResult {
        let sessions_model = CLIAgentSessionsModel::as_ref(ctx);
        let target_agent = match resolve_agent(provider) {
            Some(a) => a,
            None => return FindSessionResult::NotFound,
        };

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
            0 => FindSessionResult::NotFound,
            1 => FindSessionResult::Found(matches[0].0),
            _ => {
                let infos: Vec<SessionInfo> = matches
                    .iter()
                    .map(|(view_id, s)| SessionInfo {
                        session_id: s.session_context.session_id.clone(),
                        provider: provider.to_string(),
                        terminal_view_id: view_id.to_string().parse().unwrap_or(0),
                        cwd: s.session_context.cwd.clone(),
                        status: format!("{:?}", s.status).to_lowercase(),
                    })
                    .collect();
                FindSessionResult::Ambiguous(infos)
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
        });

        let wrapped_prompt = format!(
            "[CCB_REQ_ID:{}]\n{}\nReply with CCB_DONE:{} when done.",
            req_id, prompt, req_id
        );
        let injected = self.inject_prompt(entity_id, &wrapped_prompt, ctx);

        if injected {
            self.registry.update_status(&req_id, RequestStatus::Running);
            self.completion
                .register(req_id.clone(), provider.clone(), entity_id);
        } else {
            self.registry.update_status(&req_id, RequestStatus::Error);
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
        let view_id = match idle_view_id {
            Some(id) => id,
            None => {
                // No idle terminal. Try to create one via PaneGroup.
                if self.create_terminal_pane(ctx) {
                    // Pane creation is scheduled async. Store the launch for later.
                    self.pending_launches.push(PendingLaunch {
                        provider: provider.clone(),
                        agent,
                        prompt,
                        cwd,
                    });
                    return BusResponse::ok(BusResponseData::Launched {
                        provider,
                        session_id: None,
                        terminal_view_id: 0, // pending
                    });
                }
                return BusResponse::error("no idle terminal available for launch");
            }
        };

        // Build the CLI startup command.
        let command = build_launch_command(&agent, prompt.as_deref());

        // Write the command to the terminal's PTY.
        let weak_handle = match self.terminal_handles.get(&view_id) {
            Some(h) => h.clone(),
            None => return BusResponse::error("terminal handle lost"),
        };

        match weak_handle.upgrade(ctx) {
            Some(handle) => {
                let bytes: Vec<u8> = command.into_bytes();
                handle.update(ctx, |view, ctx| {
                    view.write_to_pty(bytes, ctx);
                });
                log::info!(
                    "LocalAgentBus: launched {} in terminal {:?}",
                    provider,
                    view_id
                );

                // Register a placeholder request to mark this terminal as busy
                // until the CLI agent session is detected.
                let now_ms = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                self.registry.insert(registry::RequestEntry {
                    req_id: format!("launch-{}", view_id),
                    provider: provider.clone(),
                    caller: "launch".to_string(),
                    terminal_view_id: view_id,
                    session_id: None,
                    cwd: None,
                    status: RequestStatus::Injecting,
                    created_at_ms: now_ms,
                    updated_at_ms: now_ms,
                    error_message: None,
                    reply_content: None,
                });

                if let Some(ref _dir) = cwd {
                    log::warn!("LocalAgentBus: cwd not yet supported for launch, agent will use terminal's cwd");
                }

                BusResponse::ok(BusResponseData::Launched {
                    provider,
                    session_id: None,
                    terminal_view_id: view_id.to_string().parse().unwrap_or(0),
                })
            }
            None => BusResponse::error("terminal view no longer exists"),
        }
    }

    /// Find a terminal without an active CLI agent session AND without active requests.
    fn find_idle_terminal(&self, ctx: &mut ModelContext<Self>) -> Option<EntityId> {
        let sessions_model = CLIAgentSessionsModel::as_ref(ctx);
        log::info!(
            "LocalAgentBus: find_idle_terminal — checking {} handles",
            self.terminal_handles.len()
        );

        for &view_id in self.terminal_handles.keys() {
            let has_session = sessions_model.session(view_id).is_some();
            let has_active = self.registry.has_active_for_terminal(view_id);
            log::info!(
                "LocalAgentBus:   terminal {:?} — session={}, active={}",
                view_id,
                has_session,
                has_active
            );
            if !has_session && !has_active {
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
                log::warn!("LocalAgentBus: no PaneGroup handle registered");
                return false;
            }
        };

        match weak_pg.upgrade(ctx) {
            Some(pg_handle) => {
                // Spawn the pane creation on the next event loop tick to avoid reentrancy.
                ctx.spawn(
                    async move {},
                    move |me, _meta, ctx| {
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
                        }

                        // Process pending launches now that the terminal is registered.
                        me.process_pending_launches(ctx);
                    },
                );
                log::info!("LocalAgentBus: scheduled terminal pane creation via PaneGroup");
                true
            }
            None => {
                log::warn!("LocalAgentBus: PaneGroup handle no longer valid");
                false
            }
        }
    }

    /// Process pending launch requests after a new pane was created.
    pub fn process_pending_launches(&mut self, ctx: &mut ModelContext<Self>) {
        if self.pending_launches.is_empty() {
            return;
        }

        let pending = std::mem::take(&mut self.pending_launches);
        for launch in pending {
            let idle = self.find_idle_terminal(ctx);
            let view_id = match idle {
                Some(id) => id,
                None => {
                    log::warn!(
                        "LocalAgentBus: still no idle terminal after pane creation for {}",
                        launch.provider
                    );
                    continue;
                }
            };

            let command = build_launch_command(&launch.agent, launch.prompt.as_deref());
            let weak_handle = match self.terminal_handles.get(&view_id) {
                Some(h) => h.clone(),
                None => continue,
            };

            match weak_handle.upgrade(ctx) {
                Some(handle) => {
                    let bytes: Vec<u8> = command.into_bytes();
                    handle.update(ctx, |view, ctx| {
                        view.write_to_pty(bytes, ctx);
                    });
                    log::info!(
                        "LocalAgentBus: launched {} in new terminal {:?}",
                        launch.provider,
                        view_id
                    );
                }
                None => continue,
            }
        }
    }

    /// Inject a prompt into a terminal view's PTY using the stored weak handle.
    fn inject_prompt(&self, view_id: EntityId, prompt: &str, ctx: &mut ModelContext<Self>) -> bool {
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

                // Two-phase injection to work around Codex's paste burst protection:
                // Phase 1: Send text via bracketed paste
                // Phase 2: After 200ms delay, send Enter separately
                //
                // Codex suppresses Enter within ~120ms of a paste burst on Windows.
                // By delaying the Enter, it arrives outside the suppression window.

                // Phase 1: bracketed paste text
                let prompt_bytes = prompt.as_bytes();
                let mut paste_bytes = Vec::with_capacity(6 + prompt_bytes.len() + 6);
                paste_bytes.extend_from_slice(b"\x1b[200~");
                paste_bytes.extend_from_slice(prompt_bytes);
                paste_bytes.extend_from_slice(b"\x1b[201~");

                let enter_handle = handle.clone();
                handle.update(ctx, |view, ctx| {
                    // Phase 1: send bracketed paste text immediately
                    view.write_to_pty(paste_bytes, ctx);

                    // Phase 2: send Enter after 200ms delay (outside paste burst window)
                    ctx.spawn(
                        Timer::after(std::time::Duration::from_millis(200)),
                        move |view, _, ctx| {
                            view.write_to_pty(b"\r", ctx);
                        },
                    );
                });

                let _ = enter_handle; // moved into spawn closure above
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
    match provider.to_lowercase().as_str() {
        "claude" => Some(CLIAgent::Claude),
        "codex" => Some(CLIAgent::Codex),
        "gemini" => Some(CLIAgent::Gemini),
        "opencode" => Some(CLIAgent::OpenCode),
        "droid" => Some(CLIAgent::Droid),
        "copilot" => Some(CLIAgent::Copilot),
        "amp" => Some(CLIAgent::Amp),
        _ => None,
    }
}

/// Build a CLI startup command for the given agent.
/// Uses \r (CR) as the line terminator for Windows PTY compatibility.
fn build_launch_command(agent: &CLIAgent, prompt: Option<&str>) -> String {
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
        _ => match prompt {
            Some(p) => format!("{} \"{}\"\r", agent.command_prefix(), p),
            None => format!("{}\r", agent.command_prefix()),
        },
    }
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

/// Extract reply content from terminal output between injected prompt and CCB_DONE marker.
fn extract_reply(req_id: &str, output: &str) -> String {
    let done_marker = format!("CCB_DONE:{}", req_id);
    let req_marker = format!("[CCB_REQ_ID:{}]", req_id);

    // Find the CCB_DONE marker position
    let done_pos = match output.rfind(&done_marker) {
        Some(pos) => pos,
        None => return String::new(),
    };

    // Find the injected prompt marker
    let req_pos = match output.rfind(&req_marker) {
        Some(pos) => pos,
        None => return String::new(),
    };

    if req_pos >= done_pos {
        return String::new();
    }

    // Take text between the markers
    let between = &output[req_pos..done_pos];
    let lines: Vec<&str> = between.lines().collect();

    // Skip the injected prompt lines:
    //   Line 0: [CCB_REQ_ID:xxx]
    //   Line 1: {user prompt}
    //   Line 2+: "Reply with CCB_DONE:xxx when done."
    // Use partial match since terminal may wrap or indent.
    let prompt_end_marker = format!("Reply with CCB_DONE:{}", req_id);
    let mut reply_start = 0;
    for (i, line) in lines.iter().enumerate() {
        if line.contains(&prompt_end_marker) || line.contains("when done") {
            reply_start = i + 1;
            break;
        }
        // Fallback: skip at least 3 lines (marker + prompt + reply instruction)
        if i >= 2 && reply_start == 0 {
            reply_start = i + 1;
        }
    }

    if reply_start == 0 {
        reply_start = 3.min(lines.len());
    }

    let reply_lines: Vec<&str> = lines[reply_start..].to_vec();
    let reply = reply_lines.join("\n").trim().to_string();
    reply
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_reply() {
        let output = "some earlier output\n[CCB_REQ_ID:abc123]\nSay hello\nReply with CCB_DONE:abc123 when done.\n● Hello from CCB Bus!\nCCB_DONE:abc123\n";
        let reply = extract_reply("abc123", output);
        assert_eq!(reply, "● Hello from CCB Bus!");
    }

    #[test]
    fn test_extract_reply_multiline() {
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
    fn test_build_launch_command_claude() {
        let cmd = build_launch_command(&CLIAgent::Claude, None);
        assert!(cmd.starts_with("claude --session-id "));
        assert!(cmd.contains("--dangerously-skip-permissions"));
        assert!(cmd.ends_with('\r'));
    }

    #[test]
    fn test_build_launch_command_codex_with_prompt() {
        let cmd = build_launch_command(&CLIAgent::Codex, Some("hello"));
        assert!(cmd.starts_with("codex --dangerously-bypass-approvals-and-sandbox"));
        assert!(cmd.contains("hello"));
    }

    #[test]
    fn test_build_launch_command_opencode() {
        let cmd = build_launch_command(&CLIAgent::OpenCode, None);
        assert!(cmd.starts_with("opencode"));
    }
}

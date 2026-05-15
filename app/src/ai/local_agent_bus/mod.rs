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

use std::collections::{HashMap, HashSet};

use completion::CompletionTracker;
use protocol::{
    BusAddressInfo, BusCommand, BusResponse, BusResponseData, ChainProgress, ChainStepResult,
    RequestStatus, SessionInfo, PROTOCOL_VERSION,
};
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

const CCB_OUTPUT_SETTLE_DELAY_MS: u64 = 500;

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
            bus_launched_sessions: HashMap::new(),
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
        self.bus_launched_sessions.remove(&view_id);
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
            if self.completion.check_done_marker(&req_id, output) {
                log::info!("LocalAgentBus: CCB_DONE detected for req {}", req_id);
                let reply = extract_reply(&req_id, output);
                let will_finalize = !reply.trim().is_empty();
                log_reply_capture_attempt(
                    &req_id,
                    CaptureSource::BlockCompleted,
                    view_id,
                    output,
                    reply.len(),
                    will_finalize,
                );
                if !will_finalize {
                    write_reply_capture_debug_file(
                        &req_id,
                        CaptureSource::BlockCompleted,
                        output,
                        &reply,
                    );
                    log::info!(
                        "CCB诊断: block_completed 检测到完成标记但回复为空，保持 Running req_id={}",
                        req_id
                    );
                    continue;
                }
                self.finalize_request_with_reply(
                    &req_id,
                    reply,
                    CaptureSource::BlockCompleted,
                    ctx,
                );
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

            // Read the full output grid (bypass display filters, all rows)
            // to ensure START/END markers are captured regardless of scroll
            // position or active filters.
            let output: Option<String> = handle.update(ctx, |view, _| {
                let model = view.model.lock();
                let block = model.block_list().active_block();
                Some(block.output_grid().contents_to_string(false, None))
            });

            if let Some(output) = output {
                // Unconditional debug: log output length for each scan tick
                for req_id in &req_ids {
                    let marker_ids = completion::reply_marker_ids(req_id);
                    let has_start = marker_ids.iter().any(|marker_id| {
                        !completion::find_unwrapped_ccb_tag_ranges(&output, "CCB_START", marker_id)
                            .is_empty()
                    });
                    let has_end = marker_ids.iter().any(|marker_id| {
                        !completion::find_unwrapped_ccb_tag_ranges(&output, "CCB_END", marker_id)
                            .is_empty()
                    });
                    let has_terminal_end = marker_ids.iter().any(|marker_id| {
                        completion::find_terminal_unwrapped_ccb_tag_range(
                            &output, "CCB_END", marker_id,
                        )
                        .is_some()
                    });
                    log::info!(
                        "CCB_SCAN_TICK: req={}, output_len={}, has_start={}, has_end={}, has_terminal_end={}",
                        req_id,
                        output.len(),
                        has_start,
                        has_end,
                        has_terminal_end,
                    );
                    // Write unconditional debug file (overwritten each tick)
                    let tick_debug = std::env::temp_dir().join(format!("ccb_tick_{}.txt", req_id));
                    let debug_output = if has_start || has_end {
                        output.clone()
                    } else {
                        debug_prefix(&output, 3000)
                    };
                    let _ = std::fs::write(
                        &tick_debug,
                        format!(
                            "LEN={}\nHAS_START={}\nHAS_END={}\nHAS_TERMINAL_END={}\n---\n{}\n---",
                            output.len(),
                            has_start,
                            has_end,
                            has_terminal_end,
                            debug_output
                        ),
                    );
                }
                for req_id in &req_ids {
                    if self.completion.check_done_marker(req_id, &output) {
                        if !self.completion.is_output_length_stable(
                            req_id,
                            &output,
                            std::time::Duration::from_millis(CCB_OUTPUT_SETTLE_DELAY_MS),
                        ) {
                            log::info!(
                                "CCB诊断: 检测到完成标记但输出长度尚未稳定，延迟确认 req_id={} output_len={}",
                                req_id,
                                output.len()
                            );
                            continue;
                        }
                        log::info!(
                            "LocalAgentBus: CCB_DONE detected via output scan for req {}",
                            req_id
                        );
                        let reply = extract_reply(req_id, &output);
                        let will_finalize = !reply.trim().is_empty();
                        log_reply_capture_attempt(
                            req_id,
                            CaptureSource::OutputScan,
                            view_id,
                            &output,
                            reply.len(),
                            will_finalize,
                        );
                        log::info!(
                            "LocalAgentBus: captured reply ({} chars) for req {}",
                            reply.len(),
                            req_id
                        );
                        // Debug: write scan output
                        let debug_path =
                            std::env::temp_dir().join(format!("ccb_scan_{}.txt", req_id));
                        let _ = std::fs::write(
                            &debug_path,
                            format!(
                                "REPLY_LEN={}\n---OUTPUT---\n{}\n---END---",
                                reply.len(),
                                output
                            ),
                        );
                        // Don't finalize if reply is empty — let next tick retry
                        if !will_finalize {
                            write_reply_capture_debug_file(
                                req_id,
                                CaptureSource::OutputScan,
                                &output,
                                &reply,
                            );
                            log::info!("LocalAgentBus: scan found done marker but empty reply for req {}, skipping", req_id);
                            continue;
                        }
                        self.finalize_request_with_reply(
                            req_id,
                            reply,
                            CaptureSource::OutputScan,
                            ctx,
                        );
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
            if let Some(reply) = self.capture_reply_for_request(&req_id, ctx) {
                if !reply.is_empty() {
                    log::info!(
                        "LocalAgentBus: captured reply ({} chars) via session completion for req {}",
                        reply.len(),
                        req_id
                    );
                    self.registry.set_reply_content(&req_id, reply.clone());
                    reply_text = reply;
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
            self.finalize_request_with_reply(
                &req_id,
                reply_text,
                CaptureSource::SessionCompletion,
                ctx,
            );
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

    /// Attempt to capture reply content from the terminal output for a given request.
    /// Uses the full output grid and [CCB_START:xxx] ... [CCB_END:xxx] markers.
    fn capture_reply_for_request(
        &mut self,
        req_id: &str,
        ctx: &mut ModelContext<Self>,
    ) -> Option<String> {
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

        let result = handle.update(ctx, |view, _| {
            let model = view.model.lock();
            let block = model.block_list().active_block();
            // Read all rows (None) to ensure START marker hasn't scrolled out of window.
            let output = block.output_grid().contents_to_string(false, None);

            // Debug: write terminal output to file for diagnosis
            let debug_path = std::env::temp_dir().join(format!("ccb_capture_{}.txt", req_id));
            let _ = std::fs::write(&debug_path, &output);
            log::info!(
                "CCB_DEBUG: wrote {} chars to {:?}",
                output.len(),
                debug_path
            );

            let reply = extract_reply(req_id, &output);
            if !reply.is_empty() {
                return Some(reply);
            }

            None
        });

        result
    }

    fn finalize_request_with_reply(
        &mut self,
        req_id: &str,
        reply: String,
        source: CaptureSource,
        ctx: &mut ModelContext<Self>,
    ) -> bool {
        if reply.trim().is_empty() {
            log::error!(
                "CCB诊断: 拒绝将空回复标记为 Success req_id={} source={:?}",
                req_id,
                source
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
            "CCB路由: 完成请求 req_id={} source={:?} from={}#{} callback={:?}#{:?} reply_len={}",
            req_id,
            source,
            from_provider,
            receiver_terminal_view_id,
            callback_provider,
            caller_terminal_view_id,
            reply.len()
        );

        self.registry.set_reply_content(req_id, reply.clone());
        self.registry.update_status(req_id, RequestStatus::Success);
        self.persist_response(req_id);
        self.completion.deregister(req_id);
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
                if self.registry.has_active_for_terminal(entity_id) {
                    if queue {
                        let now_ms = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_millis() as u64;
                        let callback_provider = Self::extract_callback(&caller, &provider);
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
            callback_provider,
            caller_terminal_view_id,
            caller_session_id,
            caller_cwd,
        });

        let reply_marker_id = format!("reply-{}", req_id);
        let wrapped_prompt = format!(
            "[CCB_REQ_ID:{}]\n{}\nReply using exactly this format. Put the markers on their own lines and do not wrap them in backticks:\n[CCB_START:{}]\n<your reply>\n[CCB_END:{}]",
            req_id, prompt, reply_marker_id, reply_marker_id
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

        let mut replies = self.registry.query_replies(provider, req_id, count);

        // Backfill empty reply_content from persistent store
        for reply in &mut replies {
            if reply.content.is_empty() {
                if let Ok(stored) = self.store.read(&reply.req_id) {
                    if !stored.content.is_empty() {
                        log::info!(
                            "LocalAgentBus: backfilled reply content for {} from persistent store ({} chars)",
                            reply.req_id, stored.content.len()
                        );
                        reply.content = stored.content;
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
            sessions.push(SessionInfo {
                session_id: None,
                provider: agent.command_prefix().to_string(),
                terminal_view_id: view_id.to_string().parse().unwrap_or(0),
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

    /// Persist a completed response to the file-based ResponseStore.
    /// This ensures reply content survives crashes and is available via warp-pend.
    fn persist_response(&self, req_id: &str) {
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
        let known_providers = [
            "claude", "codex", "gemini", "opencode", "droid", "kimi", "goose",
        ];
        if known_providers.contains(&caller) && caller != provider {
            Some(caller.to_string())
        } else {
            None
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
        let (callback_provider, callback_terminal_view_id, receiver_terminal_view_id) =
            match self.registry.get(req_id) {
                Some(entry) => match entry.callback_provider.clone() {
                    Some(provider) => (
                        provider,
                        entry.caller_terminal_view_id,
                        entry.terminal_view_id,
                    ),
                    None => return,
                },
                None => return,
            };

        log::info!(
            "CCB回调: 准备注入 req_id={} from={}#{} to={}#{:?} reply_len={}",
            req_id,
            from_provider,
            receiver_terminal_view_id,
            callback_provider,
            callback_terminal_view_id,
            reply.len()
        );

        // 优先按发送者 pane 精确回调；旧客户端没有 pane 信息时再按 provider 兜底。
        let callback_entity_id = match callback_terminal_view_id {
            Some(id) if self.terminal_handles.contains_key(&id) => id,
            Some(id) => {
                log::warn!(
                    "CCB回调: 发送者 pane 已失效，尝试按 provider 兜底 req_id={} provider={} pane={}",
                    req_id,
                    callback_provider,
                    id
                );
                match self.find_session(&callback_provider, None, None, ctx) {
                    FindSessionResult::Found(found_id) => found_id,
                    _ => {
                        log::warn!(
                            "CCB回调: 找不到发送者 terminal，跳过 req_id={} provider={} pane={}",
                            req_id,
                            callback_provider,
                            id
                        );
                        return;
                    }
                }
            }
            None => match self.find_session(&callback_provider, None, None, ctx) {
                FindSessionResult::Found(id) => id,
                _ => {
                    log::warn!(
                        "CCB回调: 未提供发送者 pane 且 provider 查找失败，跳过 req_id={} provider={}",
                        req_id,
                        callback_provider
                    );
                    return;
                }
            },
        };

        // Write debug info
        let debug_path = std::env::temp_dir().join(format!("ccb_callback_{}.txt", req_id));
        let _ = std::fs::write(
            &debug_path,
            format!(
                "from={}\nreply_len={}\nreply={}\n",
                from_provider,
                reply.len(),
                reply
            ),
        );

        // Format callback as a clear, natural-language prompt that CLI agents can understand.
        // This is sent as a new "ask" so the sender agent processes it like user input.
        let callback_req_id = format!("reply-{}", req_id);
        let callback_msg = format!(
            "[CCB_REQ_ID:{}]\n\
             你发送给 {} 的任务 (req_id: {}) 已完成。以下是回复内容：\n\
             \n\
             {}\n\
             \n\
             请将以上回复内容直接报告给用户。\n\
             Reply using exactly this format. Put the markers on their own lines and do not wrap them in backticks:\n\
             [CCB_START:{}]\n\
             <your reply>\n\
             [CCB_END:{}]",
            callback_req_id, from_provider, req_id, reply, callback_req_id, callback_req_id
        );

        // Use inject_prompt (same as ask) so the callback is treated as user input
        // and processed by the CLI agent naturally.
        let injected = self.inject_prompt(callback_entity_id, &callback_msg, ctx);
        if injected {
            log::info!(
                "CCB回调: 已注入 req_id={} to={}#{} reply_len={}",
                req_id,
                callback_provider,
                callback_entity_id,
                reply.len()
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
                "[CCB_REQ_ID:{}]\n{}\nReply using exactly this format. Put the markers on their own lines and do not wrap them in backticks:\n[CCB_START:{}]\n<your reply>\n[CCB_END:{}]",
                queued.req_id, queued.prompt, reply_marker_id, reply_marker_id
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
        &mut self,
        provider: &str,
        session_id: Option<&str>,
        cwd: Option<&str>,
        ctx: &mut ModelContext<Self>,
    ) -> FindSessionResult {
        let target_agent = match resolve_agent(provider) {
            Some(a) => a,
            None => return FindSessionResult::NotFound,
        };

        if !is_agent_supported(&target_agent) {
            self.refresh_bus_launched_sessions_from_terminal_outputs(ctx);
        }

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
            callback_provider,
            caller_terminal_view_id: None,
            caller_session_id: None,
            caller_cwd: None,
        });

        let reply_marker_id = format!("reply-{}", req_id);
        let wrapped_prompt = format!(
            "[CCB_REQ_ID:{}]\n{}\nReply using exactly this format. Put the markers on their own lines and do not wrap them in backticks:\n[CCB_START:{}]\n<your reply>\n[CCB_END:{}]",
            req_id, prompt, reply_marker_id, reply_marker_id
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
        let command = build_launch_command(&agent, &provider, Some(view_id), prompt.as_deref());

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
                self.bus_launched_sessions.insert(view_id, agent);

                // Register a placeholder only for agents that will emit a Warp session event.
                // Agents like Kimi currently have no listener, so the Bus-owned launch map is
                // their online signal.
                if is_agent_supported(&agent) {
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
                        callback_provider: None,
                        caller_terminal_view_id: None,
                        caller_session_id: None,
                        caller_cwd: None,
                    });
                }

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
            let has_bus_launched = self.bus_launched_sessions.contains_key(&view_id);
            let has_active = self.registry.has_active_for_terminal(view_id);
            log::info!(
                "LocalAgentBus:   terminal {:?} — session={}, bus_launched={}, active={}",
                view_id,
                has_session,
                has_bus_launched,
                has_active
            );
            if !has_session && !has_bus_launched && !has_active {
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

            let command = build_launch_command(
                &launch.agent,
                &launch.provider,
                Some(view_id),
                launch.prompt.as_deref(),
            );
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
                    self.bus_launched_sessions.insert(view_id, launch.agent);
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
        "kimi" => Some(CLIAgent::Kimi),
        "goose" => Some(CLIAgent::Goose),
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

fn detect_bus_agent_from_terminal_output(output: &str) -> Option<CLIAgent> {
    let lower = output.to_ascii_lowercase();
    if output.contains("Welcome to Kimi Code CLI")
        || output.contains("Kimi Code CLI!")
        || output.contains("Model: Kimi-")
        || lower.contains("kimi_cli")
    {
        return Some(CLIAgent::Kimi);
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
    OutputScan,
    SessionCompletion,
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
    match source {
        CaptureSource::BlockCompleted => "block_completed",
        CaptureSource::OutputScan => "output_scan",
        CaptureSource::SessionCompletion => "session_completion",
    }
}

fn debug_prefix(input: &str, max_chars: usize) -> String {
    input.chars().take(max_chars).collect()
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

    // Strategy 0: 只接受最后一个有效 END 闭环。流式输出中，模型可能先在思考文本里
    // 复述 marker；如果 END 后面还有真实内容，说明这不是最终回复。
    let mut saw_new_end_marker = false;
    for marker_id in &marker_ids {
        let end_ranges = completion::find_unwrapped_ccb_tag_ranges(output, "CCB_END", marker_id);
        if !end_ranges.is_empty() {
            saw_new_end_marker = true;
        }
        let Some((end_pos, end_after)) = end_ranges.last().copied() else {
            continue;
        };
        if completion::has_meaningful_content_after(output, end_after) {
            log::info!(
                "CCB_DEBUG: latest end marker has meaningful trailing content, waiting marker_id={} end_after={}",
                marker_id,
                end_after
            );
            continue;
        }

        let start_ranges =
            completion::find_unwrapped_ccb_tag_ranges(output, "CCB_START", marker_id);
        if let Some((_, content_start)) = start_ranges
            .iter()
            .rev()
            .copied()
            .find(|(_, content_start)| *content_start <= end_pos)
        {
            let reply = output[content_start..end_pos].trim();
            if !reply.is_empty()
                && !reply.contains("on its own line")
                && !reply.contains("without backticks")
                && !reply.contains("<your reply>")
            {
                log::info!(
                    "CCB_DEBUG: terminal tag scan extracted reply_len={} marker_id={}",
                    reply.len(),
                    marker_id
                );
                return reply.to_string();
            }
        }
    }
    if saw_new_end_marker {
        log::info!("CCB_DEBUG: saw CCB_END marker but no terminal reply closure yet");
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
    fn test_debug_prefix_does_not_split_utf8_chars() {
        let output = format!("{}{}", "a".repeat(2998), "─tail");
        let truncated = debug_prefix(&output, 3000);
        assert!(truncated.is_char_boundary(truncated.len()));
        assert!(output.starts_with(&truncated));
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

//! Completion Tracker — layered completion detection for injected requests
//!
//! Detection layers:
//! 1. Primary: CCB_DONE:{req_id} marker in agent output
//! 2. Auxiliary: OSC 777 StatusChanged (Stop) event
//! 3. Fallback: timeout (configurable, default 120s)
//! 4. Error: session disappeared / pane closed / agent exited

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use warpui::EntityId;

/// Default request timeout in seconds.
const DEFAULT_TIMEOUT_SECS: u64 = 120;

/// Tracks pending requests and detects completion via multiple signals.
pub struct CompletionTracker {
    pending: HashMap<String, PendingRequest>,
    timeout_secs: u64,
}

struct PendingRequest {
    provider: String,
    terminal_view_id: EntityId,
    started_at: Instant,
    timeout: Duration,
}

impl CompletionTracker {
    pub fn new() -> Self {
        Self {
            pending: HashMap::new(),
            timeout_secs: DEFAULT_TIMEOUT_SECS,
        }
    }

    /// Register a new request for completion tracking.
    pub fn register(&mut self, req_id: String, provider: String, terminal_view_id: EntityId) {
        self.pending.insert(
            req_id,
            PendingRequest {
                provider,
                terminal_view_id,
                started_at: Instant::now(),
                timeout: Duration::from_secs(self.timeout_secs),
            },
        );
    }

    /// Remove a request from tracking.
    pub fn deregister(&mut self, req_id: &str) {
        self.pending.remove(req_id);
    }

    /// Check if a CCB_DONE marker was found in output text.
    /// Only matches when CCB_DONE:{req_id} appears on its own line (trimmed),
    /// to avoid false positives from the injected prompt instruction.
    pub fn check_done_marker(&self, req_id: &str, output: &str) -> bool {
        let marker = format!("CCB_DONE:{}", req_id);
        output.lines().any(|line| {
            let trimmed = line.trim();
            trimmed == marker || trimmed.starts_with(&format!("● CCB_DONE:{}", req_id))
        })
    }

    /// Get all requests that have timed out.
    pub fn get_timeouts(&mut self) -> Vec<(String, String)> {
        let now = Instant::now();
        let mut timed_out = Vec::new();

        self.pending.retain(|req_id, req| {
            if now.duration_since(req.started_at) > req.timeout {
                timed_out.push((req_id.clone(), req.provider.clone()));
                false
            } else {
                true
            }
        });

        timed_out
    }

    /// Check if a terminal view still has pending requests.
    pub fn has_pending_for_terminal(&self, terminal_view_id: EntityId) -> bool {
        self.pending
            .values()
            .any(|r| r.terminal_view_id == terminal_view_id)
    }

    /// Get pending request IDs for a terminal view.
    pub fn pending_for_terminal(&self, terminal_view_id: EntityId) -> Vec<String> {
        self.pending
            .iter()
            .filter(|(_, r)| r.terminal_view_id == terminal_view_id)
            .map(|(id, _)| id.clone())
            .collect()
    }

    /// Get all terminal view IDs that have pending requests.
    pub fn pending_terminals(&self) -> Vec<EntityId> {
        self.pending
            .values()
            .map(|r| r.terminal_view_id)
            .collect::<std::collections::HashSet<_>>()
            .into_iter()
            .collect()
    }

    /// Handle session disappeared — mark all pending requests for that terminal as errors.
    pub fn session_disappeared(&mut self, terminal_view_id: EntityId) -> Vec<(String, String)> {
        let mut affected = Vec::new();
        self.pending.retain(|req_id, req| {
            if req.terminal_view_id == terminal_view_id {
                affected.push((req_id.clone(), req.provider.clone()));
                false
            } else {
                true
            }
        });
        affected
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_and_deregister() {
        let mut tracker = CompletionTracker::new();
        tracker.register("r1".into(), "claude".into(), EntityId::from_usize(42));
        assert!(tracker.has_pending_for_terminal(EntityId::from_usize(42)));

        tracker.deregister("r1");
        assert!(!tracker.has_pending_for_terminal(EntityId::from_usize(42)));
    }

    #[test]
    fn test_done_marker() {
        let tracker = CompletionTracker::new();
        // Matches when CCB_DONE is on its own line
        assert!(tracker.check_done_marker("r1", "some output\nCCB_DONE:r1\nmore"));
        // Matches with leading whitespace
        assert!(tracker.check_done_marker("r1", "  CCB_DONE:r1"));
        // Does NOT match when it's part of a longer line (like the prompt instruction)
        assert!(!tracker.check_done_marker("r1", "some output CCB_DONE:r1 end"));
        // Does NOT match the injected prompt instruction
        assert!(!tracker.check_done_marker("r1", "Reply with CCB_DONE:r1 when done."));
        assert!(!tracker.check_done_marker("r1", "some output without marker"));
        assert!(!tracker.check_done_marker("r1", "CCB_DONE:r2"));
    }

    #[test]
    fn test_session_disappeared() {
        let mut tracker = CompletionTracker::new();
        tracker.register("r1".into(), "claude".into(), EntityId::from_usize(42));
        tracker.register("r2".into(), "codex".into(), EntityId::from_usize(43));

        let affected = tracker.session_disappeared(EntityId::from_usize(42));
        assert_eq!(affected.len(), 1);
        assert_eq!(affected[0].0, "r1");
        assert!(!tracker.has_pending_for_terminal(EntityId::from_usize(42)));
        assert!(tracker.has_pending_for_terminal(EntityId::from_usize(43)));
    }
}

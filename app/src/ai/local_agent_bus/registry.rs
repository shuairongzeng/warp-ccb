//! Request Registry — in-memory tracking of bus request lifecycle

use std::collections::HashMap;

use warpui::EntityId;

use crate::ai::local_agent_bus::protocol::{ReplyEntry, RequestStatus};

/// A single request entry in the registry.
#[derive(Debug, Clone)]
pub struct RequestEntry {
    pub req_id: String,
    pub provider: String,
    pub caller: String,
    pub terminal_view_id: EntityId,
    pub session_id: Option<String>,
    pub cwd: Option<String>,
    pub status: RequestStatus,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub error_message: Option<String>,
    pub reply_content: Option<String>,
}

/// Thread-safe (single-threaded access on Warp main thread) request registry.
#[derive(Debug)]
pub struct RequestRegistry {
    entries: HashMap<String, RequestEntry>,
    /// Index: terminal_view_id -> active req_id (if any)
    active_by_terminal: HashMap<EntityId, String>,
}

impl RequestRegistry {
    pub fn new() -> Self {
        Self {
            entries: HashMap::new(),
            active_by_terminal: HashMap::new(),
        }
    }

    /// Insert a new request entry.
    pub fn insert(&mut self, entry: RequestEntry) {
        if entry.status == RequestStatus::Running || entry.status == RequestStatus::Injecting {
            self.active_by_terminal
                .insert(entry.terminal_view_id, entry.req_id.clone());
        }
        self.entries.insert(entry.req_id.clone(), entry);
    }

    /// Get a request by req_id.
    pub fn get(&self, req_id: &str) -> Option<&RequestEntry> {
        self.entries.get(req_id)
    }

    /// Update the status of a request. Returns false if not found.
    pub fn update_status(&mut self, req_id: &str, new_status: RequestStatus) -> bool {
        if let Some(entry) = self.entries.get_mut(req_id) {
            let old_status = entry.status;
            entry.status = new_status;
            entry.updated_at_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;

            // Update active index
            match new_status {
                RequestStatus::Running | RequestStatus::Injecting => {
                    self.active_by_terminal
                        .insert(entry.terminal_view_id, req_id.to_string());
                }
                _ => {
                    // Terminal state: clear active index if it points to this request
                    if self.active_by_terminal.get(&entry.terminal_view_id)
                        == Some(&req_id.to_string())
                    {
                        self.active_by_terminal.remove(&entry.terminal_view_id);
                    }
                }
            }

            let _ = old_status; // could emit event
            true
        } else {
            false
        }
    }

    /// Check if a terminal has an active (non-terminal) request.
    pub fn has_active_for_terminal(&self, terminal_view_id: EntityId) -> bool {
        self.active_by_terminal
            .get(&terminal_view_id)
            .and_then(|req_id| self.entries.get(req_id))
            .map(|e| {
                !matches!(
                    e.status,
                    RequestStatus::Success
                        | RequestStatus::Error
                        | RequestStatus::Timeout
                        | RequestStatus::Cancelled
                        | RequestStatus::SessionBusy
                )
            })
            .unwrap_or(false)
    }

    /// Iterate over all entries (view IDs are EntityId).
    pub fn iter(&self) -> impl Iterator<Item = (&String, &RequestEntry)> {
        self.entries.iter()
    }

    /// Remove an entry by req_id. Also clears the active index if needed.
    pub fn remove(&mut self, req_id: &str) -> Option<RequestEntry> {
        let entry = self.entries.remove(req_id)?;
        if self.active_by_terminal.get(&entry.terminal_view_id) == Some(&req_id.to_string()) {
            self.active_by_terminal.remove(&entry.terminal_view_id);
        }
        Some(entry)
    }

    /// Store captured reply content for a request.
    pub fn set_reply_content(&mut self, req_id: &str, content: String) {
        if let Some(entry) = self.entries.get_mut(req_id) {
            entry.reply_content = Some(content);
        }
    }

    /// Query replies matching the given criteria.
    pub fn query_replies(
        &self,
        provider: &str,
        req_id: Option<&str>,
        count: usize,
    ) -> Vec<ReplyEntry> {
        let mut entries: Vec<&RequestEntry> = self
            .entries
            .values()
            .filter(|e| {
                if e.provider != provider {
                    return false;
                }
                if let Some(rid) = req_id {
                    if e.req_id != rid {
                        return false;
                    }
                }
                true
            })
            .collect();

        // Sort by created_at descending
        entries.sort_by(|a, b| b.created_at_ms.cmp(&a.created_at_ms));
        entries.truncate(count);

        entries
            .into_iter()
            .map(|e| ReplyEntry {
                req_id: e.req_id.clone(),
                provider: e.provider.clone(),
                content: e
                    .reply_content
                    .clone()
                    .or_else(|| e.error_message.clone())
                    .unwrap_or_default(),
                timestamp_ms: e.updated_at_ms,
                status: e.status,
            })
            .collect()
    }

    /// Clean up entries older than the given TTL (in seconds).
    pub fn cleanup_old(&mut self, ttl_secs: u64) {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff = now_ms.saturating_sub(ttl_secs * 1000);

        self.entries.retain(|_, entry| {
            if entry.created_at_ms < cutoff {
                // Also clean active index
                if self.active_by_terminal.get(&entry.terminal_view_id) == Some(&entry.req_id) {
                    self.active_by_terminal.remove(&entry.terminal_view_id);
                }
                false
            } else {
                true
            }
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_entry(
        req_id: &str,
        provider: &str,
        tv_id: EntityId,
        status: RequestStatus,
    ) -> RequestEntry {
        RequestEntry {
            req_id: req_id.to_string(),
            provider: provider.to_string(),
            caller: "test".to_string(),
            terminal_view_id: tv_id,
            session_id: None,
            cwd: None,
            status,
            created_at_ms: 1000,
            updated_at_ms: 1000,
            error_message: None,
            reply_content: None,
        }
    }

    #[test]
    fn test_insert_and_get() {
        let mut reg = RequestRegistry::new();
        reg.insert(make_entry(
            "r1",
            "claude",
            EntityId::from_usize(1),
            RequestStatus::Running,
        ));
        assert!(reg.get("r1").is_some());
        assert!(reg.get("r2").is_none());
    }

    #[test]
    fn test_update_status() {
        let mut reg = RequestRegistry::new();
        reg.insert(make_entry(
            "r1",
            "claude",
            EntityId::from_usize(1),
            RequestStatus::Running,
        ));
        assert!(reg.update_status("r1", RequestStatus::Success));
        assert_eq!(reg.get("r1").unwrap().status, RequestStatus::Success);
        assert!(!reg.has_active_for_terminal(EntityId::from_usize(1)));
    }

    #[test]
    fn test_has_active() {
        let mut reg = RequestRegistry::new();
        reg.insert(make_entry(
            "r1",
            "claude",
            EntityId::from_usize(1),
            RequestStatus::Running,
        ));
        assert!(reg.has_active_for_terminal(EntityId::from_usize(1)));
        assert!(!reg.has_active_for_terminal(EntityId::from_usize(2)));

        reg.update_status("r1", RequestStatus::Success);
        assert!(!reg.has_active_for_terminal(EntityId::from_usize(1)));
    }

    #[test]
    fn test_query_replies() {
        let mut reg = RequestRegistry::new();
        reg.insert(make_entry(
            "r1",
            "claude",
            EntityId::from_usize(1),
            RequestStatus::Success,
        ));
        reg.insert(make_entry(
            "r2",
            "codex",
            EntityId::from_usize(2),
            RequestStatus::Success,
        ));
        reg.insert(make_entry(
            "r3",
            "claude",
            EntityId::from_usize(3),
            RequestStatus::Running,
        ));

        let replies = reg.query_replies("claude", None, 10);
        assert_eq!(replies.len(), 2);

        let replies = reg.query_replies("claude", Some("r1"), 10);
        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].req_id, "r1");
    }

    #[test]
    fn test_session_busy_rejection() {
        let mut reg = RequestRegistry::new();
        reg.insert(make_entry(
            "r1",
            "claude",
            EntityId::from_usize(1),
            RequestStatus::Running,
        ));
        assert!(reg.has_active_for_terminal(EntityId::from_usize(1)));

        // Trying to insert another request for the same terminal should be caught
        // by the caller checking has_active_for_terminal
    }

    #[test]
    fn test_set_reply_content() {
        let mut reg = RequestRegistry::new();
        reg.insert(make_entry(
            "r1",
            "claude",
            EntityId::from_usize(1),
            RequestStatus::Success,
        ));
        reg.set_reply_content("r1", "Hello world".to_string());
        assert_eq!(
            reg.get("r1").unwrap().reply_content,
            Some("Hello world".to_string())
        );

        let replies = reg.query_replies("claude", None, 10);
        assert_eq!(replies[0].content, "Hello world");
    }
}

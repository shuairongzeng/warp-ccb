//! Shared Memory Layer — Protocol Types
//!
//! Data models for cross-agent shared memory entries.

use serde::{Deserialize, Serialize};

/// Memory scope determines visibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryScope {
    /// Visible to any agent connected to the same LocalAgentBus.
    Global,
    /// Visible to agents sharing the same project_id (same working directory).
    Project,
    /// Visible only to the owner agent.
    Private,
}

impl std::fmt::Display for MemoryScope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MemoryScope::Global => write!(f, "global"),
            MemoryScope::Project => write!(f, "project"),
            MemoryScope::Private => write!(f, "private"),
        }
    }
}

/// A fully materialized memory entry as stored in SQLite.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryEntry {
    pub id: String,
    pub scope: MemoryScope,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    pub owner: String,
    pub key: String,
    pub content: String,
    #[serde(default = "default_content_type")]
    pub content_type: String,
    pub version: u32,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u32>,
    #[serde(default)]
    pub access_count: u32,
    pub last_accessed_ms: u64,
    #[serde(default)]
    pub size_bytes: u32,
    #[serde(default)]
    pub compressed: bool,
    #[serde(default)]
    pub tags: Vec<String>,
}

fn default_content_type() -> String {
    "text".to_string()
}

/// Input type for writing a new or updated entry.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryEntryInput {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    pub scope: MemoryScope,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    pub key: String,
    pub content: String,
    #[serde(default = "default_content_type")]
    pub content_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expected_version: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ttl_secs: Option<u32>,
    #[serde(default)]
    pub tags: Vec<String>,
}

/// Filter for querying entries.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MemoryFilter {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scope: Option<MemoryScope>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub owner: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_prefix: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tag: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_updated_at_ms: Option<u64>,
}

/// Result of a successful write.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryWriteResult {
    pub id: String,
    pub version: u32,
    pub created: bool, // true = insert, false = update
}

/// A conflict response when optimistic locking fails.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryConflict {
    pub stored_version: u32,
    pub stored_entry: MemoryEntry,
}

/// Project metadata returned by list-projects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryProjectInfo {
    pub project_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub friendly_name: Option<String>,
    pub created_at_ms: u64,
    pub last_active_ms: u64,
    pub entry_count: u64,
}

/// Quota status for a project.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryQuotaStatus {
    pub project_id: String,
    pub used_bytes: u64,
    pub limit_bytes: u64,
    pub compression_savings_bytes: u64,
}

// ---------------------------------------------------------------------------
// Bus response data extensions (merged into local_agent_bus/protocol.rs)
// ---------------------------------------------------------------------------

/// Response payloads for memory commands.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MemoryBusResponseData {
    MemoryWriteResult(MemoryWriteResult),
    MemoryReadResult { entry: Option<MemoryEntry> },
    MemoryQueryResult { entries: Vec<MemoryEntry>, total: usize },
    MemoryDeleted { id: String },
    MemoryConflict(MemoryConflict),
    MemoryProjectList { projects: Vec<MemoryProjectInfo> },
    MemoryQuotaStatus(MemoryQuotaStatus),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_scope_display() {
        assert_eq!(MemoryScope::Project.to_string(), "project");
    }

    #[test]
    fn test_memory_entry_serde() {
        let entry = MemoryEntry {
            id: "uuid-1".to_string(),
            scope: MemoryScope::Project,
            project_id: Some("abc123".to_string()),
            owner: "claude".to_string(),
            key: "ctx.rust-refactor".to_string(),
            content: "{\"files\":[\"main.rs\"]}".to_string(),
            content_type: "json".to_string(),
            version: 1,
            created_at_ms: 1000,
            updated_at_ms: 2000,
            ttl_secs: Some(3600),
            access_count: 5,
            last_accessed_ms: 2000,
            size_bytes: 28,
            compressed: false,
            tags: vec!["rust".to_string()],
        };
        let json = serde_json::to_string(&entry).unwrap();
        assert!(json.contains("\"scope\":\"project\""));
        let de: MemoryEntry = serde_json::from_str(&json).unwrap();
        assert_eq!(de.key, "ctx.rust-refactor");
    }
}

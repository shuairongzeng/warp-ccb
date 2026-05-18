//! Response Store — file-based persistence for request state and responses

use std::path::PathBuf;

use crate::ai::local_agent_bus::protocol::RequestStatus;

/// Persistent response store for crash recovery and cross-process access.
pub struct ResponseStore {
    base_dir: PathBuf,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredResponse {
    pub req_id: String,
    pub provider: String,
    pub status: RequestStatus,
    pub content: String,
    pub created_at_ms: u64,
    pub updated_at_ms: u64,
    pub schema_version: u32,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub warnings: Vec<String>,
    #[serde(default)]
    pub raw_len: Option<usize>,
    #[serde(default)]
    pub filtered_len: Option<usize>,
    #[serde(default)]
    pub truncated: Option<bool>,
    #[serde(default)]
    pub finalized_at_ms: Option<u64>,
}

impl StoredResponse {
    pub const SCHEMA_VERSION: u32 = 2;
}

impl ResponseStore {
    pub fn new(base_dir: PathBuf) -> Self {
        Self { base_dir }
    }

    /// Write a response file atomically (temp file + rename).
    pub fn write(&self, response: &StoredResponse) -> std::io::Result<()> {
        std::fs::create_dir_all(&self.base_dir)?;

        let filename = format!("{}.json", response.req_id);
        let target = self.base_dir.join(&filename);
        let temp = self.base_dir.join(format!("{}.tmp", response.req_id));

        let mut response = response.clone();
        response.schema_version = StoredResponse::SCHEMA_VERSION;
        let json = serde_json::to_string_pretty(&response)?;
        std::fs::write(&temp, json)?;
        std::fs::rename(&temp, &target)?;

        Ok(())
    }

    /// Read a response file by req_id.
    pub fn read(&self, req_id: &str) -> std::io::Result<StoredResponse> {
        let path = self.base_dir.join(format!("{}.json", req_id));
        let content = std::fs::read_to_string(path)?;
        serde_json::from_str(&content)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }

    /// List all response files matching a provider prefix.
    pub fn list_by_provider(&self, provider: &str) -> Vec<StoredResponse> {
        let Ok(entries) = std::fs::read_dir(&self.base_dir) else {
            return vec![];
        };

        entries
            .filter_map(|e| e.ok())
            .filter(|e| {
                e.path()
                    .extension()
                    .map(|ext| ext == "json")
                    .unwrap_or(false)
            })
            .filter_map(|e| {
                let content = std::fs::read_to_string(e.path()).ok()?;
                let resp: StoredResponse = serde_json::from_str(&content).ok()?;
                if resp.provider == provider {
                    Some(resp)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Clean up files older than TTL seconds.
    pub fn cleanup_old(&self, ttl_secs: u64) -> std::io::Result<()> {
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        let cutoff = now_ms.saturating_sub(ttl_secs * 1000);

        let entries = std::fs::read_dir(&self.base_dir)?;
        for entry in entries.filter_map(|e| e.ok()) {
            let path = entry.path();
            if path.extension().map(|ext| ext == "json").unwrap_or(false) {
                if let Ok(content) = std::fs::read_to_string(&path) {
                    if let Ok(resp) = serde_json::from_str::<StoredResponse>(&content) {
                        if resp.updated_at_ms < cutoff {
                            let _ = std::fs::remove_file(path);
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_write_and_read() {
        let dir = std::env::temp_dir().join("warp-ccb-test-store");
        let _ = std::fs::remove_dir_all(&dir);
        let store = ResponseStore::new(dir.clone());

        let resp = StoredResponse {
            req_id: "test-req-1".to_string(),
            provider: "claude".to_string(),
            status: RequestStatus::Success,
            content: "hello world".to_string(),
            created_at_ms: 1000,
            updated_at_ms: 2000,
            schema_version: StoredResponse::SCHEMA_VERSION,
            source: None,
            confidence: None,
            warnings: Vec::new(),
            raw_len: None,
            filtered_len: None,
            truncated: None,
            finalized_at_ms: None,
        };

        store.write(&resp).unwrap();
        let loaded = store.read("test-req-1").unwrap();
        assert_eq!(loaded.req_id, "test-req-1");
        assert_eq!(loaded.provider, "claude");
        assert_eq!(loaded.status, RequestStatus::Success);
        assert_eq!(loaded.content, "hello world");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_v2_metadata_round_trip() {
        let dir = std::env::temp_dir().join("warp-ccb-test-store-v2");
        let _ = std::fs::remove_dir_all(&dir);
        let store = ResponseStore::new(dir.clone());

        let resp = StoredResponse {
            req_id: "test-req-v2".to_string(),
            provider: "kimi".to_string(),
            status: RequestStatus::Success,
            content: "hello with metadata".to_string(),
            created_at_ms: 1000,
            updated_at_ms: 2000,
            schema_version: StoredResponse::SCHEMA_VERSION,
            source: Some("passive_raw_output".to_string()),
            confidence: Some(0.7),
            warnings: vec!["filtered_instruction_echo".to_string()],
            raw_len: Some(2048),
            filtered_len: Some(128),
            truncated: Some(false),
            finalized_at_ms: Some(2500),
        };

        store.write(&resp).unwrap();
        let loaded = store.read("test-req-v2").unwrap();

        assert_eq!(loaded.schema_version, 2);
        assert_eq!(loaded.source.as_deref(), Some("passive_raw_output"));
        assert_eq!(loaded.confidence, Some(0.7));
        assert_eq!(loaded.warnings, vec!["filtered_instruction_echo"]);
        assert_eq!(loaded.raw_len, Some(2048));
        assert_eq!(loaded.filtered_len, Some(128));
        assert_eq!(loaded.truncated, Some(false));
        assert_eq!(loaded.finalized_at_ms, Some(2500));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_v1_response_defaults_metadata() {
        let dir = std::env::temp_dir().join("warp-ccb-test-store-v1");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = ResponseStore::new(dir.clone());
        std::fs::write(
            dir.join("legacy-req.json"),
            r#"{
  "req_id": "legacy-req",
  "provider": "codex",
  "status": "success",
  "content": "legacy content",
  "created_at_ms": 1000,
  "updated_at_ms": 2000,
  "schema_version": 1
}"#,
        )
        .unwrap();

        let loaded = store.read("legacy-req").unwrap();

        assert_eq!(loaded.source, None);
        assert_eq!(loaded.confidence, None);
        assert!(loaded.warnings.is_empty());
        assert_eq!(loaded.raw_len, None);
        assert_eq!(loaded.filtered_len, None);
        assert_eq!(loaded.truncated, None);
        assert_eq!(loaded.finalized_at_ms, None);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn test_list_by_provider() {
        let dir = std::env::temp_dir().join("warp-ccb-test-list");
        let _ = std::fs::remove_dir_all(&dir);
        let store = ResponseStore::new(dir.clone());

        for (i, provider) in ["claude", "codex", "claude"].iter().enumerate() {
            store
                .write(&StoredResponse {
                    req_id: format!("req-{}", i),
                    provider: provider.to_string(),
                    status: RequestStatus::Success,
                    content: format!("response {}", i),
                    created_at_ms: 1000 + i as u64,
                    updated_at_ms: 2000 + i as u64,
                    schema_version: StoredResponse::SCHEMA_VERSION,
                    source: None,
                    confidence: None,
                    warnings: Vec::new(),
                    raw_len: None,
                    filtered_len: None,
                    truncated: None,
                    finalized_at_ms: None,
                })
                .unwrap();
        }

        let claude_responses = store.list_by_provider("claude");
        assert_eq!(claude_responses.len(), 2);

        let codex_responses = store.list_by_provider("codex");
        assert_eq!(codex_responses.len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }
}

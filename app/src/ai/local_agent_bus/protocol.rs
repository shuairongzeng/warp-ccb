//! LocalAgentBus Protocol Types
//!
//! JSONL-based request/response protocol for CCB-Warp communication.
//! Each request is a single JSON line sent over Unix socket or Windows named pipe.

use serde::{Deserialize, Serialize};

/// Protocol version
pub const PROTOCOL_VERSION: u32 = 1;

// ---------------------------------------------------------------------------
// Request
// ---------------------------------------------------------------------------

/// Top-level request envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusRequest {
    /// Protocol version (currently 1)
    pub v: u32,
    /// Authentication token (must match bus-address.json)
    pub token: String,
    /// The command to execute
    #[serde(flatten)]
    pub command: BusCommand,
}

/// Supported bus commands.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BusCommand {
    Ask {
        provider: String,
        prompt: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        req_id: String,
        #[serde(default = "default_caller")]
        caller: String,
        #[serde(default)]
        queue: bool,
    },
    Pend {
        provider: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        #[serde(default = "default_count")]
        count: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        req_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        chain_id: Option<String>,
    },
    Ping {
        provider: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
    },
    ListSessions {
        #[serde(skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
    },
    Cancel {
        req_id: String,
    },
    Launch {
        provider: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        prompt: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
    },
    Chain {
        steps: Vec<ChainStep>,
        #[serde(skip_serializing_if = "Option::is_none")]
        caller: Option<String>,
    },
    Wait {
        #[serde(skip_serializing_if = "Option::is_none")]
        req_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        timeout_ms: Option<u64>,
    },
}

fn default_caller() -> String {
    "unknown".to_string()
}

fn default_count() -> usize {
    1
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

/// Top-level response envelope.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusResponse {
    pub ok: bool,
    #[serde(flatten)]
    pub data: BusResponseData,
}

impl BusResponse {
    pub fn ok(data: BusResponseData) -> Self {
        Self { ok: true, data }
    }

    pub fn error(message: impl Into<String>) -> Self {
        Self {
            ok: false,
            data: BusResponseData::Error {
                message: message.into(),
            },
        }
    }
}

/// Response payload variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BusResponseData {
    AskAccepted {
        req_id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        provider: String,
    },
    PendResult {
        replies: Vec<ReplyEntry>,
        #[serde(skip_serializing_if = "Option::is_none")]
        chain: Option<ChainProgress>,
    },
    PingResult {
        online: bool,
        #[serde(skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        provider: String,
        details: String,
    },
    SessionList {
        sessions: Vec<SessionInfo>,
    },
    Cancelled {
        req_id: String,
    },
    Launched {
        provider: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        session_id: Option<String>,
        terminal_view_id: u64,
    },
    ChainStarted {
        chain_id: String,
        step_count: usize,
    },
    WaitResult {
        req_id: String,
        status: RequestStatus,
        content: String,
        elapsed_ms: u64,
    },
    Error {
        message: String,
    },
}

// ---------------------------------------------------------------------------
// Shared types
// ---------------------------------------------------------------------------

/// A single reply entry returned by Pend.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplyEntry {
    pub req_id: String,
    pub provider: String,
    pub content: String,
    pub timestamp_ms: u64,
    pub status: RequestStatus,
}

/// A single step in a chain request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainStep {
    pub provider: String,
    pub prompt: String,
}

/// Status of a single chain step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainStepResult {
    pub step: usize,
    pub provider: String,
    pub req_id: String,
    pub status: String,
    pub content: String,
}

/// Progress of a chain execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChainProgress {
    pub chain_id: String,
    pub current_step: usize,
    pub total_steps: usize,
    pub status: String,
    pub steps: Vec<ChainStepResult>,
}

/// Request lifecycle status.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestStatus {
    Queued,
    Injecting,
    Running,
    Success,
    Error,
    Timeout,
    Cancelled,
    SessionBusy,
}

impl std::fmt::Display for RequestStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Queued => write!(f, "queued"),
            Self::Injecting => write!(f, "injecting"),
            Self::Running => write!(f, "running"),
            Self::Success => write!(f, "success"),
            Self::Error => write!(f, "error"),
            Self::Timeout => write!(f, "timeout"),
            Self::Cancelled => write!(f, "cancelled"),
            Self::SessionBusy => write!(f, "session_busy"),
        }
    }
}

/// Info about a CLI agent session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SessionInfo {
    pub provider: String,
    pub terminal_view_id: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cwd: Option<String>,
    pub status: String,
}

// ---------------------------------------------------------------------------
// Bus address file (written to ~/.warp-ccb/bus-address.json)
// ---------------------------------------------------------------------------

/// Contents of the bus-address.json file for client discovery.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusAddressInfo {
    pub protocol_version: u32,
    pub pid: u32,
    pub socket_path: String,
    pub auth_token: String,
    pub created_at_ms: u64,
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_ask_request() {
        let req = BusRequest {
            v: 1,
            token: "test-token".to_string(),
            command: BusCommand::Ask {
                provider: "claude".to_string(),
                prompt: "say hello".to_string(),
                session_id: None,
                cwd: Some("/tmp/project".to_string()),
                req_id: "20260513-120000-000-1234-0".to_string(),
                caller: "codex".to_string(),
                queue: false,
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"type\":\"ask\""));
        assert!(json.contains("\"provider\":\"claude\""));
        assert!(json.contains("\"prompt\":\"say hello\""));
    }

    #[test]
    fn test_serialize_ping_response() {
        let resp = BusResponse::ok(BusResponseData::PingResult {
            online: true,
            session_id: Some("sess-123".to_string()),
            provider: "codex".to_string(),
            details: "connected".to_string(),
        });
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"ok\":true"));
        assert!(json.contains("\"type\":\"ping_result\""));
        assert!(json.contains("\"online\":true"));
    }

    #[test]
    fn test_serialize_error_response() {
        let resp = BusResponse::error("session not found");
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"ok\":false"));
        assert!(json.contains("\"type\":\"error\""));
        assert!(json.contains("session not found"));
    }

    #[test]
    fn test_roundtrip_bus_request() {
        let req = BusRequest {
            v: 1,
            token: "tok".to_string(),
            command: BusCommand::Ping {
                provider: "gemini".to_string(),
                session_id: None,
                cwd: None,
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        let de: BusRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(de.v, 1);
        assert_eq!(de.token, "tok");
        match de.command {
            BusCommand::Ping { provider, .. } => assert_eq!(provider, "gemini"),
            _ => panic!("expected Ping"),
        }
    }

    #[test]
    fn test_request_status_serde() {
        let statuses = vec![
            RequestStatus::Queued,
            RequestStatus::Running,
            RequestStatus::Success,
            RequestStatus::Error,
            RequestStatus::Timeout,
            RequestStatus::Cancelled,
        ];
        for s in statuses {
            let json = serde_json::to_string(&s).unwrap();
            let de: RequestStatus = serde_json::from_str(&json).unwrap();
            assert_eq!(s, de);
        }
    }
}

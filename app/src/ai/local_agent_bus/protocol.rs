//! LocalAgentBus Protocol Types
//!
//! JSONL-based request/response protocol for CCB-Warp communication.
//! Each request is a single JSON line sent over Unix socket or Windows named pipe.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
        #[serde(skip_serializing_if = "Option::is_none")]
        caller_terminal_view_id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none")]
        caller_session_id: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        caller_cwd: Option<String>,
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
        alias: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        prompt: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        cwd: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        tab_config_name: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        params: Option<HashMap<String, String>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        target_view_id: Option<u64>,
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
    ClosePane {
        view_id: u64,
    },
    CloseSession {
        provider: String,
        view_id: u64,
    },
    #[serde(alias = "Reply")]
    Reply {
        req_id: String,
        content: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        caller: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        caller_terminal_view_id: Option<u64>,
        #[serde(skip_serializing_if = "Option::is_none", alias = "caller_cwd")]
        cwd: Option<String>,
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
    AskSkipped {
        req_id: String,
        provider: String,
        reason: String,
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
    TabConfigLaunched {
        config_name: String,
        pane_ids: HashMap<String, u64>,
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
        #[serde(default)]
        source: Option<String>,
        #[serde(default)]
        confidence: Option<f64>,
        #[serde(default)]
        warnings: Vec<String>,
    },
    ReplyAccepted {
        req_id: String,
        already_finalized: bool,
    },
    PaneClosed {
        view_id: u64,
    },
    SessionClosed {
        provider: String,
        view_id: u64,
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
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub confidence: Option<f64>,
    #[serde(default)]
    pub warnings: Vec<String>,
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
    pub alias: Option<String>,
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
                caller_terminal_view_id: Some(2247),
                caller_session_id: Some("caller-session".to_string()),
                caller_cwd: Some("/tmp/caller".to_string()),
                queue: false,
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"type\":\"ask\""));
        assert!(json.contains("\"provider\":\"claude\""));
        assert!(json.contains("\"prompt\":\"say hello\""));
        assert!(json.contains("\"caller_terminal_view_id\":2247"));
    }

    #[test]
    fn test_deserialize_ask_request_with_caller_identity() {
        let json = r#"{
            "v": 1,
            "token": "tok",
            "type": "ask",
            "provider": "claude",
            "prompt": "hello",
            "req_id": "req-1",
            "caller": "kimi",
            "caller_terminal_view_id": 2247,
            "caller_session_id": "kimi-session",
            "caller_cwd": "D:\\GitHub\\warp-ccb"
        }"#;

        let req: BusRequest = serde_json::from_str(json).unwrap();
        match req.command {
            BusCommand::Ask {
                caller,
                caller_terminal_view_id,
                caller_session_id,
                caller_cwd,
                ..
            } => {
                assert_eq!(caller, "kimi");
                assert_eq!(caller_terminal_view_id, Some(2247));
                assert_eq!(caller_session_id.as_deref(), Some("kimi-session"));
                assert_eq!(caller_cwd.as_deref(), Some("D:\\GitHub\\warp-ccb"));
            }
            _ => panic!("expected Ask"),
        }
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
    fn test_serialize_ask_skipped_response() {
        let resp = BusResponse::ok(BusResponseData::AskSkipped {
            req_id: "r-self".to_string(),
            provider: "claude".to_string(),
            reason: "self_request_skipped".to_string(),
        });
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"ok\":true"));
        assert!(json.contains("\"type\":\"ask_skipped\""));
        assert!(json.contains("\"reason\":\"self_request_skipped\""));
    }

    #[test]
    fn test_deserialize_reply_request() {
        let json = r#"{
            "v": 1,
            "token": "tok",
            "type": "reply",
            "req_id": "req-1",
            "content": "hello",
            "caller": "codex",
            "caller_terminal_view_id": 2247,
            "cwd": "D:\\GitHub\\warp-ccb"
        }"#;

        let req: BusRequest = serde_json::from_str(json).unwrap();
        match req.command {
            BusCommand::Reply {
                req_id,
                content,
                caller,
                caller_terminal_view_id,
                cwd,
            } => {
                assert_eq!(req_id, "req-1");
                assert_eq!(content, "hello");
                assert_eq!(caller.as_deref(), Some("codex"));
                assert_eq!(caller_terminal_view_id, Some(2247));
                assert_eq!(cwd.as_deref(), Some("D:\\GitHub\\warp-ccb"));
            }
            _ => panic!("expected Reply"),
        }
    }

    #[test]
    fn test_deserialize_reply_request_accepts_pascal_case_type() {
        let json = r#"{
            "v": 1,
            "token": "tok",
            "type": "Reply",
            "req_id": "req-1",
            "content": "hello"
        }"#;

        let req: BusRequest = serde_json::from_str(json).unwrap();
        match req.command {
            BusCommand::Reply {
                req_id, content, ..
            } => {
                assert_eq!(req_id, "req-1");
                assert_eq!(content, "hello");
            }
            _ => panic!("expected Reply"),
        }
    }

    #[test]
    fn test_deserialize_reply_request_accepts_caller_cwd_alias() {
        let json = r#"{
            "v": 1,
            "token": "tok",
            "type": "reply",
            "req_id": "req-1",
            "content": "hello",
            "caller_cwd": "D:\\GitHub\\warp-ccb"
        }"#;

        let req: BusRequest = serde_json::from_str(json).unwrap();
        match req.command {
            BusCommand::Reply { cwd, .. } => {
                assert_eq!(cwd.as_deref(), Some("D:\\GitHub\\warp-ccb"));
            }
            _ => panic!("expected Reply"),
        }
    }

    #[test]
    fn test_serialize_reply_accepted_response() {
        let resp = BusResponse::ok(BusResponseData::ReplyAccepted {
            req_id: "req-1".to_string(),
            already_finalized: false,
        });
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"ok\":true"));
        assert!(json.contains("\"type\":\"reply_accepted\""));
        assert!(json.contains("\"req_id\":\"req-1\""));
        assert!(json.contains("\"already_finalized\":false"));
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

    #[test]
    fn test_serialize_close_pane_request() {
        let req = BusRequest {
            v: 1,
            token: "test-token".to_string(),
            command: BusCommand::ClosePane { view_id: 42 },
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"type\":\"close_pane\""));
        assert!(json.contains("\"view_id\":42"));

        let de: BusRequest = serde_json::from_str(&json).unwrap();
        match de.command {
            BusCommand::ClosePane { view_id } => assert_eq!(view_id, 42),
            _ => panic!("expected ClosePane"),
        }
    }

    #[test]
    fn test_serialize_close_session_request() {
        let req = BusRequest {
            v: 1,
            token: "test-token".to_string(),
            command: BusCommand::CloseSession {
                provider: "claude".to_string(),
                view_id: 99,
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"type\":\"close_session\""));
        assert!(json.contains("\"provider\":\"claude\""));
        assert!(json.contains("\"view_id\":99"));

        let de: BusRequest = serde_json::from_str(&json).unwrap();
        match de.command {
            BusCommand::CloseSession { provider, view_id } => {
                assert_eq!(provider, "claude");
                assert_eq!(view_id, 99);
            }
            _ => panic!("expected CloseSession"),
        }
    }

    #[test]
    fn test_serialize_pane_closed_response() {
        let resp = BusResponse::ok(BusResponseData::PaneClosed { view_id: 42 });
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"ok\":true"));
        assert!(json.contains("\"type\":\"pane_closed\""));
        assert!(json.contains("\"view_id\":42"));

        let de: BusResponse = serde_json::from_str(&json).unwrap();
        assert!(de.ok);
        match de.data {
            BusResponseData::PaneClosed { view_id } => assert_eq!(view_id, 42),
            _ => panic!("expected PaneClosed"),
        }
    }

    #[test]
    fn test_serialize_session_closed_response() {
        let resp = BusResponse::ok(BusResponseData::SessionClosed {
            provider: "codex".to_string(),
            view_id: 77,
        });
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"ok\":true"));
        assert!(json.contains("\"type\":\"session_closed\""));
        assert!(json.contains("\"provider\":\"codex\""));
        assert!(json.contains("\"view_id\":77"));

        let de: BusResponse = serde_json::from_str(&json).unwrap();
        assert!(de.ok);
        match de.data {
            BusResponseData::SessionClosed { provider, view_id } => {
                assert_eq!(provider, "codex");
                assert_eq!(view_id, 77);
            }
            _ => panic!("expected SessionClosed"),
        }
    }

    #[test]
    fn test_serialize_extended_launch() {
        let mut params = HashMap::new();
        params.insert("project_dir".to_string(), "/project".to_string());
        let req = BusRequest {
            v: 1,
            token: "test-token".to_string(),
            command: BusCommand::Launch {
                provider: "claude".to_string(),
                alias: None,
                prompt: Some("hello".to_string()),
                cwd: Some("/tmp".to_string()),
                tab_config_name: Some("my_team".to_string()),
                params: Some(params),
                target_view_id: Some(42),
            },
        };
        let json = serde_json::to_string(&req).unwrap();
        assert!(json.contains("\"type\":\"launch\""));
        assert!(json.contains("\"provider\":\"claude\""));
        assert!(json.contains("\"tab_config_name\":\"my_team\""));
        assert!(json.contains("\"target_view_id\":42"));
        assert!(json.contains("\"project_dir\":\"/project\""));
    }

    #[test]
    fn test_deserialize_old_launch_backward_compat() {
        let json = r#"{
            "v": 1,
            "token": "tok",
            "type": "launch",
            "provider": "codex",
            "prompt": "hello",
            "cwd": "/tmp"
        }"#;

        let req: BusRequest = serde_json::from_str(json).unwrap();
        match req.command {
            BusCommand::Launch {
                provider,
                alias,
                prompt,
                cwd,
                tab_config_name,
                params,
                target_view_id,
            } => {
                assert_eq!(provider, "codex");
                assert!(alias.is_none());
                assert_eq!(prompt, Some("hello".to_string()));
                assert_eq!(cwd, Some("/tmp".to_string()));
                assert!(tab_config_name.is_none());
                assert!(params.is_none());
                assert!(target_view_id.is_none());
            }
            _ => panic!("expected Launch"),
        }
    }

    #[test]
    fn test_roundtrip_tab_config_launched() {
        let mut pane_ids = HashMap::new();
        pane_ids.insert("pane0".to_string(), 100);
        pane_ids.insert("pane1".to_string(), 101);
        let resp = BusResponse::ok(BusResponseData::TabConfigLaunched {
            config_name: "my_team".to_string(),
            pane_ids,
        });
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("\"ok\":true"));
        assert!(json.contains("\"type\":\"tab_config_launched\""));
        assert!(json.contains("\"config_name\":\"my_team\""));

        let de: BusResponse = serde_json::from_str(&json).unwrap();
        assert!(de.ok);
        match de.data {
            BusResponseData::TabConfigLaunched { config_name, pane_ids } => {
                assert_eq!(config_name, "my_team");
                assert_eq!(pane_ids.get("pane0"), Some(&100));
                assert_eq!(pane_ids.get("pane1"), Some(&101));
            }
            _ => panic!("expected TabConfigLaunched"),
        }
    }
}

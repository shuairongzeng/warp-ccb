//! Completion Tracker — layered completion detection for injected requests
//!
//! Detection layers:
//! 1. Primary: CCB_DONE:{req_id} marker in agent output
//! 2. Auxiliary: OSC 777 StatusChanged (Stop) event
//! 3. Fallback: timeout (configurable, default 120s)
//! 4. Error: session disappeared / pane closed / agent exited

use std::collections::HashMap;
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
    last_output_len: Option<usize>,
    last_output_changed_at: Instant,
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
        let now = Instant::now();
        self.pending.insert(
            req_id,
            PendingRequest {
                provider,
                terminal_view_id,
                started_at: now,
                timeout: Duration::from_secs(self.timeout_secs),
                last_output_len: None,
                last_output_changed_at: now,
            },
        );
    }

    /// Remove a request from tracking.
    pub fn deregister(&mut self, req_id: &str) {
        self.pending.remove(req_id);
    }

    /// Check if a completion marker was found in output text.
    ///
    /// Detects `[CCB_END:{req_id}]` that is NOT wrapped in backticks.
    /// Instruction markers are backtick-wrapped (`` `[CCB_END:xxx]` ``),
    /// agent markers are plain (`[CCB_END:xxx]`), so we skip backtick-wrapped ones.
    /// Falls back to old format: CCB_DONE:{req_id} on its own line.
    pub fn check_done_marker(&self, req_id: &str, output: &str) -> bool {
        for marker_id in reply_marker_ids(req_id) {
            if find_terminal_unwrapped_ccb_tag_range(output, "CCB_END", &marker_id).is_some() {
                return true;
            }
        }

        // Old format: CCB_DONE:{req_id} on its own line
        let marker = format!("CCB_DONE:{}", req_id);
        output.lines().any(|line| {
            let trimmed = line.trim();
            trimmed == marker || trimmed.starts_with(&format!("● CCB_DONE:{}", req_id))
        })
    }

    /// 判断当前输出长度是否已经稳定一段时间，避免流式输出还没结束就截取回复。
    pub fn is_output_length_stable(
        &mut self,
        req_id: &str,
        output: &str,
        stable_for: Duration,
    ) -> bool {
        let Some(req) = self.pending.get_mut(req_id) else {
            return false;
        };

        let now = Instant::now();
        let output_len = output.len();
        if req.last_output_len != Some(output_len) {
            req.last_output_len = Some(output_len);
            req.last_output_changed_at = now;
            return false;
        }

        now.duration_since(req.last_output_changed_at) >= stable_for
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

pub(crate) fn find_unwrapped_ccb_tag_ranges(
    output: &str,
    tag_name: &str,
    req_id: &str,
) -> Vec<(usize, usize)> {
    let prefix = format!("[{}:", tag_name);
    let mut ranges = Vec::new();
    let mut search_from = 0;

    while search_from < output.len() {
        let Some(offset) = output[search_from..].find(&prefix) else {
            break;
        };
        let start = search_from + offset;
        let mut idx = start + prefix.len();
        let mut matched = true;

        for expected in req_id.chars() {
            idx = skip_whitespace_at(output, idx);
            match next_char_at(output, idx) {
                Some((ch, next_idx)) if ch == expected => {
                    idx = next_idx;
                }
                _ => {
                    matched = false;
                }
            }
            if !matched {
                break;
            }
        }

        if matched {
            idx = skip_whitespace_at(output, idx);
            match next_char_at(output, idx) {
                Some((ch, next_idx)) if ch == ']' => {
                    let preceded_by_backtick = output[..start].chars().next_back() == Some('`');
                    let followed_by_backtick = output[next_idx..].chars().next() == Some('`');
                    if !preceded_by_backtick && !followed_by_backtick {
                        ranges.push((start, next_idx));
                    }
                    search_from = next_idx;
                }
                _ => {
                    search_from = start + prefix.len();
                }
            }
        } else {
            search_from = start + prefix.len();
        }
    }

    ranges
}

pub(crate) fn reply_marker_ids(req_id: &str) -> Vec<String> {
    vec![format!("reply-{}", req_id), req_id.to_string()]
}

pub(crate) fn find_terminal_unwrapped_ccb_tag_range(
    output: &str,
    tag_name: &str,
    marker_id: &str,
) -> Option<(usize, usize)> {
    let ranges = find_unwrapped_ccb_tag_ranges(output, tag_name, marker_id);
    let range = ranges.last().copied()?;
    if has_meaningful_content_after(output, range.1) {
        None
    } else {
        Some(range)
    }
}

pub(crate) fn has_meaningful_content_after(output: &str, index: usize) -> bool {
    let Some(tail) = output.get(index..) else {
        return false;
    };

    tail.lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .any(|line| !is_terminal_ui_line(line))
}

fn is_terminal_ui_line(line: &str) -> bool {
    let trimmed = line.trim();
    let lower = trimmed.to_ascii_lowercase();
    if lower.contains("context:")
        || lower.starts_with("yolo  agent")
        || lower.starts_with("yolo agent")
        || lower.contains(" agent (kimi-")
    {
        return true;
    }

    let compact: String = line
        .chars()
        .filter(|ch| !ch.is_whitespace() && !is_box_drawing_or_separator(*ch))
        .collect();

    compact.is_empty() || compact.eq_ignore_ascii_case("input")
}

fn is_box_drawing_or_separator(ch: char) -> bool {
    matches!(
        ch,
        '─' | '━'
            | '│'
            | '┃'
            | '╭'
            | '╮'
            | '╰'
            | '╯'
            | '┌'
            | '┐'
            | '└'
            | '┘'
            | '├'
            | '┤'
            | '┬'
            | '┴'
            | '┼'
            | '═'
            | '║'
            | '╔'
            | '╗'
            | '╚'
            | '╝'
            | '-'
    )
}

fn skip_whitespace_at(input: &str, mut index: usize) -> usize {
    while let Some((ch, next_idx)) = next_char_at(input, index) {
        if !ch.is_whitespace() {
            break;
        }
        index = next_idx;
    }
    index
}

fn next_char_at(input: &str, index: usize) -> Option<(char, usize)> {
    let ch = input.get(index..)?.chars().next()?;
    Some((ch, index + ch.len_utf8()))
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
        // New format: [CCB_END:{req_id}] at terminal position (not backtick-wrapped)
        assert!(tracker.check_done_marker("r1", "some output\n[CCB_END:r1]\n"));
        // Does not match when real content appears after the last END marker.
        assert!(!tracker.check_done_marker("r1", "text [CCB_END:r1] more"));
        // Matches when terminal wrapping inserts whitespace into the req_id.
        assert!(tracker.check_done_marker(
            "20260515-130313-6aea5fe7",
            "[CCB_END:20\n260515-130313-6aea5fe7]"
        ));
        // With ● prefix
        assert!(tracker.check_done_marker("r1", "some output\n● [CCB_END:r1]\n"));
        // Old format: CCB_DONE on its own line
        assert!(tracker.check_done_marker("r1", "some output\nCCB_DONE:r1\nmore"));
        // Matches with leading whitespace
        assert!(tracker.check_done_marker("r1", "  CCB_DONE:r1"));
        // Does NOT match when it's part of a longer line (like the prompt instruction)
        assert!(!tracker.check_done_marker("r1", "some output CCB_DONE:r1 end"));
        // Does NOT match the injected prompt instruction
        assert!(!tracker.check_done_marker("r1", "Reply with CCB_DONE:r1 when done."));
        // Does NOT match backtick-wrapped markers (instruction text)
        assert!(!tracker.check_done_marker("r1", "`[CCB_END:r1]`"));
        assert!(!tracker.check_done_marker("r1", "without backticks:\n`[CCB_END:r1]`"));
        assert!(!tracker.check_done_marker("r1", "some output without marker"));
        assert!(!tracker.check_done_marker("r1", "CCB_DONE:r2"));
    }

    #[test]
    fn test_done_marker_incomplete_tag_does_not_match() {
        let tracker = CompletionTracker::new();
        // 缺失右括号的半截标记不能匹配，也不能让扫描卡住。
        assert!(!tracker.check_done_marker(
            "20260515-130313-6aea5fe7",
            "[CCB_END:20260515-130313-6aea5fe7"
        ));
    }

    #[test]
    fn test_done_marker_requires_terminal_end_marker() {
        let tracker = CompletionTracker::new();
        let req_id = "20260515-155427-7d26c5be";
        let thinking = "\
• 用户要求我介绍一下自己，格式要求是 [CCB_START:20260515-155427-7d26c5be] 和 [CCB_END:20260515-155427-7d26c5be] 包裹回复内容。

  我应该简洁地介绍自己。

⠙ Composing... <1s · 2 tokens";
        assert!(!tracker.check_done_marker(req_id, thinking));

        let final_output = "\
[CCB_START:20260515-155427-7d26c5be]
我是 Kimi Code CLI。
[CCB_END:20260515-155427-7d26c5be]

── input ──────────────────────────────────────────────────";
        assert!(tracker.check_done_marker(req_id, final_output));
    }

    #[test]
    fn test_done_marker_allows_kimi_status_footer_after_end() {
        let tracker = CompletionTracker::new();
        let req_id = "20260515-192826-f8fdad02";
        let output = "\
[CCB_START:reply-20260515-192826-f8fdad02]
你好！我是 Kimi Code CLI。
[CCB_END:reply-20260515-192826-f8fdad02]



── input ──────────────────────────────────────────────────







───────────────────────────────────────────────────────────
yolo  agent (Kimi-k2.6 ●)  D:\\GitHub\\warp-ccb
                               context: 4.9% (12.8k/262.1k)";
        assert!(tracker.check_done_marker(req_id, output));
    }

    #[test]
    fn test_done_marker_supports_reply_prefixed_wrapped_marker() {
        let tracker = CompletionTracker::new();
        let req_id = "20260515-155427-7d26c5be";
        assert!(tracker.check_done_marker(
            req_id,
            "[CCB_START:reply-20260515-155427-7d26c5be]ok[CCB_END:reply-20260515-155427-\n7d26c5be]"
        ));
    }

    #[test]
    fn test_output_length_stability_waits_for_settle_delay() {
        let mut tracker = CompletionTracker::new();
        let req_id = "stable-r1";
        tracker.register(req_id.into(), "kimi".into(), EntityId::from_usize(42));

        assert!(!tracker.is_output_length_stable(req_id, "abc", Duration::from_millis(500)));
        assert!(!tracker.is_output_length_stable(req_id, "abcd", Duration::from_millis(500)));
        std::thread::sleep(Duration::from_millis(550));
        assert!(tracker.is_output_length_stable(req_id, "abcd", Duration::from_millis(500)));
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

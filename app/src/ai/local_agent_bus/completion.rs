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

use crate::terminal::cli_agent::CLIAgent;

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
    has_session_listener: bool,
    provider_agent: Option<CLIAgent>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionStrategy {
    /// 有 session listener，快速完成
    SessionAware,
    /// 无 session listener，中等等待
    RawOutputOnly,
    /// Hard cutoff
    HardTimeout,
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
                has_session_listener: false,
                provider_agent: None,
            },
        );
    }

    /// Remove a request from tracking.
    pub fn deregister(&mut self, req_id: &str) {
        self.pending.remove(req_id);
    }

    /// Check if a completion marker was found in output text.
    ///
    /// 优先检测最后一个有效 `[CCB_START:reply-{req_id}]... [CCB_END:reply-{req_id}]`
    /// 闭环。提示词和思考文本可能会复述 marker，所以不能只看第一个 END。
    /// Falls back to old format: CCB_DONE:{req_id} on its own line.
    pub fn check_done_marker(&self, req_id: &str, output: &str) -> bool {
        for marker_id in reply_marker_ids(req_id) {
            let start_ranges = find_unwrapped_ccb_tag_ranges(output, "CCB_START", &marker_id);
            if find_last_complete_reply_span(output, &marker_id).is_some() {
                return true;
            }

            // 兼容早期只输出 END/CCB_DONE 的场景；一旦已经出现 START，就不再用
            // END-only fallback，避免把提示词或思考文本里的 marker 当成完成信号。
            if start_ranges.is_empty()
                && find_terminal_unwrapped_ccb_tag_range(output, "CCB_END", &marker_id).is_some()
            {
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

    /// 判断请求是否应该被完成（不是超时，而是输出稳定后的正常完成）
    pub fn should_finalize_passive(&self, req_id: &str, output_stable_for: Duration) -> bool {
        let pending = match self.pending.get(req_id) {
            Some(p) => p,
            None => return false,
        };

        let strategy = self.strategy_for(pending);
        let elapsed = pending.started_at.elapsed();
        let min_runtime = self.min_runtime_for(strategy);

        log::debug!(
            "CompletionTracker: should_finalize_passive req_id={} strategy={:?} elapsed={:?} min_runtime={:?} output_stable_for={:?}",
            req_id,
            strategy,
            elapsed,
            min_runtime,
            output_stable_for,
        );

        match strategy {
            CompletionStrategy::SessionAware => {
                elapsed >= min_runtime && output_stable_for >= Duration::from_secs(2)
            }
            CompletionStrategy::RawOutputOnly => {
                elapsed >= min_runtime && output_stable_for >= Duration::from_secs(8)
            }
            CompletionStrategy::HardTimeout => false,
        }
    }

    fn strategy_for(&self, pending: &PendingRequest) -> CompletionStrategy {
        if pending.has_session_listener {
            CompletionStrategy::SessionAware
        } else {
            CompletionStrategy::RawOutputOnly
        }
    }

    fn min_runtime_for(&self, strategy: CompletionStrategy) -> Duration {
        match strategy {
            CompletionStrategy::SessionAware => Duration::from_secs(3),
            CompletionStrategy::RawOutputOnly => Duration::from_secs(10),
            CompletionStrategy::HardTimeout => Duration::from_secs(60),
        }
    }

    pub fn set_has_session_listener(&mut self, req_id: &str, has: bool) {
        if let Some(pending) = self.pending.get_mut(req_id) {
            pending.has_session_listener = has;
            log::debug!(
                "CompletionTracker: set_has_session_listener req_id={} has={}",
                req_id,
                has
            );
        }
    }

    pub fn set_provider_agent(&mut self, req_id: &str, agent: CLIAgent) {
        if let Some(pending) = self.pending.get_mut(req_id) {
            pending.provider_agent = Some(agent);
        }
    }

    /// 计算当前输出已经稳定了多久。如果输出长度还在变化，返回 None。
    pub fn output_stable_duration(&mut self, req_id: &str, output: &str) -> Option<Duration> {
        let req = self.pending.get_mut(req_id)?;
        let output_len = output.len();
        if req.last_output_len != Some(output_len) {
            req.last_output_len = Some(output_len);
            req.last_output_changed_at = Instant::now();
            return None;
        }
        Some(req.last_output_changed_at.elapsed())
    }
}

pub(crate) fn find_unwrapped_ccb_tag_ranges(
    output: &str,
    tag_name: &str,
    req_id: &str,
) -> Vec<(usize, usize)> {
    let mut ranges = Vec::new();
    for prefix in ccb_tag_prefixes(tag_name) {
        let mut search_from = 0;

        while search_from < output.len() {
            let Some(offset) = output[search_from..].find(&prefix.text) else {
                break;
            };
            let prefix_start = search_from + offset;
            let Some(start) = resolve_ccb_tag_start(output, prefix_start, &prefix, tag_name) else {
                search_from = prefix_start + prefix.text.len();
                continue;
            };

            let mut idx = prefix_start + prefix.text.len();
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
                        search_from = prefix_start + prefix.text.len();
                    }
                }
            } else {
                search_from = prefix_start + prefix.text.len();
            }
        }
    }

    ranges.sort_unstable();
    ranges.dedup();
    ranges
}

struct CcbTagPrefix {
    text: String,
    split_after_ccb: bool,
}

fn ccb_tag_prefixes(tag_name: &str) -> Vec<CcbTagPrefix> {
    if tag_name == "CCB_END" {
        vec![
            CcbTagPrefix::new("[CCB_END:", false),
            CcbTagPrefix::new("CCB_END:", false),
            CcbTagPrefix::new("B_END:", false),
            CcbTagPrefix::new("END:", true),
        ]
    } else if tag_name == "CCB_START" {
        vec![
            CcbTagPrefix::new("[CCB_START:", false),
            CcbTagPrefix::new("START:", true),
        ]
    } else {
        vec![CcbTagPrefix::new(format!("[{}:", tag_name), false)]
    }
}

impl CcbTagPrefix {
    fn new(text: impl Into<String>, split_after_ccb: bool) -> Self {
        Self {
            text: text.into(),
            split_after_ccb,
        }
    }
}

fn resolve_ccb_tag_start(
    output: &str,
    prefix_start: usize,
    prefix: &CcbTagPrefix,
    tag_name: &str,
) -> Option<usize> {
    if prefix.split_after_ccb {
        return split_ccb_prefix_start(output, prefix_start);
    }

    if is_valid_ccb_tag_prefix_occurrence(output, prefix_start, &prefix.text, tag_name) {
        Some(prefix_start)
    } else {
        None
    }
}

fn split_ccb_prefix_start(output: &str, prefix_start: usize) -> Option<usize> {
    let mut end = prefix_start;
    while end > 0 {
        let (prev, ch) = prev_char_before(output, end)?;
        if ch.is_whitespace() {
            end = prev;
        } else {
            break;
        }
    }

    let before = output.get(..end)?;
    if before.ends_with("[CCB_") {
        Some(end - "[CCB_".len())
    } else if before.ends_with("CCB_") {
        Some(end - "CCB_".len())
    } else {
        None
    }
}

fn is_valid_ccb_tag_prefix_occurrence(
    output: &str,
    start: usize,
    prefix: &str,
    tag_name: &str,
) -> bool {
    if tag_name != "CCB_END" {
        return true;
    }

    let before = &output[..start];
    match prefix {
        "CCB_END:" => !before.ends_with('['),
        "B_END:" => !before.ends_with("[CC") && !before.ends_with("CC"),
        _ => true,
    }
}

pub(crate) fn reply_marker_ids(req_id: &str) -> Vec<String> {
    vec![format!("reply-{}", req_id), req_id.to_string()]
}

pub(crate) fn find_last_complete_reply_span(
    output: &str,
    marker_id: &str,
) -> Option<(usize, usize)> {
    let start_ranges = find_unwrapped_ccb_tag_ranges(output, "CCB_START", marker_id);
    let end_ranges = find_unwrapped_ccb_tag_ranges(output, "CCB_END", marker_id);

    for (end_pos, end_after) in end_ranges.iter().rev().copied() {
        if !has_protocol_end_context(output, end_after) {
            continue;
        }

        let Some((_start_pos, content_start)) =
            start_ranges
                .iter()
                .rev()
                .copied()
                .find(|(start_pos, content_start)| {
                    *start_pos < end_pos
                        && *content_start <= end_pos
                        && has_protocol_start_context(output, *start_pos)
                })
        else {
            continue;
        };

        let Some(reply) = output.get(content_start..end_pos) else {
            continue;
        };

        if is_valid_reply_content(reply) {
            return Some((content_start, end_pos));
        }
    }

    None
}

pub(crate) fn is_valid_reply_content(reply: &str) -> bool {
    let reply = reply.trim();
    !reply.is_empty() && !is_instruction_like_reply_content(reply)
}

fn is_instruction_like_reply_content(reply: &str) -> bool {
    reply.contains("on its own line")
        || reply.contains("Before your final reply")
        || reply.contains("After your final reply")
        || reply.contains("Reply using exactly this format")
        || reply.contains("without backticks")
        || reply.contains("<your reply>")
}

fn has_protocol_start_context(output: &str, marker_start: usize) -> bool {
    let line_start = output[..marker_start]
        .rfind('\n')
        .map(|p| p + 1)
        .unwrap_or(0);
    let prefix = &output[line_start..marker_start];
    let compact: String = prefix.chars().filter(|ch| !ch.is_whitespace()).collect();

    compact.is_empty() || matches!(compact.as_str(), "●" | "•" | "⛬" | "▎" | "┃")
}

fn has_protocol_end_context(output: &str, marker_end: usize) -> bool {
    let line_end = output[marker_end..]
        .find('\n')
        .map(|p| marker_end + p)
        .unwrap_or(output.len());
    output[marker_end..line_end].trim().is_empty()
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
        || lower == ">"
        || lower.starts_with('›')
        || (lower.starts_with("gpt-") && lower.contains(" · "))
        || lower.contains("ready (restart to apply)")
        || (lower.starts_with("glm-") && lower.contains(" - openai"))
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

fn prev_char_before(input: &str, index: usize) -> Option<(usize, char)> {
    input.get(..index)?.char_indices().next_back()
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
    fn test_done_marker_allows_droid_status_footer_after_end() {
        let tracker = CompletionTracker::new();
        let req_id = "20260515-212919-2ba19670";
        let output = "\
⛬  [CCB_START:reply-20260515-212919-2ba1
   9670]
   你好！我是 Droid，一个由 Factory 构建的 AI 软件工程代理。
   [CCB_END:reply-20260515-212919-2ba196
   70]

GLM-5.1 [GLM Coding Plan China] - Openai […]

 >

[⏱ 21s] ✓ v0.126.0 ready (restart to apply)";
        assert!(tracker.check_done_marker(req_id, output));
    }

    #[test]
    fn test_done_marker_allows_codex_status_footer_after_end() {
        let tracker = CompletionTracker::new();
        let req_id = "20260515-223836-3484bb45";
        let output = "\
• [CCB_START:reply-20260515-223836-
  3484bb45]
  我是 Codex，一个在你当前工作区内协作的 AI 编程助手。
  [CCB_END:reply-20260515-223836-3484bb45]

───────────────────────────────────────────


› Explain this codebase

  gpt-5.5 xhigh · D:\\GitHub\\warp-ccb";
        assert!(tracker.check_done_marker(req_id, output));
    }

    #[test]
    fn test_done_marker_uses_last_valid_reply_closure() {
        let tracker = CompletionTracker::new();
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
你好！我是 Kimi Code CLI。
[CCB_END:reply-20260515-225555-979c6156]
后面出现新的交互提示或任意终端内容";

        assert!(
            tracker.check_done_marker(req_id, output),
            "应识别最后一个有效 START/END 闭环，而不是被前面的 instruction marker 或 END 后尾巴阻塞"
        );
    }

    #[test]
    fn test_done_marker_allows_truncated_grid_end_prefix() {
        let tracker = CompletionTracker::new();
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

        assert!(
            tracker.check_done_marker(req_id, output),
            "Warp 输出网格可能在行尾裁掉 END marker 的 `[CC` 前缀，仍应识别真实回复闭环"
        );
    }

    #[test]
    fn test_done_marker_allows_split_ccb_end_tag_name() {
        let tracker = CompletionTracker::new();
        let req_id = "20260516-144955-8cc19f1e";
        let output = "\
• [CCB_START:reply-20260516-
  144955-8cc19f1e] 重读《背影》，依然湿了眼眶。
  有些爱，经不起等待；有些人，需要好好珍惜。 [CCB_
  END:reply-20260516-144955-
  8cc19f1e]

── input ──────────────────";

        assert!(
            tracker.check_done_marker(req_id, output),
            "Warp 输出网格可能把 `[CCB_END:` 的 tag name 本身拆成 `[CCB_` + `END:`"
        );
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

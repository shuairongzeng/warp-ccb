use std::{
    collections::{HashMap, HashSet},
    time::{Duration, Instant},
};

use warpui::EntityId;

const DEFAULT_MAX_CAPTURE_BYTES: usize = 256 * 1024;

#[derive(Debug)]
pub(crate) struct RawOutputCapture {
    panes: HashSet<EntityId>,
    active_by_pane: HashMap<EntityId, HashSet<String>>,
    captures: HashMap<String, RawRequestCapture>,
    pending_utf8_by_pane: HashMap<EntityId, Vec<u8>>,
    max_capture_bytes: usize,
}

#[derive(Debug)]
struct RawRequestCapture {
    pane: EntityId,
    output: String,
    last_output_changed_at: Instant,
    started_at: Instant,
}

impl RawOutputCapture {
    pub(crate) fn new() -> Self {
        Self::with_max_bytes(DEFAULT_MAX_CAPTURE_BYTES)
    }

    pub(crate) fn with_max_bytes(max_capture_bytes: usize) -> Self {
        Self {
            panes: HashSet::new(),
            active_by_pane: HashMap::new(),
            captures: HashMap::new(),
            pending_utf8_by_pane: HashMap::new(),
            max_capture_bytes,
        }
    }

    pub(crate) fn register_pane(&mut self, pane: EntityId) {
        self.panes.insert(pane);
    }

    pub(crate) fn deregister_pane(&mut self, pane: EntityId) {
        self.panes.remove(&pane);
        self.pending_utf8_by_pane.remove(&pane);
        if let Some(req_ids) = self.active_by_pane.remove(&pane) {
            for req_id in req_ids {
                self.captures.remove(&req_id);
            }
        }
    }

    pub(crate) fn register_request(&mut self, req_id: String, pane: EntityId) {
        self.register_pane(pane);
        self.active_by_pane
            .entry(pane)
            .or_default()
            .insert(req_id.clone());
        let now = Instant::now();
        self.captures.insert(
            req_id,
            RawRequestCapture {
                pane,
                output: String::new(),
                last_output_changed_at: now,
                started_at: now,
            },
        );
    }

    pub(crate) fn deregister_request(&mut self, req_id: &str) {
        if let Some(capture) = self.captures.remove(req_id) {
            if let Some(req_ids) = self.active_by_pane.get_mut(&capture.pane) {
                req_ids.remove(req_id);
                if req_ids.is_empty() {
                    self.active_by_pane.remove(&capture.pane);
                }
            }
        }
    }

    pub(crate) fn append(&mut self, pane: EntityId, bytes: &[u8]) {
        if !self.panes.contains(&pane) {
            return;
        }
        let Some(req_ids) = self.active_by_pane.get(&pane).cloned() else {
            return;
        };
        if req_ids.is_empty() {
            return;
        }

        let text = self.decode_utf8_chunk(pane, bytes);
        if text.is_empty() {
            return;
        }
        let normalized = normalize_raw_terminal_text(&strip_ansi_sequences(&text));
        if normalized.is_empty() {
            return;
        }

        let now = Instant::now();
        for req_id in req_ids {
            if let Some(capture) = self.captures.get_mut(&req_id) {
                capture.output.push_str(&normalized);
                trim_to_max_bytes(&mut capture.output, self.max_capture_bytes);
                capture.last_output_changed_at = now;
            }
        }
    }

    pub(crate) fn snapshot(&self, req_id: &str) -> Option<String> {
        self.captures
            .get(req_id)
            .map(|capture| capture.output.clone())
    }

    pub(crate) fn started_at(&self, req_id: &str) -> Option<Instant> {
        self.captures.get(req_id).map(|capture| capture.started_at)
    }

    pub(crate) fn is_request_stable(&self, req_id: &str, stable_for: Duration) -> bool {
        self.captures
            .get(req_id)
            .map(|capture| {
                Instant::now().duration_since(capture.last_output_changed_at) >= stable_for
            })
            .unwrap_or(false)
    }

    fn decode_utf8_chunk(&mut self, pane: EntityId, bytes: &[u8]) -> String {
        let mut pending = self.pending_utf8_by_pane.remove(&pane).unwrap_or_default();
        pending.extend_from_slice(bytes);

        let mut decoded = String::new();
        let mut remaining = pending.as_slice();
        loop {
            match std::str::from_utf8(remaining) {
                Ok(valid) => {
                    decoded.push_str(valid);
                    self.pending_utf8_by_pane.remove(&pane);
                    break;
                }
                Err(err) => {
                    let valid_up_to = err.valid_up_to();
                    if valid_up_to > 0 {
                        decoded
                            .push_str(std::str::from_utf8(&remaining[..valid_up_to]).unwrap_or(""));
                    }

                    match err.error_len() {
                        Some(invalid_len) => {
                            decoded.push('\u{FFFD}');
                            let next = valid_up_to + invalid_len;
                            remaining = &remaining[next.min(remaining.len())..];
                            if remaining.is_empty() {
                                self.pending_utf8_by_pane.remove(&pane);
                                break;
                            }
                        }
                        None => {
                            self.pending_utf8_by_pane
                                .insert(pane, remaining[valid_up_to..].to_vec());
                            break;
                        }
                    }
                }
            }
        }

        decoded
    }
}

fn normalize_raw_terminal_text(input: &str) -> String {
    let mut normalized = String::with_capacity(input.len());
    let mut chars = input.chars().peekable();
    while let Some(ch) = chars.next() {
        if ch == '\r' {
            if chars.peek() == Some(&'\n') {
                continue;
            }
            normalized.push('\n');
        } else {
            normalized.push(ch);
        }
    }
    normalized
}

fn strip_ansi_sequences(input: &str) -> String {
    #[derive(Clone, Copy)]
    enum State {
        Normal,
        Escape,
        Csi,
        Osc,
        OscEscape,
    }

    let mut output = String::with_capacity(input.len());
    let mut state = State::Normal;
    for ch in input.chars() {
        match state {
            State::Normal => {
                if ch == '\x1b' {
                    state = State::Escape;
                } else {
                    output.push(ch);
                }
            }
            State::Escape => match ch {
                '[' => state = State::Csi,
                ']' => state = State::Osc,
                '\x1b' => state = State::Escape,
                _ => state = State::Normal,
            },
            State::Csi => {
                if ('@'..='~').contains(&ch) {
                    state = State::Normal;
                }
            }
            State::Osc => match ch {
                '\x07' => state = State::Normal,
                '\x1b' => state = State::OscEscape,
                _ => {}
            },
            State::OscEscape => {
                state = if ch == '\\' {
                    State::Normal
                } else {
                    State::Osc
                };
            }
        }
    }
    output
}

fn trim_to_max_bytes(output: &mut String, max_bytes: usize) {
    if max_bytes == 0 {
        output.clear();
        return;
    }
    if output.len() <= max_bytes {
        return;
    }

    let mut start = output.len() - max_bytes;
    while start < output.len() && !output.is_char_boundary(start) {
        start += 1;
    }
    output.drain(..start);
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::time::Duration;

    use warpui::EntityId;

    #[test]
    fn captures_only_output_after_request_registration() {
        let pane = EntityId::from_usize(42);
        let mut capture = RawOutputCapture::with_max_bytes(4096);

        capture.register_pane(pane);
        capture.append(pane, b"before request\n");
        capture.register_request("r1".to_string(), pane);
        capture.append(
            pane,
            b"[CCB_START:reply-r1]\nraw reply\n[CCB_END:reply-r1]\n",
        );

        let snapshot = capture.snapshot("r1").expect("raw capture should exist");
        assert_eq!(
            snapshot,
            "[CCB_START:reply-r1]\nraw reply\n[CCB_END:reply-r1]\n"
        );
    }

    #[test]
    fn strips_ansi_sequences_and_normalizes_carriage_returns() {
        let pane = EntityId::from_usize(7);
        let mut capture = RawOutputCapture::with_max_bytes(4096);

        capture.register_pane(pane);
        capture.register_request("r1".to_string(), pane);
        capture.append(
            pane,
            b"\x1b[32m[CCB_START:reply-r1]\x1b[0r\nhello\r[CCB_END:reply-r1]\r\n",
        );

        let snapshot = capture.snapshot("r1").expect("raw capture should exist");
        assert_eq!(
            snapshot,
            "[CCB_START:reply-r1]\nhello\n[CCB_END:reply-r1]\n"
        );
    }

    #[test]
    fn clears_request_capture_on_deregister() {
        let pane = EntityId::from_usize(42);
        let mut capture = RawOutputCapture::with_max_bytes(4096);

        capture.register_pane(pane);
        capture.register_request("r1".to_string(), pane);
        capture.append(pane, b"reply");
        capture.deregister_request("r1");

        assert!(capture.snapshot("r1").is_none());
    }

    #[test]
    fn trims_to_max_bytes_without_splitting_utf8() {
        let pane = EntityId::from_usize(42);
        let mut capture = RawOutputCapture::with_max_bytes(10);

        capture.register_pane(pane);
        capture.register_request("r1".to_string(), pane);
        capture.append(pane, "一二三四五".as_bytes());

        let snapshot = capture.snapshot("r1").expect("raw capture should exist");
        assert!(snapshot.is_char_boundary(snapshot.len()));
        assert_eq!(snapshot, "三四五");
    }

    #[test]
    fn reports_stability_after_output_length_stops_changing() {
        let pane = EntityId::from_usize(42);
        let mut capture = RawOutputCapture::with_max_bytes(4096);

        capture.register_pane(pane);
        capture.register_request("r1".to_string(), pane);
        capture.append(pane, b"partial");

        assert!(!capture.is_request_stable("r1", Duration::from_millis(20)));
        std::thread::sleep(Duration::from_millis(25));
        assert!(capture.is_request_stable("r1", Duration::from_millis(20)));
    }
}

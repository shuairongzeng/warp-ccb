# Alias-Based Multi-Instance Launch Design

> **Implementation Status: COMPLETE** (2026-05-23)
>
> All layers implemented and verified:
> - Layer 1 (Python CLI): `parse_providers()`, `load_config()`, alias-based dedup
> - Layer 2 (Config file): INI-style `.warp-ccb/warp-ccb.config` support
> - Layer 3 (Bus protocol): `launch(alias=...)`, alias-based routing
> - Layer 4 (Rust): `BusLaunchedSession`, `alias_to_view_id`, `find_session()`, `WARP_CCB_PROVIDER` env var
> - Rust tests: `test_build_launch_command_alias_in_env`, `test_parse_alias_and_provider_*`

## Problem

Currently `warp-ccb` identifies each agent session solely by its provider name
(e.g., `claude`, `codex`). This means you cannot launch two instances of the
same provider — the second launch is either silently reused or rejected:

```
# Current behavior — only one codex session allowed
warp-ccb codex,codex,claude
# → [codex] already has 1 active session(s), reusing terminal pane ...
```

The user wants **role-based aliases** that map to real providers, so they can
run multiple instances of the same CLI with different identities:

```
warp-ccb writer:codex,reviewer:codex,planner:claude
```

## Requirements

1. **Config format**: `alias:provider` pairs in the positional argument
2. **Config file fallback**: `.warp-ccb/warp-ccb.config` in the project directory
3. **Environment variable**: Each pane sets `WARP_CCB_PROVIDER=writer` (the alias,
   not the real provider name) so agents know their role
4. **Sidebar display**: Warp's sidebar shows the alias name
5. **Message routing**: `warp-ask writer 'xxx'` routes to the correct pane
6. **Backward compatible**: `warp-ccb codex,claude` still works as before

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│  warp-ccb (CLI launcher)                                │
│  parse_providers() → [{alias, provider}, ...]           │
│  For each entry: launch(provider, alias=alias)          │
├─────────────────────────────────────────────────────────┤
│  bus_client.py                                          │
│  launch() → sends {"type":"launch", "provider":"codex", │
│             "alias":"writer"} to bus                    │
│  ask() → sends {"type":"ask", "provider":"writer"}      │
│  ping()/pend() → same alias-based routing               │
├─────────────────────────────────────────────────────────┤
│  Warp Rust side (LocalAgentBus)                         │
│  handle_launch() → resolves provider, stores alias      │
│  find_session() → matches by alias OR provider          │
│  bus_launched_sessions → HashMap<EntityId, AliasInfo>   │
│  inject_launch() → sets env WARP_CCB_PROVIDER=alias     │
└─────────────────────────────────────────────────────────┘
```

## Changes by Layer

### Layer 1: `ccb-bridge/bin/warp-ccb` — Provider Parsing

**Current** (`parse_providers` at line 36):
```python
def parse_providers(arg):
    providers = [p.strip().lower() for p in arg.split(",")]
    invalid = [p for p in providers if p not in VALID_PROVIDERS]
    ...
    return providers  # ["codex", "claude"]
```

**New** — return `[(alias, provider)]` tuples:
```python
def parse_providers(arg):
    """Parse 'alias:provider,provider2,...' into [(alias, provider)] pairs.

    Formats:
      writer:codex         → alias='writer', provider='codex'
      codex                → alias='codex',   provider='codex'  (no alias)
      writer:codex,reviewer:codex,planner:claude
    """
    entries = []
    for part in arg.split(","):
        token = part.strip()
        if ":" in token:
            alias, provider = token.split(":", 1)
            alias = alias.strip().lower()
            provider = provider.strip().lower()
        else:
            alias = token.lower()
            provider = alias
        if provider not in VALID_PROVIDERS:
            print(f"Error: unknown provider: {provider}", file=sys.stderr)
            sys.exit(1)
        if alias in VALID_PROVIDERS and alias != provider:
            print(f"Error: alias '{alias}' conflicts with a provider name", file=sys.stderr)
            sys.exit(1)
        entries.append((alias, provider))
    # Check for duplicate aliases
    seen = set()
    for alias, _ in entries:
        if alias in seen:
            print(f"Error: duplicate alias '{alias}'", file=sys.stderr)
            sys.exit(1)
        seen.add(alias)
    return entries
```

The `main()` loop changes from iterating `providers` to iterating
`(alias, provider)` pairs. It calls `launch(provider, alias=alias)` instead
of `launch(provider)`.

The duplicate-session check uses `alias` as the unique key instead of
`provider`, allowing multiple sessions of the same provider under different
aliases.

### Layer 2: `.warp-ccb/warp-ccb.config` — Config File

A project-level config file that provides a default alias layout:

```ini
# .warp-ccb/warp-ccb.config
[agents]
writer = codex
reviewer = codex
planner = claude
qa = claude
```

The `warp-ccb` launcher reads this when no positional argument is given,
or merges it with explicit CLI arguments:

```python
def load_config(cwd=None):
    """Load alias config from .warp-ccb/warp-ccb.config."""
    config_path = os.path.join(cwd or os.getcwd(), ".warp-ccb", "warp-ccb.config")
    if not os.path.exists(config_path):
        return []
    # Simple INI-style parsing
    entries = []
    with open(config_path) as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or line.startswith("["):
                continue
            if "=" in line:
                alias, provider = line.split("=", 1)
                entries.append((alias.strip().lower(), provider.strip().lower()))
    return entries
```

Usage:

```bash
# Uses config file
warp-ccb

# CLI overrides config file
warp-ccb writer:codex,reviewer:codex
```

### Layer 3: `ccb-bridge/lib/bus_client.py` — Bus Protocol

#### `launch()` — Add alias parameter

```python
def launch(provider, prompt=None, cwd=None, bus_info=None, auto_tab=True, alias=None):
    cmd = {
        "type": "launch",
        "provider": provider,  # real provider for agent resolution
    }
    if alias:
        cmd["alias"] = alias   # display name / routing key
    if prompt:
        cmd["prompt"] = prompt
    if cwd:
        cmd["cwd"] = cwd
    ...
```

#### `ask()`, `ping()`, `pend()` — Alias-based routing

The `provider` parameter in these functions becomes the **routing key**.
When an alias is used, it matches against sessions registered with that alias:

```python
def ask(provider, prompt, req_id=None, caller=None, ...):
    cmd = {
        "type": "ask",
        "provider": provider,  # can be alias OR real provider name
        ...
    }
```

No Python-side changes needed for ask/ping/pend — the routing happens in the
Rust bus.

### Layer 4: Warp Rust Side — Session Management

#### `normalize_provider_name()` — Parse alias:provider format

**Current** (line 2702): splits on `:` and takes the first part, which is
wrong for `alias:provider` (it would take `alias`, which isn't a valid provider).

**New** — split on `:`, check both parts:

```rust
fn normalize_provider_name(value: &str) -> Option<String> {
    let trimmed = value.trim().to_ascii_lowercase();
    // If colon-separated, the provider is after the colon
    if let Some(pos) = trimmed.find(':') {
        let provider = trimmed[pos + 1..].trim();
        return match provider {
            "claude" | "codex" | "gemini" | "opencode" | "droid"
            | "kimi" | "goose" | "copilot" | "amp" => Some(provider.to_string()),
            _ => None,
        };
    }
    // No colon — treat the whole string as a provider name
    match trimmed.as_str() {
        "claude" | "codex" | "gemini" | "opencode" | "droid"
        | "kimi" | "goose" | "copilot" | "amp" => Some(trimmed),
        _ => None,
    }
}
```

#### New: `parse_alias_and_provider()` helper

```rust
/// Parse "alias:provider" or bare "provider" into (alias, provider).
fn parse_alias_and_provider(value: &str) -> Option<(String, String)> {
    let trimmed = value.trim().to_ascii_lowercase();
    if let Some(pos) = trimmed.find(':') {
        let alias = trimmed[..pos].trim().to_string();
        let provider = trimmed[pos + 1..].trim().to_string();
        // Validate provider
        if !["claude","codex","gemini","opencode","droid","kimi","goose","copilot","amp"]
            .contains(&provider.as_str()) {
            return None;
        }
        Some((alias, provider))
    } else {
        if ["claude","codex","gemini","opencode","droid","kimi","goose","copilot","amp"]
            .contains(&trimmed.as_str()) {
            Some((trimmed.clone(), trimmed))
        } else {
            None
        }
    }
}
```

#### `PendingLaunch` — Add alias field

```rust
struct PendingLaunch {
    provider: String,     // real provider name (e.g., "codex")
    alias: Option<String>, // display alias (e.g., "writer"), None if no alias
    agent: CLIAgent,
    prompt: Option<String>,
    cwd: Option<String>,
}
```

#### `bus_launched_sessions` — Store alias info with dedicated index

Change from `HashMap<EntityId, CLIAgent>` to:

```rust
struct BusLaunchedSession {
    agent: CLIAgent,
    provider: String,     // real provider
    alias: Option<String>, // display alias
}
bus_launched_sessions: HashMap<EntityId, BusLaunchedSession>,

// Dedicated O(1) alias lookup (from Kimi's review)
alias_to_view_id: HashMap<String, EntityId>,
```

The `alias_to_view_id` map provides fast routing without iterating all sessions.
On session deregistration, clean up both maps:
```rust
fn deregister_terminal_handle(&mut self, view_id: EntityId) {
    self.alias_to_view_id.retain(|_, v| *v != view_id);
    self.bus_launched_sessions.remove(&view_id);
    // ... rest unchanged
}
```

#### `handle_launch()` — Accept alias in protocol

The bus protocol `Launch` command gets a new optional `alias` field:

```rust
// In protocol.rs
struct LaunchCommand {
    provider: String,
    alias: Option<String>,  // NEW
    prompt: Option<String>,
    cwd: Option<String>,
    ...
}
```

`handle_launch()` passes the alias through to `PendingLaunch` and stores it
in `bus_launched_sessions`.

#### `build_launch_command()` — Set env var

The agent startup command injects the alias as `WARP_CCB_PROVIDER`:

```rust
fn build_launch_command(agent: &CLIAgent, provider: &str, alias: Option<&str>, ...) -> String {
    let display_name = alias.unwrap_or(provider);
    // Set WARP_CCB_PROVIDER to the alias (or provider if no alias)
    let env_var = format!("WARP_CCB_PROVIDER={}", display_name);
    ...
}
```

This is the key: each pane gets `WARP_CCB_PROVIDER=writer` instead of
`WARP_CCB_PROVIDER=codex`, so Warp's sidebar displays the alias.

#### `find_session()` — Match by alias with O(1) lookup

Current logic matches by `resolve_agent(provider)`. New logic:

```rust
fn find_session(&mut self, name: &str, ...) -> FindSessionResult {
    // Path 0: O(1) alias lookup via dedicated index
    if let Some(&view_id) = self.alias_to_view_id.get(name) {
        if self.terminal_handles.contains_key(&view_id) {
            return FindSessionResult::Found { ... };
        }
    }
    // Path 1: Fallback to agent-type matching (backward compat)
    let target_agent = match resolve_agent(name) {
        Some(a) => a,
        None => return FindSessionResult::NotFound,
    };
    // ... existing provider-based matching ...
}
```

#### `handle_list_sessions()` — Include alias in response

The `SessionInfo` struct gets an optional `alias` field:

```rust
struct SessionInfo {
    provider: String,
    alias: Option<String>,  // NEW
    terminal_view_id: u64,
    session_id: Option<String>,
    cwd: Option<String>,
    online: bool,
}
```

### Layer 5: Tab Config Integration

When using aliases, `warp-ccb` can auto-generate a tab config with alias-named
panes:

```toml
# Auto-generated for: warp-ccb writer:codex,reviewer:codex,planner:claude
name = "ccb_aliases"
title = "Writer / Reviewer / Planner"

[[panes]]
id = "root"
split = "vertical"
children = ["col_left", "col_right"]

[[panes]]
id = "col_left"
type = "terminal"
directory = "{{cwd}}"

[[panes]]
id = "col_right"
split = "horizontal"
children = ["reviewer", "planner"]

[[panes]]
id = "reviewer"
type = "terminal"
directory = "{{cwd}}"

[[panes]]
id = "planner"
type = "terminal"
directory = "{{cwd}}"
```

The layout is auto-selected based on the number of panes (2 → pair, 3 → team_3,
4+ → team_4 grid).

## Bus Protocol Changes

### Launch Command (v2)

```json
{"type": "launch", "provider": "codex", "alias": "writer", "prompt": "..."}
```

The `alias` field is optional. When absent, behavior is identical to v1.

### Session Response

```json
{"provider": "codex", "alias": "writer", "terminal_view_id": 123, "online": true}
```

### Ask/Ping/Pend — Alias Routing

```json
{"type": "ask", "provider": "writer", "prompt": "..."}
```

The bus first checks `bus_launched_sessions` for an alias match. If not found,
falls back to `resolve_agent()` for backward compatibility.

## Migration Path

1. **Phase 1 — Python-only changes** (no Rust changes needed):
   - `warp-ccb` parses `alias:provider` format
   - Calls `launch(provider)` as before (alias ignored by bus)
   - Each pane gets its own terminal, differentiated by position
   - `warp-ask` can target panes by terminal_view_id
   - Limitation: same-provider sessions can't be distinguished by name

2. **Phase 2 — Rust changes** (full alias support):
   - Add `alias` field to bus protocol
   - Modify `PendingLaunch`, `bus_launched_sessions`, `find_session()`
   - `WARP_CCB_PROVIDER` env var set to alias
   - Sidebar shows alias names
   - `warp-ask` routes by alias

## Example Usage

```bash
# Create config file
mkdir -p .warp-ccb
cat > .warp-ccb/warp-ccb.config << 'EOF'
[agents]
writer = codex
reviewer = codex
planner = claude
EOF

# Launch with config
warp-ccb

# Or specify inline
warp-ccb writer:codex,reviewer:codex,planner:claude

# Route messages by alias
warp-ask writer "Write a function to parse TOML"
warp-ask reviewer "Review the PR for security issues"
warp-ping writer
warp-pend reviewer

# Inside each agent pane:
# $env:WARP_CCB_PROVIDER = 'writer'  (not 'codex')
# Sidebar shows: "writer", "reviewer", "planner"
```

## Files to Modify

| File | Change | Status |
|------|--------|--------|
| `ccb-bridge/bin/warp-ccb` | `parse_providers()` returns `[(alias, provider)]`; `main()` uses alias for dedup | **DONE** |
| `ccb-bridge/lib/bus_client.py` | `launch()` gains `alias=` param | **DONE** |
| `warp/app/src/ai/local_agent_bus/mod.rs` | `handle_launch()` accepts alias; `find_session()` matches by alias; `PendingLaunch` gets alias field; `build_launch_command()` sets env var | **DONE** |
| `warp/app/src/ai/local_agent_bus/protocol.rs` | `BusCommand::Launch` gets `alias: Option<String>` | **DONE** (pre-existing) |
| `ccb-bridge/bin/warp-ask` | No change needed — provider param becomes the alias | N/A |

## Multi-Agent Review Summary

This design was reviewed by Claude, Droid, and Kimi via warp-ccb cross-agent
discussion. Key findings from each reviewer:

### Droid's Finding
Confirmed that `normalize_provider_name()` splits on `:` and takes the first
part via `.next()`. Input `writer:codex` → splits → takes `writer` → not a
valid provider → returns `None`. The Rust side will **reject** `alias:provider`
format without code changes.

### Kimi's Proposal (Full text in `tmp_ccb_reply.md`)
Kimi produced a comprehensive counter-proposal with several key differences:

1. **Encoding string vs protocol field**: Reuse `provider` field for
   `alias:provider` encoding (no new `alias` field in bus protocol). This is
   more minimal — the Rust `parse_provider_alias()` function decodes internally.
2. **Dedicated `alias_to_view_id` HashMap**: `HashMap<String, EntityId>` for
   O(1) alias lookup instead of iterating `bus_launched_sessions`.
3. **Deeper UI layer analysis**: Identified specific files —
   `terminal/view/pane_impl.rs` (`selected_cli_agent_title_for_chrome`) for
   tab title display, `CLIAgentSessionContext` for storing alias, and
   `AIConversation::set_agent_name()` for sidebar/orchestration UI.
4. **Shared session compatibility**: `CLIAgentSessionState` comes from external
   crate `session_sharing_protocol` — alias should NOT be added there. Keep
   alias local-only (Bus + UI), accept that remote shared sessions won't see
   alias names.
5. **Backward compat fallback**: If new Python sends `writer:codex` to old Warp,
   `normalize_provider_name` rejects it. Python should detect this and fallback
   to launching with plain provider name (sacrificing alias).
6. **Simpler config format**: Single-line `writer:codex, reviewer:codex` instead
   of INI-style `[agents]` section.

### Merged Design Decision

The final design below incorporates the best of both approaches:
- **Protocol**: Use the explicit `alias` field approach (from Claude's original
  design). While Kimi's encoding-string approach is more minimal, an explicit
  field is clearer for future maintainers and avoids subtle parsing edge cases.
- **Data structures**: Adopt Kimi's `alias_to_view_id` HashMap for fast lookup
  and `BusLaunchedSession` struct replacing the bare `CLIAgent` in
  `bus_launched_sessions`.
- **UI integration**: Follow Kimi's specific file-level analysis for
  `pane_impl.rs` and `CLIAgentSessionContext`.
- **Config format**: Keep INI-style for extensibility (future settings sections).

## Risks and Considerations

1. **Alias conflicts**: An alias must not match a real provider name (e.g.,
   `claude:codex` is rejected because `claude` is a valid provider). This
   prevents ambiguity in `warp-ask` routing.

2. **Session persistence**: If Warp restarts, alias info in
   `bus_launched_sessions` and `alias_to_view_id` is lost. The sessions
   re-register via terminal output scanning, but without alias info.
   - Short-term: Accept the limitation. Users re-run `warp-ccb --force`.
   - Mid-term: Persist alias in pane metadata or `~/.warp-ccb/alias-registry.json`.
   - Long-term: Store alias in `CLIAgentSessionContext` so plugin-triggered
     session rebuild can recover it.

3. **Shared session compatibility**: `CLIAgentSessionState` from external crate
   `session_sharing_protocol` must NOT be modified. Alias is local-only (Bus +
   UI). Remote shared session viewers won't see alias names — acceptable for
   local multi-agent collaboration use case.

4. **Tab config limits**: Warp tab configs define static layouts. For dynamic
   alias counts, the Python side generates appropriate TOML on the fly.

5. **Backward compatibility**: All changes are additive. The `alias` field is
   optional in the protocol. When absent, behavior is identical to current.
   Python should detect Warp version and fallback gracefully if old Warp
   rejects `alias:provider` format.

6. **Duplicate alias protection**: `warp-ccb` checks `list_sessions()` for
   existing aliases before launching. Duplicate aliases are rejected (not
   reused) to prevent routing ambiguity. Use `--force` to override.

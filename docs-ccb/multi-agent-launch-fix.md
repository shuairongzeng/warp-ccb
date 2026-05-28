# Multi-Agent Launch Fix — Batch Pane Creation

> Date: 2026-05-20 (revised v3)
> Status: In Progress (Phase 2 — Root Cause Re-diagnosis)
> Files: `warp/app/src/ai/local_agent_bus/mod.rs`, `warp/app/src/pane_group/mod.rs`

## 1. Problem

Running `warp-ccb kimi,claude,codex,droid` reports **"3 launched, 0 failed"** but only creates panes when they are **manually pre-opened**. Without pre-opened panes, auto pane creation fails silently — no new panes appear, agents never start.

## 2. Root Cause (Phase 2 Analysis)

### Root Cause 1: Missing `pane_group_handle` registration

`register_pane_group_handle()` was only called in `PaneGroup::new_with_panes_layout`. Four other constructors never registered:

| Constructor | Registration |
|------------|-------------|
| `new_with_panes_layout` | YES (only one) |
| `new_from_existing_pane` | MISSING |
| `new_for_shared_session_viewer` | MISSING |
| `new_for_conversation_transcript_viewer` | MISSING |
| `new_for_conversation_transcript_viewer_loading` | MISSING |

If the active tab was created through any missing path, `LocalAgentBus::pane_group_handle` is either `None` or points to a stale PaneGroup from a previous tab.

### Root Cause 2: `add_pane` failure silently ignored

In `add_session_in_directory` (pane_group/mod.rs:6552):
```rust
let _ = self.add_pane(direction, base_pane_id, Box::new(pane_data), true, ctx);
```

When `add_pane_with_options` → `split` fails, it cleans up the pane from `pane_contents`. But `new_pane_id` is still returned. Then `create_agent_terminal_for_bus`:
```rust
let pane_content = self.pane_contents.get(&pane_id)?; // Returns None!
```

The spawn callback sees `new_terminal = None` and logs a warning, but the HTTP response already returned success. Pending launches retry 10 times then get dropped.

### Root Cause 3 (Original, partially fixed by v1): `active_block` race

`find_idle_terminal()` rejects new panes whose shell boot output sets `active_block = true`. Fixed in v1 by spawn callback direct inject, but the underlying pane creation itself was broken.

## 3. Fix (v3)

### 3a. Fix A: Register in all PaneGroup constructors

Added `register_pane_group_handle` block to all 4 missing constructors:

```rust
let mut pane_group = Self::new_internal(..., ctx);

{
    let weak_pg = ctx.handle();
    crate::ai::local_agent_bus::LocalAgentBusModel::handle(ctx).update(ctx, |bus, _ctx| {
        bus.register_pane_group_handle(weak_pg);
    });
}

pane_group
```

Affected functions:
- `new_from_existing_pane`
- `new_for_shared_session_viewer`
- `new_for_conversation_transcript_viewer`
- `new_for_conversation_transcript_viewer_loading`

### 3b. Fix B: Detect `add_pane` failure in `create_agent_terminal_for_bus`

Added explicit check before the `pane_contents.get()`:

```rust
let pane_content = self.pane_contents.get(&pane_id);
if pane_content.is_none() {
    log::error!(
        "PaneGroup: create_agent_terminal_for_bus failed — pane {:?} not in pane_contents \
         (focused_pane={:?}, pane_count={})",
        pane_id, focused, self.pane_count()
    );
    return None;
}
```

### 3c. Diagnostic logging (all critical paths)

Enhanced logging throughout the pane creation chain:
- `handle_launch`: logs `find_idle_terminal` result, terminal_handles count, pending count, pane_creation_in_progress
- `create_terminal_pane`: logs pending count when scheduling, spawn callback state on entry
- Spawn callback: logs detailed failure reasons (no handle, inject failed, no terminal returned)
- `inject_launch_to_terminal`: logs when terminal handle not found for view_id
- `create_agent_terminal_for_bus`: logs focused pane and pane count on failure

### 3d. Previous v1/v2 changes (still in place)

- `inject_launch_to_terminal()` unified function (L2196)
- Spawn callback direct inject via `pending_launches.remove(0)` (L2167)
- `process_pending_launches()` refactored to use inject function
- `handle_launch()` immediate path uses inject function
- `find_idle_terminal()` cleanup of stale entries

## 4. Key Design Decisions

- **Fix A covers all tab types**: Every PaneGroup now registers with LocalAgentBus, regardless of how the tab was created.
- **Fix B provides observability**: Even if add_pane fails for an unexpected reason, the error log will identify the exact failure.
- **No architectural changes needed (yet)**: Fix A + Fix B should resolve the immediate issue. Fix C (LocalAgentBus tracks active tab PaneGroup) is deferred as a future improvement.
- **`inject_launch_to_terminal()` as single source of truth**: All three paths (immediate, spawn-callback, pending-queue) use the same function.
- **Spawn callback directly consumes pending**: Bypasses `find_idle_terminal()` to avoid the `active_block` race.

## 5. Discussion History

### Phase 1: Original diagnosis (10 rounds, droid+kimi)

| Round | Topic | Result |
|-------|-------|--------|
| 1–3 | Problem diagnosis | `find_idle_terminal` rejects new pane due to shell boot `active_block` |
| 4 | Solution proposals | droid: direct inject (A), kimi: bus_owned_panes (B), Claude: hybrid (C) |
| 5 | Compromise attempt | 方案D (A+B hybrid) proposed — droid rejected, kimi endorsed |
| 6 | Consensus reached | kimi conceded: "don't block shipping for architectural purity" — **方案C** selected |
| 7 | Race conditions | PTY buffer safe, `remove(0)` FIFO, no regression to original bug |
| 8 | Python changes | None needed — pure Rust fix |
| 9 | Edge cases | GPUI single-threaded (no race), MAX=10 keeps retry cap |
| 10 | Final plan | Both agents confirmed implementation plan |

### Phase 2: Root cause re-diagnosis (ongoing)

| Round | Topic | Result |
|-------|-------|--------|
| 1 | Diagnostic questions | droid: raw TUI output (unhelpful), kimi: identified missing registration + silent failure |
| 2 | Fix implementation | Claude implemented Fix A + Fix B + diagnostic logging, sent to both agents for validation |
| 3+ | Pending | Awaiting droid + kimi Round 2 responses |

## 6. Testing Plan

1. Rebuild Warp with all fixes
2. Open Warp with single tab + single pane
3. Run `warp-ccb kimi,claude,droid`
4. Expected: 3 new panes auto-created, agents launch successfully
5. Check Warp logs for diagnostic messages if any step fails

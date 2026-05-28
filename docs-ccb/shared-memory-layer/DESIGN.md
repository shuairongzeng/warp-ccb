# Shared Memory Layer (SML) — Architecture Design

## 1. Problem Statement

Currently each Agent (Claude, Codex, Gemini, etc.) maintains isolated memory:
- Session files: `.ccb_config/.claude-session`, `.codex-session`
- Terminal output grids as ephemeral "memory"
- `ResponseStore` only persists request/response pairs per Warp instance

**Gap**: There is no cross-agent, cross-session persistent memory. When Claude finishes a task, Codex cannot access Claude's analysis unless it is manually copied into the prompt.

## 2. Design Goals

1. **Unified namespace** — All agents read/write the same logical memory space keyed by `(scope, project_id, key)`.
2. **Crash-safe & concurrent** — Multiple agents (different OS processes) may read/write simultaneously.
3. **Minimal intrusion** — Reuse existing `LocalAgentBus` JSONL protocol and `persistence` SQLite infrastructure.
4. **Observable lifecycle** — TTL, compression, archival are first-class, not afterthoughts.
5. **Fine-grained ACL** — Not every agent should see everything.

## 3. Data Model

### 3.1 Storage Engine

**SQLite (WAL mode)** in `~/.warp-ccb/shared_memory.db`.

Why SQLite instead of flat files or a new server?
- Warp already depends on `crates/persistence` (Diesel + SQLite).
- WAL mode gives **readers non-blocking, writers serialized** concurrency for free.
- Single file, easy to back up / delete / inspect.

### 3.2 Schema

```sql
-- Core entries table
CREATE TABLE memory_entries (
    id              TEXT PRIMARY KEY,           -- UUID v4
    scope           TEXT NOT NULL,              -- 'global' | 'project' | 'private'
    project_id      TEXT,                       -- hash of work_dir_norm (nullable for global)
    owner           TEXT NOT NULL,              -- provider name: claude | codex | ...
    key             TEXT NOT NULL,              -- logical key: e.g. "task-context", "bug-42-analysis"
    content         TEXT NOT NULL,              -- payload (JSON, markdown, free text)
    content_type    TEXT NOT NULL DEFAULT 'text', -- 'text' | 'json' | 'markdown'
    version         INTEGER NOT NULL DEFAULT 1, -- optimistic locking
    created_at_ms   INTEGER NOT NULL,
    updated_at_ms   INTEGER NOT NULL,
    ttl_secs        INTEGER,                    -- NULL = never expires
    access_count    INTEGER NOT NULL DEFAULT 0,
    last_accessed_ms INTEGER NOT NULL,
    size_bytes      INTEGER NOT NULL DEFAULT 0, -- len(content) for quota tracking
    compressed      INTEGER NOT NULL DEFAULT 0, -- 0 = raw, 1 = summary-replaced
    UNIQUE(scope, project_id, key)
);

-- Tag index (many-to-many lite)
CREATE TABLE memory_tags (
    entry_id        TEXT NOT NULL REFERENCES memory_entries(id) ON DELETE CASCADE,
    tag             TEXT NOT NULL,
    PRIMARY KEY (entry_id, tag)
);

-- Access audit log (append-only, rotated manually)
CREATE TABLE memory_access_log (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    entry_id        TEXT NOT NULL,
    actor           TEXT NOT NULL,              -- provider or "system"
    action          TEXT NOT NULL,              -- 'read' | 'write' | 'delete' | 'compress' | 'archive'
    timestamp_ms    INTEGER NOT NULL
);

-- Project registry (for list-projects API)
CREATE TABLE memory_projects (
    project_id      TEXT PRIMARY KEY,
    friendly_name   TEXT,
    created_at_ms   INTEGER NOT NULL,
    last_active_ms  INTEGER NOT NULL
);
```

### 3.3 Logical Key Naming Convention

Agents are encouraged (not enforced) to use hierarchical dot-separated keys:

| Key Pattern | Example | Purpose |
|-------------|---------|---------|
| `ctx.{task}` | `ctx.rust-refactor` | Current task context |
| `analysis.{id}` | `analysis.bug-1442` | Structured analysis output |
| `decision.{id}` | `decision.adr-003` | Architecture decision record |
| `note.{topic}` | `note.dependencies` | Free-form shared notes |
| `scratch.{agent}` | `scratch.claude` | Private working space (scope=private) |

## 4. Read/Write Protocol

### 4.1 Bus Command Extensions

Extend `BusCommand` in `local_agent_bus/protocol.rs`:

```rust
pub enum BusCommand {
    // ... existing variants ...

    MemoryWrite { entry: MemoryEntryInput },
    MemoryRead  { id: Option<String>, key: String, scope: MemoryScope, project_id: Option<String> },
    MemoryQuery { filter: MemoryFilter, limit: usize, offset: usize },
    MemoryDelete { id: String, expected_version: Option<u32> },
    MemoryListProjects,
}
```

### 4.2 Request/Response Flow

```
Agent A (Claude)          LocalAgentBus           SQLite SML
     |                          |                      |
     |-- MemoryWrite ---------->|                      |
     |                          |-- ACL check -------->|
     |                          |-- INSERT/UPDATE ---->|
     |                          |<-- row --------------|
     |<-- MemoryWriteResult ----|                      |
     |                          |                      |
     |                          |<-- TTL cleanup ------| (background)
```

### 4.3 Conflict Avoidance — Optimistic Locking

Every `MemoryWrite` carries `expected_version`. If the stored version differs, the bus returns `Conflict { stored_version, stored_entry }`. The caller can:
1. **Retry** with merged content (recommended for agents).
2. **Abort** and notify the user.
3. **Force** with `expected_version = None` (requires `admin` role).

### 4.4 Batch Operations

For efficiency, `MemoryQuery` supports bulk reads. Agents should prefer querying multiple entries in one round-trip rather than N individual `MemoryRead` calls.

## 5. Synchronization Mechanism

### 5.1 Database Level

- **SQLite WAL mode**: Multiple readers, single writer. No file-level locks needed.
- **Busy timeout**: `PRAGMA busy_timeout = 5000` (5s) so writers queue gracefully.

### 5.2 Application Level

- **Optimistic versioning**: `version` column incremented on every write.
- **Atomic compare-and-swap**: `UPDATE ... WHERE version = expected_version`.
- **No distributed locks**: Because all agents connect to the same LocalAgentBus instance (per Warp process), the bus serializes writes via its single `mpsc` channel to the main thread.

### 5.3 Multi-Warp-Instance Scenario

If multiple Warp terminals run on the same machine, each has its own SQLite file (`shared_memory-{pid}.db`). Cross-instance sharing is out of scope for Phase 1; instances can share via the existing `warp-ask` bridge if needed.

## 6. Permission Model

### 6.1 Scope-Based ACL

| Scope | Read | Write | Delete |
|-------|------|-------|--------|
| `global` | Any authenticated agent | Owner or `admin` role | Owner or `admin` |
| `project` | Same `project_id` | Same `project_id` | Owner or `admin` |
| `private` | Owner only | Owner only | Owner only |

### 6.2 Role Resolution

```rust
pub enum MemoryRole {
    Reader,   // can read project/global
    Writer,   // can read + write project/global
    Admin,    // can do everything including force-overwrite and delete others
}
```

Role is derived from `(caller_provider, entry.owner, entry.scope, entry.project_id)` at runtime. No separate user table is needed.

### 6.3 Project ID

Project ID is the `ccb_project_id` hash already present in session files (SHA-256 of normalized work dir). This ensures agents working in the same directory automatically share `project` scope memory.

## 7. Lifecycle Management

### 7.1 States

```
[Active] --(ttl expired)--> [Expired] --(cleanup job)--> [Deleted]
   |
   +--(cold: 30d no access)--> [Archived] --(gzip JSONL)--> [Archive File]
   |
   +--(large & old)--> [Compressed] --(summarize)--> [Active but smaller]
```

### 7.2 TTL & Expiration

- Writers may set `ttl_secs` (e.g., 86400 for 24h).
- Every write triggers a **lazy cleanup**: `DELETE FROM memory_entries WHERE ttl_secs IS NOT NULL AND updated_at_ms + ttl_secs * 1000 < now()`.
- A **background timer** (every 5 min) also runs cleanup.

### 7.3 Compression

Trigger: `access_count < 3 AND age > 7 days AND size_bytes > 4096 AND compressed = 0`.

Action: Replace `content` with an AI-generated summary (max 512 chars), set `compressed = 1`. Original full text is moved to the archive file for forensic recovery.

Implementation note: In Phase 1, "AI-generated summary" can be a naive truncation + ellipsis. A future Phase 2 can integrate the Warp AI subsystem for real summarization.

### 7.4 Archival

Trigger: `last_accessed_ms < now - 30 days`.

Action:
1. Append entry to `~/.warp-ccb/archives/{project_id}/{yyyy-mm}.jsonl.gz`.
2. Delete from SQLite.
3. Log to `memory_access_log`.

### 7.5 Quotas

Per-project soft quota: 10 MB of `size_bytes`. When exceeded, the lifecycle manager compresses oldest entries first, then archives.

## 8. Directory Structure

```
warp-ccb/
├── docs/shared-memory-layer/
│   └── DESIGN.md
│
├── ccb-bridge/lib/
│   ├── bus_client.py              (extended with memory_* helpers)
│   └── shared_memory.py           (new: idiomatic Python client)
│
├── warp/app/src/ai/
│   ├── local_agent_bus/
│   │   ├── protocol.rs            (extended BusCommand/BusResponseData)
│   │   └── mod.rs                 (wires Memory commands into handle_command)
│   └── shared_memory/
│       ├── mod.rs                 (public API: SharedMemoryLayer)
│       ├── store.rs               (SQLite CRUD + migrations)
│       ├── sync.rs                (optimistic locking + CAS)
│       ├── acl.rs                 (permission checks)
│       ├── lifecycle.rs           (TTL, compression, archive, quota)
│       └── protocol.rs            (MemoryEntry, MemoryScope, MemoryFilter)
│
└── warp/crates/persistence/migrations/
    └── 2026-05-14-000000_add_shared_memory/
        ├── up.sql
        └── down.sql
```

## 9. Integration Points

### 9.1 LocalAgentBus Wiring

In `local_agent_bus/mod.rs`, add a `shared_memory: SharedMemoryLayer` field to `LocalAgentBusModel`. New `BusCommand` variants are dispatched to `shared_memory.handle_*` methods. Results are wrapped in `BusResponseData::Memory*Result`.

### 9.2 Session Auto-Project Binding

When `LocalAgentBus` handles an `Ask` command, it extracts `cwd` from the request. The `SharedMemoryLayer` derives `project_id` from the normalized cwd hash (same algorithm as `.ccb_config/.claude-session`). This allows agents to read project-scoped memory without explicitly passing `project_id`.

### 9.3 Python Skill Helpers

The `warp-ask` skill for Claude and the equivalent for Codex will expose:
```python
# Injected into agent context automatically
ccb_memory_write(key, content, scope="project", ttl_secs=None)
ccb_memory_read(key, scope="project")
ccb_memory_query(tag=None, limit=10)
```

## 10. Failure Modes & Mitigations

| Scenario | Mitigation |
|----------|------------|
| SQLite locked too long | `busy_timeout = 5000`; bus returns `TemporarilyUnavailable` |
| Disk full | Lifecycle manager aggressively archives; writes return `QuotaExceeded` |
| Version conflict | `Conflict` response with 3-way merge suggestion (Phase 2) |
| Corrupt DB | Detect on open; rename to `.db.bak` and re-create empty |
| Agent impersonation | Auth token from `bus-address.json` already validates all bus clients |

## 11. Phase Plan

| Phase | Scope | Deliverables |
|-------|-------|--------------|
| 1 (MVP) | SQLite store, basic CRUD, scope ACL, TTL cleanup | This design + working code |
| 2 | Optimistic lock merge, tag search, compression with AI summary | Enhanced `sync.rs`, AI hook |
| 3 | Cross-instance sync via Warp Cloud, semantic search (embeddings) | Networking + vector index |

---
*Document version: 2026-05-14*
*Authors: warp-ccb architecture team*

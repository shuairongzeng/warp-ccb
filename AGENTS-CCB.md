# Repository Guidelines

## Project Overview

**warp-ccb** is a multi-agent orchestration layer built on top of the [Warp terminal](https://github.com/warpdotdev/warp). It provides a LocalAgentBus that lets multiple CLI coding agents (Claude, Codex, Droid, Gemini, Kimi, etc.) collaborate in Warp panes. Communication uses a **SQLite WAL database** as the primary message bus (crash-safe, no Warp dependency), with optional TCP JSONL for PTY injection.

## Project Structure & Module Organization

```
warp-ccb/
├── warp/                  # Warp terminal fork (Rust workspace)
│   ├── app/               # Main application crate
│   ├── crates/            # ~40 workspace crates (warp_core, warpui, ai, editor, etc.)
│   ├── specs/             # Feature specs (product.md + tech.md per feature)
│   ├── script/            # Build and CI scripts (bootstrap, presubmit, run)
│   └── tests/             # Test assets
├── ccb-bridge/            # Python orchestration layer
│   ├── bin/               # CLI entry points (warp-ccb, warp-ask, warp-reply, etc.)
│   ├── lib/               # Core library
│   │   ├── bus_client.py  # TCP JSONL socket client (legacy, used for PTY inject)
│   │   ├── bus_sqlite.py  # SQLite message bus (primary — ask/reply/wait/notify)
│   │   └── bus_notifier.py # Watch + push notification layer
│   ├── tab_config_templates/  # TOML tab config templates for agent teams
│   ├── test_bus.py        # End-to-end TCP bus protocol tests (requires Warp)
│   ├── test_bus_sqlite.py # SQLite bus unit tests (no Warp required)
│   └── test_tab_configs.py    # Tab config validation tests
├── docs/                  # Design docs (passive-capture, shared-memory-layer, etc.)
├── .ccb_config/           # Per-project session state (agent sessions)
└── .spec-workflow/        # Spec workflow state (approvals, archive, steering)
```

## Build, Test, and Development Commands

### Warp (Rust)

| Command | Description |
|---|---|
| `cd warp && ./script/bootstrap` | Platform-specific setup and dependency install |
| `cd warp && cargo run` | Build and run Warp |
| `cd warp && ./script/presubmit` | Run fmt, clippy, and tests (required before PRs) |
| `cd warp && cargo nextest run` | Run unit tests with nextest |
| `cd warp && cargo clippy --workspace --all-targets --all-features --tests -- -D warnings` | Lint |

### CCB-Bridge (Python)

| Command | Description |
|---|---|
| `python ccb-bridge/test_bus_sqlite.py` | SQLite bus unit tests (no Warp required) |
| `python ccb-bridge/test_bus.py` | TCP bus protocol tests (requires Warp running) |
| `python ccb-bridge/test_tab_configs.py` | Validate tab config templates |
| `ccb-bridge/bin/warp-ccb <providers>` | Launch agent sessions (e.g., `warp-ccb claude,droid`) |

No Python package manager is used — dependencies are limited to the standard library.

## Coding Style & Naming Conventions

### Rust (warp/)
- Rust edition 2018, toolchain `1.92.0` with `rustfmt` and `clippy` components.
- Run `cargo fmt` before committing; `cargo clippy` must pass with zero warnings.
- Prefer imports over path qualifiers, inline format args (`"{x}"`), and exhaustive `match` over `_` wildcards.
- Use `instant::Instant` instead of `std::time::Instant` (WASM compatibility).
- Use `command::blocking::Command` instead of `std::process::Command` (Windows console flash prevention).
- See `warp/.clippy.toml` for the full list of disallowed macros, types, and methods.

### Python (ccb-bridge/)
- Python 3.7+ compatible, no external dependencies.
- CLI entry points in `bin/` are executable scripts with shebangs.
- Shared logic lives in `lib/bus_sqlite.py` (primary) and `lib/bus_client.py` (TCP legacy).
- All CLI tools accept `--mode <sqlite|tcp|dual>` to select the communication mode. Default is `dual` for `warp-ask`/`warp-reply` and `sqlite` for `warp-wait`/`warp-pend`.

## Testing Guidelines

- **Rust:** Bug fixes require regression tests. Non-trivial logic needs unit tests. User-facing flows should have integration tests under `crates/integration/`. Run `cargo nextest run` for unit tests; integration tests use a custom Builder/TestStep framework.
- **Python:** Tests are standalone scripts (`test_bus.py`, `test_tab_configs.py`). Bus tests require a running Warp instance.

## Commit & Pull Request Guidelines

- **Branch naming:** Prefix with your handle (e.g., `alice/fix-parser`).
- **Commit messages:** Explain *what* and *why*.
- **PRs:** Use the PR template. Add a changelog entry (`CHANGELOG-NEW-FEATURE`, `CHANGELOG-IMPROVEMENT`, or `CHANGELOG-BUG-FIX`) unless docs/refactor only.
- **Specs first:** Feature requests require `product.md` + `tech.md` under `specs/GH<issue-number>/` before code. Bug fixes are implicitly ready to implement.
- **Review:** Oz (automated reviewer) reviews first; human SME review follows Oz approval. Comment `/oz-review` for re-reviews (up to 3 per PR).

## Architecture Notes

- The **LocalAgentBus** supports two communication channels:
  - **SQLite bus** (primary): `bus_sqlite.py` provides a WAL-mode SQLite database for persistent messaging. Agents communicate via `ask`/`reply`/`wait`/`cancel` operations that write to `bus_messages` and `bus_notifications` tables. When a reply is written, a `reply_ready` notification triggers push delivery to the requesting agent — no polling needed. No Warp dependency.
  - **TCP JSONL bus** (legacy): `bus_client.py` connects to Warp's TCP server for PTY prompt injection. Used when Warp is running and agents need their prompts injected into terminal panes.
  - **Dual mode**: Both channels are written simultaneously for maximum reliability.
- **Tab configs** (`ccb-bridge/tab_config_templates/`) define agent team compositions in TOML (e.g., 2-agent pairs, 3-agent teams).
- **Session discovery** uses SQLite `bus_agents` table for online status, with `bus-address.json` files as fallback for TCP instances.
- **Database location**: `~/.warp-ccb/bus-<project_hash>.db` (per-project isolation).

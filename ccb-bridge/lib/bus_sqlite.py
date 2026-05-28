"""
bus_sqlite.py — SQLite-based message bus for multi-agent communication

Replaces the fragile TCP JSONL protocol with a crash-safe SQLite WAL database.
Messages are persisted to disk immediately; a notification table enables
watch-based push delivery so the asking agent never needs to poll.

Database location:
  Windows: %LOCALAPPDATA%\\warp-ccb\\bus-<project_hash>.db
  Unix:    ~/.warp-ccb/bus-<project_hash>.db

Python 3.7+, no external dependencies (sqlite3 is stdlib).
"""

import json
import os
import sqlite3
import sys
import time
import uuid

if sys.platform == "win32":
    _BUS_DIR = os.path.join(
        os.environ.get("LOCALAPPDATA", os.path.expanduser("~\\AppData\\Local")),
        "warp-ccb",
    )
else:
    _BUS_DIR = os.path.expanduser("~/.warp-ccb")

SCHEMA_VERSION = 1

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS bus_messages (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    req_id          TEXT    NOT NULL,
    msg_type        TEXT    NOT NULL,   -- 'ask' | 'reply' | 'cancel'
    from_agent      TEXT    NOT NULL,
    to_agent        TEXT    NOT NULL,
    content         TEXT    NOT NULL DEFAULT '',
    status          TEXT    NOT NULL DEFAULT 'pending',
    priority        INTEGER NOT NULL DEFAULT 0,
    created_at      REAL    NOT NULL,
    updated_at      REAL    NOT NULL,
    delivered_at    REAL,
    done_at         REAL,

    caller_cwd          TEXT,
    caller_session_id   TEXT,
    caller_view_id      INTEGER,
    source              TEXT,
    confidence          REAL,

    UNIQUE(req_id, msg_type)
);

CREATE INDEX IF NOT EXISTS idx_msg_to_status
    ON bus_messages(to_agent, status);
CREATE INDEX IF NOT EXISTS idx_msg_from_status
    ON bus_messages(from_agent, status);
CREATE INDEX IF NOT EXISTS idx_msg_req_id
    ON bus_messages(req_id);

CREATE TABLE IF NOT EXISTS bus_agents (
    agent_name      TEXT PRIMARY KEY,
    real_provider   TEXT    NOT NULL,
    view_id         INTEGER,
    session_id      TEXT,
    status          TEXT    NOT NULL DEFAULT 'offline',
    cwd             TEXT,
    last_heartbeat  REAL,
    pid             INTEGER,
    started_at      REAL
);

CREATE TABLE IF NOT EXISTS bus_notifications (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    req_id          TEXT    NOT NULL,
    agent_name      TEXT    NOT NULL,
    notif_type      TEXT    NOT NULL,   -- 'new_request' | 'reply_ready' | 'cancelled'
    content_preview TEXT    NOT NULL DEFAULT '',
    created_at      REAL    NOT NULL,
    consumed        INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_notif_agent_consumed
    ON bus_notifications(agent_name, consumed);

CREATE TABLE IF NOT EXISTS bus_meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);
"""


def _project_hash(cwd=None):
    import hashlib
    path = (cwd or os.getcwd()).lower().replace("\\", "/").replace("//", "/")
    return hashlib.sha256(path.encode()).hexdigest()[:12]


def _default_db_path(cwd=None):
    os.makedirs(_BUS_DIR, exist_ok=True)
    # Use a single shared database (not per-project) so all agents
    # and CLI tools see the same data regardless of cwd.
    return os.path.join(_BUS_DIR, "bus.db")


def _now():
    return time.time()


# ------------------------------------------------------------------
# Alias resolution
# ------------------------------------------------------------------

_VALID_PROVIDERS = {"claude", "codex", "gemini", "opencode", "droid", "kimi", "goose", "copilot", "amp"}

_alias_cache = {"_time": 0.0, "data": {}}


def _load_alias_config():
    """Load alias→provider mapping from .warp-ccb/warp-ccb.config.

    Returns dict like {"writer": "kimi", "reviewer": "droid", ...}
    Caches for 5 seconds to avoid repeated file I/O.
    """
    now = _now()
    if now - _alias_cache["_time"] < 5.0 and _alias_cache["data"]:
        return _alias_cache["data"]

    aliases = {}
    # Try CWD first, then parent directories
    search_dir = os.getcwd()
    for _ in range(10):
        config_path = os.path.join(search_dir, ".warp-ccb", "warp-ccb.config")
        if os.path.exists(config_path):
            try:
                in_agents = False
                with open(config_path, encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line or line.startswith("#"):
                            continue
                        if line.startswith("["):
                            in_agents = line.lower().strip("[]") == "agents"
                            continue
                        if in_agents and "=" in line:
                            alias, provider = line.split("=", 1)
                            alias = alias.strip().lower()
                            provider = provider.strip().lower()
                            if provider in _VALID_PROVIDERS and alias not in _VALID_PROVIDERS:
                                aliases[alias] = provider
                break
            except Exception:
                break
        parent = os.path.dirname(search_dir)
        if parent == search_dir:
            break
        search_dir = parent

    _alias_cache["_time"] = now
    _alias_cache["data"] = aliases
    return aliases


def resolve_alias(name):
    """Resolve an alias to (real_provider, alias_name) or return (name, None).

    If `name` is a known provider, returns (name, None).
    If `name` is an alias (e.g. "writer"), returns ("kimi", "writer").
    If `name` is neither, returns (name, None).
    """
    if name in _VALID_PROVIDERS:
        return name, None
    aliases = _load_alias_config()
    if name in aliases:
        return aliases[name], name
    return name, None


class BusDB:
    """Thin wrapper around the SQLite bus database."""

    def __init__(self, db_path=None, cwd=None):
        self.db_path = db_path or _default_db_path(cwd)
        self.cwd = cwd or os.getcwd()
        self._conn = sqlite3.connect(self.db_path, timeout=10, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        self._init_schema()

    def _init_schema(self):
        # Always run CREATE TABLE IF NOT EXISTS first (idempotent)
        self._conn.executescript(_SCHEMA_SQL)
        self._conn.commit()

        cur = self._conn.execute(
            "SELECT value FROM bus_meta WHERE key='schema_version'"
        )
        row = cur.fetchone()
        current = int(row["value"]) if row else 0
        if current < SCHEMA_VERSION:
            self._conn.execute(
                "INSERT OR REPLACE INTO bus_meta (key, value) VALUES ('schema_version', ?)",
                (str(SCHEMA_VERSION),),
            )
            self._conn.commit()

    def close(self):
        self._conn.close()

    # ------------------------------------------------------------------
    # Messages
    # ------------------------------------------------------------------

    def ask(self, provider, prompt, req_id=None, caller=None,
            caller_cwd=None, caller_session_id=None, caller_view_id=None,
            priority=0):
        """Send a request to another agent. Returns req_id."""
        if req_id is None:
            ts = time.strftime("%Y%m%d-%H%M%S")
            req_id = f"{ts}-{uuid.uuid4().hex[:8]}"
        caller = caller or _detect_caller()
        now = _now()
        content = prompt

        with self._conn:
            self._conn.execute(
                """INSERT INTO bus_messages
                   (req_id, msg_type, from_agent, to_agent, content,
                    status, priority, created_at, updated_at,
                    caller_cwd, caller_session_id, caller_view_id)
                   VALUES (?, 'ask', ?, ?, ?, 'pending', ?, ?, ?, ?, ?, ?)""",
                (req_id, caller, provider, content,
                 priority, now, now,
                 caller_cwd or self.cwd, caller_session_id, caller_view_id),
            )
            self._conn.execute(
                """INSERT INTO bus_notifications
                   (req_id, agent_name, notif_type, content_preview, created_at)
                   VALUES (?, ?, 'new_request', ?, ?)""",
                (req_id, provider, content[:500], now),
            )
        return req_id

    def reply(self, req_id, content, caller=None, source=None, confidence=None):
        """Submit a reply for a pending request."""
        caller = caller or _detect_caller()
        now = _now()

        # Find the original ask to determine the target (from_agent)
        row = self._conn.execute(
            "SELECT from_agent FROM bus_messages WHERE req_id=? AND msg_type='ask'",
            (req_id,),
        ).fetchone()
        if not row:
            raise ValueError(f"No ask found for req_id={req_id}")
        target = row["from_agent"]

        with self._conn:
            self._conn.execute(
                """INSERT INTO bus_messages
                   (req_id, msg_type, from_agent, to_agent, content,
                    status, created_at, updated_at, done_at,
                    source, confidence)
                   VALUES (?, 'reply', ?, ?, ?, 'done', ?, ?, ?, ?, ?)""",
                (req_id, caller, target, content, now, now, now,
                 source, confidence),
            )
            self._conn.execute(
                """UPDATE bus_messages
                   SET status='done', done_at=?, updated_at=?
                   WHERE req_id=? AND msg_type='ask'""",
                (now, now, req_id),
            )
            self._conn.execute(
                """INSERT INTO bus_notifications
                   (req_id, agent_name, notif_type, content_preview, created_at)
                   VALUES (?, ?, 'reply_ready', ?, ?)""",
                (req_id, target, content[:500], now),
            )
        return True

    def cancel(self, req_id, caller=None):
        """Cancel a pending request."""
        caller = caller or _detect_caller()
        now = _now()
        row = self._conn.execute(
            "SELECT to_agent FROM bus_messages WHERE req_id=? AND msg_type='ask' AND status='pending'",
            (req_id,),
        ).fetchone()
        if not row:
            return False
        target = row["to_agent"]

        with self._conn:
            self._conn.execute(
                """UPDATE bus_messages
                   SET status='cancelled', updated_at=?
                   WHERE req_id=? AND msg_type='ask'""",
                (now, req_id),
            )
            self._conn.execute(
                """INSERT INTO bus_notifications
                   (req_id, agent_name, notif_type, content_preview, created_at)
                   VALUES (?, ?, 'cancelled', '', ?)""",
                (req_id, target, now),
            )
        return True

    def get_request(self, req_id):
        """Get the ask message for a req_id."""
        row = self._conn.execute(
            "SELECT * FROM bus_messages WHERE req_id=? AND msg_type='ask'",
            (req_id,),
        ).fetchone()
        return dict(row) if row else None

    def get_reply(self, req_id):
        """Get the reply message for a req_id, or None if not ready."""
        row = self._conn.execute(
            "SELECT * FROM bus_messages WHERE req_id=? AND msg_type='reply'",
            (req_id,),
        ).fetchone()
        return dict(row) if row else None

    def poll_requests(self, agent_name, limit=10, status="pending"):
        """Get pending (or other status) requests for an agent."""
        rows = self._conn.execute(
            """SELECT * FROM bus_messages
               WHERE to_agent=? AND msg_type='ask' AND status=?
               ORDER BY priority DESC, created_at ASC LIMIT ?""",
            (agent_name, status, limit),
        ).fetchall()
        return [dict(r) for r in rows]

    def mark_delivered(self, req_id):
        """Mark a request as delivered (agent has seen it)."""
        now = _now()
        self._conn.execute(
            "UPDATE bus_messages SET status='delivered', delivered_at=?, updated_at=? WHERE req_id=? AND msg_type='ask'",
            (now, now, req_id),
        )
        self._conn.commit()

    def pend(self, agent_name, count=1, req_id=None):
        """Get replies addressed to an agent (i.e. where to_agent matches).

        A reply's to_agent is the original ask's from_agent (the caller).
        So `pend('claude')` returns replies where to_agent='claude',
        meaning replies sent back TO claude from other agents.
        """
        if req_id:
            rows = self._conn.execute(
                """SELECT * FROM bus_messages
                   WHERE to_agent=? AND msg_type='reply' AND req_id=?
                   ORDER BY created_at DESC LIMIT ?""",
                (agent_name, req_id, count),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """SELECT * FROM bus_messages
                   WHERE to_agent=? AND msg_type='reply'
                   ORDER BY created_at DESC LIMIT ?""",
                (agent_name, count),
            ).fetchall()
        return [dict(r) for r in rows]

    def wait(self, req_id, timeout=300):
        """Block until a reply is available, then return it."""
        deadline = _now() + timeout
        poll_interval = 0.3
        while _now() < deadline:
            reply = self.get_reply(req_id)
            if reply:
                ask = self.get_request(req_id)
                elapsed_ms = int((reply["done_at"] - ask["created_at"]) * 1000) if ask and reply.get("done_at") else 0
                return {
                    "ok": True,
                    "req_id": req_id,
                    "status": reply["status"],
                    "content": reply["content"],
                    "elapsed_ms": elapsed_ms,
                    "source": reply.get("source"),
                    "confidence": reply.get("confidence"),
                }
            remaining = deadline - _now()
            time.sleep(min(poll_interval, max(0.05, remaining)))
        return {"ok": False, "message": "timeout"}

    # ------------------------------------------------------------------
    # Notifications
    # ------------------------------------------------------------------

    def get_notifications(self, agent_name, limit=50, consume=True, notif_type=None):
        """Get unconsumed notifications for an agent.

        If consume=True, marks them as consumed after reading.
        If notif_type is given (e.g. 'reply_ready'), only rows of that type are
        returned AND consumed — other types are left untouched. Callers that
        only care about replies MUST pass notif_type='reply_ready' to avoid
        silently eating new_request notifications meant for the agent's worker.
        Returns list of notification dicts.
        """
        if notif_type is not None:
            rows = self._conn.execute(
                """SELECT * FROM bus_notifications
                   WHERE agent_name=? AND consumed=0 AND notif_type=?
                   ORDER BY id ASC LIMIT ?""",
                (agent_name, notif_type, limit),
            ).fetchall()
        else:
            rows = self._conn.execute(
                """SELECT * FROM bus_notifications
                   WHERE agent_name=? AND consumed=0
                   ORDER BY id ASC LIMIT ?""",
                (agent_name, limit),
            ).fetchall()
        results = [dict(r) for r in rows]
        if consume and results:
            ids = ",".join(str(r["id"]) for r in results)
            self._conn.execute(
                f"UPDATE bus_notifications SET consumed=1 WHERE id IN ({ids})"
            )
            self._conn.commit()
        return results

    def watch(self, agent_name, callback, poll_interval=0.5, stop_event=None):
        """Watch for new notifications and invoke callback for each.

        callback(notification_dict) is called for every new notification.
        Blocks until stop_event is set (if provided).
        """
        import threading
        if stop_event is None:
            stop_event = threading.Event()

        while not stop_event.is_set():
            notifs = self.get_notifications(agent_name, consume=True)
            for n in notifs:
                callback(n)
            stop_event.wait(poll_interval)

    # ------------------------------------------------------------------
    # Agent registration
    # ------------------------------------------------------------------

    def register_agent(self, agent_name, real_provider, view_id=None,
                       session_id=None, cwd=None, pid=None):
        """Register an agent as online."""
        now = _now()
        self._conn.execute(
            """INSERT INTO bus_agents
               (agent_name, real_provider, view_id, session_id,
                status, cwd, last_heartbeat, pid, started_at)
               VALUES (?, ?, ?, ?, 'online', ?, ?, ?, ?)
               ON CONFLICT(agent_name) DO UPDATE SET
                   real_provider=excluded.real_provider,
                   view_id=excluded.view_id,
                   session_id=excluded.session_id,
                   status='online',
                   cwd=excluded.cwd,
                   last_heartbeat=excluded.last_heartbeat,
                   pid=excluded.pid""",
            (agent_name, real_provider, view_id, session_id,
             cwd or self.cwd, now, pid, now),
        )
        self._conn.commit()

    def deregister_agent(self, agent_name):
        """Mark an agent as offline."""
        now = _now()
        self._conn.execute(
            "UPDATE bus_agents SET status='offline', last_heartbeat=? WHERE agent_name=?",
            (now, agent_name),
        )
        self._conn.commit()

    def deregister_all(self):
        """Mark all agents as offline."""
        now = _now()
        self._conn.execute(
            "UPDATE bus_agents SET status='offline', last_heartbeat=?",
            (now,),
        )
        self._conn.commit()

    def heartbeat(self, agent_name):
        """Update the heartbeat timestamp for an agent."""
        self._conn.execute(
            "UPDATE bus_agents SET last_heartbeat=? WHERE agent_name=?",
            (_now(), agent_name),
        )
        self._conn.commit()

    def get_agent(self, agent_name):
        """Get agent info by name."""
        row = self._conn.execute(
            "SELECT * FROM bus_agents WHERE agent_name=?", (agent_name,)
        ).fetchone()
        return dict(row) if row else None

    def list_agents(self, status=None):
        """List all registered agents, optionally filtered by status."""
        if status:
            rows = self._conn.execute(
                "SELECT * FROM bus_agents WHERE status=?", (status,)
            ).fetchall()
        else:
            rows = self._conn.execute("SELECT * FROM bus_agents").fetchall()
        return [dict(r) for r in rows]

    def ping_agent(self, agent_name):
        """Check if an agent is online."""
        row = self._conn.execute(
            "SELECT status, last_heartbeat FROM bus_agents WHERE agent_name=?",
            (agent_name,),
        ).fetchone()
        if not row:
            return {"online": False, "details": "not registered"}
        online = row["status"] == "online"
        return {"online": online, "details": row["status"]}

    # ------------------------------------------------------------------
    # Cleanup
    # ------------------------------------------------------------------

    def cleanup(self, ttl_secs=86400):
        """Remove old messages and notifications past TTL."""
        cutoff = _now() - ttl_secs
        with self._conn:
            self._conn.execute(
                "DELETE FROM bus_messages WHERE updated_at < ?", (cutoff,)
            )
            self._conn.execute(
                "DELETE FROM bus_notifications WHERE created_at < ?", (cutoff,)
            )

    def stats(self):
        """Return quick stats for diagnostics."""
        msgs = self._conn.execute(
            "SELECT msg_type, status, COUNT(*) as cnt FROM bus_messages GROUP BY msg_type, status"
        ).fetchall()
        agents = self._conn.execute(
            "SELECT status, COUNT(*) as cnt FROM bus_agents GROUP BY status"
        ).fetchall()
        notifs = self._conn.execute(
            "SELECT consumed, COUNT(*) as cnt FROM bus_notifications GROUP BY consumed"
        ).fetchall()
        return {
            "messages": [dict(r) for r in msgs],
            "agents": [dict(r) for r in agents],
            "notifications": [dict(r) for r in notifs],
        }

    def sync_from_tcp(self, providers=None):
        """Bridge replies from TCP bus into SQLite.

        Polls TCP `pend` for each provider and writes any new replies
        into the SQLite bus_messages + bus_notifications tables.
        This bridges the gap: agent replies via terminal output are
        captured by the Rust bus but not written to SQLite.

        Args:
            providers: List of provider names to poll. If None, polls all
                       registered online agents.

        Returns:
            Number of new replies synced.
        """
        try:
            sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
            from bus_client import pend as tcp_pend
        except ImportError:
            return 0

        if providers is None:
            online = self.list_agents(status="online")
            providers = list(set(a["real_provider"] for a in online))

        synced = 0
        for provider in providers:
            try:
                response = tcp_pend(provider, count=10)
                if not response.get("ok"):
                    continue
                for reply in response.get("replies", []):
                    req_id = reply.get("req_id", "")
                    content = reply.get("content", "")
                    status = reply.get("status", "")
                    if not req_id or not content:
                        continue
                    # Check if we already have a reply for this req_id in SQLite
                    existing = self._conn.execute(
                        "SELECT req_id FROM bus_messages WHERE req_id=? AND msg_type='reply'",
                        (req_id,),
                    ).fetchone()
                    if existing:
                        continue
                    # Find the original ask to get the caller
                    ask = self._conn.execute(
                        "SELECT from_agent FROM bus_messages WHERE req_id=? AND msg_type='ask'",
                        (req_id,),
                    ).fetchone()
                    target = ask["from_agent"] if ask else "unknown"

                    now = _now()
                    self._conn.execute(
                        """INSERT INTO bus_messages
                           (req_id, msg_type, from_agent, to_agent, content, status,
                            created_at, updated_at, done_at, source, confidence)
                           VALUES (?, 'reply', ?, ?, ?, 'done', ?, ?, ?, ?, ?)""",
                        (req_id, provider, target, content, now, now, now,
                         reply.get("source"), reply.get("confidence")),
                    )
                    self._conn.execute(
                        """UPDATE bus_messages SET status='done', done_at=?, updated_at=?
                           WHERE req_id=? AND msg_type='ask'""",
                        (now, now, req_id),
                    )
                    self._conn.execute(
                        """INSERT INTO bus_notifications
                           (req_id, agent_name, notif_type, content_preview, created_at)
                           VALUES (?, ?, 'reply_ready', ?, ?)""",
                        (req_id, target, content[:500], now),
                    )
                    self._conn.commit()
                    synced += 1
            except Exception:
                continue
        return synced


# ------------------------------------------------------------------
# Caller detection (reuses logic from bus_client.py)
# ------------------------------------------------------------------

def _detect_caller():
    """Auto-detect the calling provider from env or process tree."""
    env = os.environ.get("WARP_CCB_PROVIDER")
    if env:
        return env
    # Lightweight: try env-based detection first, then process tree
    return _detect_caller_from_process()


def _detect_caller_from_process():
    """Walk parent process tree to find known CLI agent names."""
    known = {"claude", "codex", "gemini", "opencode", "droid", "kimi", "goose"}
    try:
        if sys.platform == "win32":
            return _detect_caller_windows(known)
        else:
            return _detect_caller_unix(known)
    except Exception:
        return "cli"


def _detect_caller_windows(known):
    import ctypes
    from ctypes import wintypes

    ntdll = ctypes.windll.ntdll
    kernel32 = ctypes.windll.kernel32

    class PROCESS_BASIC_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("Reserved1", ctypes.c_void_p),
            ("PebBaseAddress", ctypes.c_void_p),
            ("Reserved2", ctypes.c_void_p * 2),
            ("UniqueProcessId", ctypes.c_void_p),
            ("InheritedFromUniqueProcessId", ctypes.c_void_p),
        ]

    PROCESS_QUERY_INFORMATION = 0x0400
    PROCESS_VM_READ = 0x0010
    PROCESS_QUERY_LIMITED_INFORMATION = 0x1000

    seen = set()
    pid = os.getpid()

    for _ in range(20):
        if pid in seen:
            break
        seen.add(pid)
        try:
            handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if handle:
                buf = ctypes.create_unicode_buffer(260)
                size = wintypes.DWORD(260)
                ctypes.windll.kernel32.QueryFullProcessImageNameW(
                    handle, 0, buf, ctypes.byref(size)
                )
                kernel32.CloseHandle(handle)
                exe_name = buf.value.rsplit("\\", 1)[-1].lower()
                exe_base = exe_name.rsplit(".", 1)[0] if "." in exe_name else exe_name
                if exe_base in known:
                    return exe_base
        except Exception:
            pass

        handle = kernel32.OpenProcess(
            PROCESS_QUERY_INFORMATION | PROCESS_VM_READ, False, pid
        )
        if not handle:
            break
        pbi = PROCESS_BASIC_INFORMATION()
        status = ntdll.NtQueryInformationProcess(
            handle, 0, ctypes.byref(pbi), ctypes.sizeof(pbi), None
        )
        kernel32.CloseHandle(handle)
        if status != 0:
            break
        parent_pid = pbi.InheritedFromUniqueProcessId
        if not parent_pid:
            break
        pid = parent_pid

    return "cli"


def _detect_caller_unix(known):
    pid = os.getpid()
    seen = set()

    for _ in range(20):
        if pid in seen:
            break
        seen.add(pid)
        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmdline = f.read().decode("utf-8", errors="replace")
            for name in known:
                if name in cmdline.lower():
                    return name
        except OSError:
            break
        try:
            with open(f"/proc/{pid}/status", "r") as f:
                for line in f:
                    if line.startswith("PPid:"):
                        pid = int(line.split()[1])
                        break
                else:
                    break
        except OSError:
            break

    return "cli"


# ------------------------------------------------------------------
# Module-level convenience (mirrors bus_client.py API)
# ------------------------------------------------------------------

_db_instance = None


def _get_db(cwd=None):
    global _db_instance
    if _db_instance is None:
        _db_instance = BusDB(cwd=cwd)
    return _db_instance


def ask(provider, prompt, req_id=None, caller=None, cwd=None, **kwargs):
    db = _get_db(cwd)
    return db.ask(provider, prompt, req_id=req_id, caller=caller,
                  caller_cwd=cwd, **kwargs), req_id or ""


def reply(req_id, content, caller=None, **kwargs):
    db = _get_db()
    return db.reply(req_id, content, caller=caller, **kwargs)


def pend(provider, count=1, req_id=None, cwd=None, **kwargs):
    db = _get_db(cwd)
    return db.pend(provider, count=count, req_id=req_id)


def wait(req_id, timeout=300, cwd=None, **kwargs):
    db = _get_db(cwd)
    return db.wait(req_id, timeout=timeout)


def cancel(req_id, **kwargs):
    db = _get_db()
    return db.cancel(req_id)


def ping(provider, cwd=None, **kwargs):
    db = _get_db(cwd)
    return db.ping_agent(provider)


def list_sessions(cwd=None, **kwargs):
    db = _get_db(cwd)
    agents = db.list_agents(status="online")
    return {
        "ok": True,
        "sessions": [
            {
                "provider": a["real_provider"],
                "alias": a["agent_name"] if a["agent_name"] != a["real_provider"] else None,
                "terminal_view_id": a.get("view_id", 0),
                "session_id": a.get("session_id"),
                "cwd": a.get("cwd"),
                "status": a["status"],
            }
            for a in agents
        ],
    }


def register_agent(agent_name, real_provider, **kwargs):
    db = _get_db()
    return db.register_agent(agent_name, real_provider, **kwargs)


if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: bus_sqlite.py <command> [args...]")
        print("Commands: ping <agent>, list, stats, cleanup")
        sys.exit(1)

    cmd = sys.argv[1]
    db = BusDB()

    if cmd == "ping" and len(sys.argv) >= 3:
        print(json.dumps(db.ping_agent(sys.argv[2]), indent=2))
    elif cmd == "list":
        print(json.dumps(db.list_agents(), indent=2))
    elif cmd == "stats":
        print(json.dumps(db.stats(), indent=2))
    elif cmd == "cleanup":
        db.cleanup()
        print("Cleanup done")
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)

    db.close()

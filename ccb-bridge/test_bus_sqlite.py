#!/usr/bin/env python3
"""
test_bus_sqlite.py — Tests for the SQLite message bus

No external dependencies. No Warp required.
Tests the full ask/reply/wait lifecycle using SQLite only.

Usage:
    python ccb-bridge/test_bus_sqlite.py
"""

import os
import sys
import tempfile
import time
import shutil

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "lib"))

from bus_sqlite import BusDB


class TempDB:
    """Context manager for a temporary test database."""
    def __init__(self):
        self.dir = tempfile.mkdtemp(prefix="warp-ccb-test-")
        self.path = os.path.join(self.dir, "test-bus.db")
        self.db = None

    def __enter__(self):
        self.db = BusDB(db_path=self.path, cwd=self.dir)
        return self.db

    def __exit__(self, *args):
        if self.db:
            self.db.close()
        shutil.rmtree(self.dir, ignore_errors=True)


def test_schema_init():
    """Database initializes with correct schema."""
    with TempDB() as db:
        tables = db._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
        ).fetchall()
        names = [r["name"] for r in tables]
        assert "bus_messages" in names, f"Missing bus_messages table, got {names}"
        assert "bus_agents" in names, f"Missing bus_agents table, got {names}"
        assert "bus_notifications" in names, f"Missing bus_notifications table, got {names}"
        assert "bus_meta" in names, f"Missing bus_meta table, got {names}"
    print("  [PASS] schema init")


def test_ask_creates_message_and_notification():
    """ask() creates a pending message and a notification for the target."""
    with TempDB() as db:
        req_id = db.ask("claude", "hello world", caller="droid")
        assert req_id, "ask should return a req_id"

        msg = db.get_request(req_id)
        assert msg is not None, "ask message should exist"
        assert msg["msg_type"] == "ask"
        assert msg["from_agent"] == "droid"
        assert msg["to_agent"] == "claude"
        assert msg["content"] == "hello world"
        assert msg["status"] == "pending"

        notifs = db.get_notifications("claude", consume=False)
        assert len(notifs) == 1, f"Expected 1 notification, got {len(notifs)}"
        assert notifs[0]["notif_type"] == "new_request"
        assert notifs[0]["req_id"] == req_id
    print("  [PASS] ask creates message + notification")


def test_reply_updates_ask_and_creates_notification():
    """reply() creates a reply message, updates ask status, notifies caller."""
    with TempDB() as db:
        req_id = db.ask("claude", "analyze this", caller="droid")

        # Consume the new_request notification first
        db.get_notifications("claude", consume=True)

        db.reply(req_id, "analysis complete", caller="claude",
                 source="explicit_reply", confidence=1.0)

        # Check ask is now done
        ask_msg = db.get_request(req_id)
        assert ask_msg["status"] == "done", f"Expected done, got {ask_msg['status']}"
        assert ask_msg["done_at"] is not None

        # Check reply exists
        reply_msg = db.get_reply(req_id)
        assert reply_msg is not None
        assert reply_msg["content"] == "analysis complete"
        assert reply_msg["from_agent"] == "claude"
        assert reply_msg["to_agent"] == "droid"
        assert reply_msg["source"] == "explicit_reply"
        assert reply_msg["confidence"] == 1.0

        # Check notification sent to caller (droid)
        notifs = db.get_notifications("droid", consume=False)
        assert len(notifs) == 1
        assert notifs[0]["notif_type"] == "reply_ready"
    print("  [PASS] reply updates ask + creates notification")


def test_wait_blocks_until_reply():
    """wait() blocks until a reply is available, then returns it."""
    with TempDB() as db:
        import threading

        req_id = db.ask("claude", "hello", caller="droid")

        result_holder = {}

        def wait_thread():
            result_holder["result"] = db.wait(req_id, timeout=10)

        t = threading.Thread(target=wait_thread)
        t.start()

        # Small delay to ensure wait is blocking
        time.sleep(0.3)
        assert "result" not in result_holder, "wait should not return before reply"

        db.reply(req_id, "hi there", caller="claude")
        t.join(timeout=5)

        assert "result" in result_holder
        r = result_holder["result"]
        assert r["ok"]
        assert r["content"] == "hi there"
        assert r["status"] == "done"
    print("  [PASS] wait blocks until reply")


def test_wait_timeout():
    """wait() returns error after timeout."""
    with TempDB() as db:
        req_id = db.ask("claude", "hello", caller="droid")
        result = db.wait(req_id, timeout=1)
        assert not result["ok"]
        assert "timeout" in result["message"]
    print("  [PASS] wait timeout")


def test_cancel():
    """cancel() marks the ask as cancelled and notifies target."""
    with TempDB() as db:
        req_id = db.ask("claude", "do something", caller="droid")
        db.get_notifications("claude", consume=True)  # clear new_request

        ok = db.cancel(req_id, caller="droid")
        assert ok, "cancel should succeed"

        msg = db.get_request(req_id)
        assert msg["status"] == "cancelled"

        notifs = db.get_notifications("claude", consume=False)
        assert len(notifs) == 1
        assert notifs[0]["notif_type"] == "cancelled"
    print("  [PASS] cancel")


def test_poll_requests():
    """poll_requests returns pending requests for an agent."""
    with TempDB() as db:
        db.ask("claude", "task 1", caller="droid")
        db.ask("claude", "task 2", caller="kimi")
        db.ask("codex", "task 3", caller="droid")

        claude_tasks = db.poll_requests("claude")
        assert len(claude_tasks) == 2
        assert claude_tasks[0]["content"] == "task 1"  # FIFO

        codex_tasks = db.poll_requests("codex")
        assert len(codex_tasks) == 1

        kimi_tasks = db.poll_requests("kimi")
        assert len(kimi_tasks) == 0
    print("  [PASS] poll requests")


def test_mark_delivered():
    """mark_delivered updates status to 'delivered'."""
    with TempDB() as db:
        req_id = db.ask("claude", "hello", caller="droid")
        db.mark_delivered(req_id)

        msg = db.get_request(req_id)
        assert msg["status"] == "delivered"
        assert msg["delivered_at"] is not None
    print("  [PASS] mark delivered")


def test_pend_returns_replies():
    """pend(agent) returns replies addressed TO that agent (to_agent match).

    Real scenario: droid asks claude → claude replies → reply has
    to_agent='droid'. So pend('droid') finds it, pend('claude') does not.
    """
    with TempDB() as db:
        r1 = db.ask("claude", "q1", caller="droid")
        r2 = db.ask("claude", "q2", caller="droid")
        db.reply(r1, "a1", caller="claude")
        db.reply(r2, "a2", caller="claude")

        # droid is the asker → replies are addressed TO droid
        replies = db.pend("droid", count=2)
        assert len(replies) == 2, f"Expected 2 replies, got {len(replies)}"
        assert replies[0]["content"] == "a2"  # most recent first

        single = db.pend("droid", req_id=r1)
        assert len(single) == 1
        assert single[0]["content"] == "a1"

        # claude is the replier, not the recipient → should find nothing
        claude_replies = db.pend("claude", count=10)
        assert len(claude_replies) == 0, f"claude should have 0 replies, got {len(claude_replies)}"
    print("  [PASS] pend returns replies")


def test_agent_registration():
    """register/deregister/heartbeat agent lifecycle."""
    with TempDB() as db:
        db.register_agent("writer", "codex", view_id=100, pid=1234)

        agent = db.get_agent("writer")
        assert agent is not None
        assert agent["real_provider"] == "codex"
        assert agent["status"] == "online"
        assert agent["view_id"] == 100

        result = db.ping_agent("writer")
        assert result["online"]

        db.heartbeat("writer")
        agent2 = db.get_agent("writer")
        assert agent2["last_heartbeat"] >= agent["last_heartbeat"]

        db.deregister_agent("writer")
        result = db.ping_agent("writer")
        assert not result["online"]

        agents = db.list_agents(status="online")
        assert len(agents) == 0
    print("  [PASS] agent registration")


def test_list_agents():
    """list_agents returns all or filtered agents."""
    with TempDB() as db:
        db.register_agent("writer", "codex")
        db.register_agent("reviewer", "codex")
        db.register_agent("planner", "claude")
        db.deregister_agent("planner")

        all_agents = db.list_agents()
        assert len(all_agents) == 3

        online = db.list_agents(status="online")
        assert len(online) == 2
        assert all(a["status"] == "online" for a in online)
    print("  [PASS] list agents")


def test_notifications_consumed():
    """get_notifications marks notifications as consumed."""
    with TempDB() as db:
        req1 = db.ask("claude", "q1", caller="droid")
        req2 = db.ask("claude", "q2", caller="droid")

        # First read: consume both
        notifs = db.get_notifications("claude", consume=True)
        assert len(notifs) == 2

        # Second read: none left
        notifs2 = db.get_notifications("claude", consume=True)
        assert len(notifs2) == 0
    print("  [PASS] notifications consumed")


def test_cleanup():
    """cleanup removes old messages and notifications."""
    with TempDB() as db:
        req_id = db.ask("claude", "old task", caller="droid")

        # Manually age the message
        old_time = _now() - 100000
        db._conn.execute(
            "UPDATE bus_messages SET updated_at=?, created_at=? WHERE req_id=?",
            (old_time, old_time, req_id),
        )
        db._conn.execute(
            "UPDATE bus_notifications SET created_at=? WHERE req_id=?",
            (old_time, req_id),
        )
        db._conn.commit()

        db.cleanup(ttl_secs=60)

        msg = db.get_request(req_id)
        assert msg is None, "Old message should be cleaned up"
    print("  [PASS] cleanup")


def test_stats():
    """stats returns message/agent/notification counts."""
    with TempDB() as db:
        db.register_agent("claude", "claude")
        db.ask("claude", "q1", caller="droid")
        db.ask("codex", "q2", caller="droid")

        s = db.stats()
        assert len(s["messages"]) > 0
        assert len(s["agents"]) > 0
    print("  [PASS] stats")


def test_reply_for_nonexistent_ask_fails():
    """reply() raises ValueError if no matching ask exists."""
    with TempDB() as db:
        try:
            db.reply("nonexistent-req-id", "content", caller="claude")
            assert False, "Should have raised ValueError"
        except ValueError as e:
            assert "No ask found" in str(e)
    print("  [PASS] reply for nonexistent ask fails")


def test_cancel_nonexistent_request():
    """cancel() returns False for nonexistent request."""
    with TempDB() as db:
        ok = db.cancel("nonexistent-req-id")
        assert not ok
    print("  [PASS] cancel nonexistent returns False")


def test_wal_mode():
    """Verify WAL journal mode is set."""
    with TempDB() as db:
        row = db._conn.execute("PRAGMA journal_mode").fetchone()
        assert row["journal_mode"] == "wal", f"Expected wal, got {row['journal_mode']}"
    print("  [PASS] WAL mode")


def test_concurrent_access():
    """Multiple BusDB instances can read/write concurrently."""
    with TempDB() as db1:
        db2 = BusDB(db_path=db1.db_path, cwd=db1.cwd)
        try:
            req_id = db1.ask("claude", "from db1", caller="droid")
            db2.reply(req_id, "from db2", caller="claude")

            reply = db1.get_reply(req_id)
            assert reply is not None
            assert reply["content"] == "from db2"
        finally:
            db2.close()
    print("  [PASS] concurrent access")


def _now():
    return time.time()


def run_all():
    tests = [
        test_schema_init,
        test_ask_creates_message_and_notification,
        test_reply_updates_ask_and_creates_notification,
        test_wait_blocks_until_reply,
        test_wait_timeout,
        test_cancel,
        test_poll_requests,
        test_mark_delivered,
        test_pend_returns_replies,
        test_agent_registration,
        test_list_agents,
        test_notifications_consumed,
        test_cleanup,
        test_stats,
        test_reply_for_nonexistent_ask_fails,
        test_cancel_nonexistent_request,
        test_wal_mode,
        test_concurrent_access,
    ]

    passed = 0
    failed = 0
    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except Exception as e:
            print(f"  [FAIL] {test_fn.__name__}: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed, {passed+failed} total")
    return failed == 0


if __name__ == "__main__":
    success = run_all()
    sys.exit(0 if success else 1)

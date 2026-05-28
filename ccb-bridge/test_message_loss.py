"""
test_message_loss.py — Regression tests for the "occasional missed message"
bug class. Run standalone, no Warp required.

Covers three failure modes that previously caused silent task loss:

  1. warp-pend (or any consumer that only cares about replies) used to
     call db.get_notifications() without a type filter, eating the agent's
     pending new_request notifications. The receiving worker would then
     never see the task.

  2. bus_notifier.ReplyWatcher had the same shape: it consumed everything,
     skipped non-reply_ready, and silently dropped new_request.

  3. Even with notifications behaving correctly, any new_request lost for
     any other reason (worker restart, crash between consume and dispatch,
     unrelated tool consuming notifs) would leave the ask stuck in
     `bus_messages` as 'pending' forever. ccb-worker now sweeps for these
     orphans on startup and every SWEEP_EVERY ticks.

Run:
    python ccb-bridge/test_message_loss.py
"""

import os
import sys
import tempfile
import time
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "lib"))
from bus_sqlite import BusDB
from bus_notifier import ReplyWatcher


def _fresh_db():
    """Allocate a fresh DB file in a temp dir; caller closes the BusDB."""
    tmp = tempfile.mkdtemp(prefix="ccb-msgloss-")
    return BusDB(db_path=os.path.join(tmp, "bus.db"))


class TestGetNotificationsTypeFilter(unittest.TestCase):
    """get_notifications must support filtering by notif_type so callers
    can consume reply_ready without touching new_request."""

    def test_filter_leaves_other_types_alone(self):
        db = _fresh_db()
        try:
            req_id = db.ask("bob", "task X", caller="alice")
            db.reply(req_id, "done", caller="bob")

            # bob has a new_request; alice has a reply_ready. Consuming
            # alice's reply_ready must not touch bob's new_request.
            alice_replies = db.get_notifications(
                "alice", consume=True, notif_type="reply_ready"
            )
            self.assertEqual(len(alice_replies), 1)
            self.assertEqual(alice_replies[0]["notif_type"], "reply_ready")

            bob_unread = db._conn.execute(
                """SELECT notif_type FROM bus_notifications
                   WHERE agent_name='bob' AND consumed=0"""
            ).fetchall()
            self.assertEqual(
                [r["notif_type"] for r in bob_unread],
                ["new_request"],
                "bob's new_request must still be unconsumed",
            )
        finally:
            db.close()

    def test_filter_only_returns_matching_type(self):
        db = _fresh_db()
        try:
            # carol has BOTH a new_request (from dave) and a reply_ready
            # (because she previously asked dave who replied).
            db.ask("carol", "carol-task", caller="dave")
            req2 = db.ask("dave", "dave-task", caller="carol")
            db.reply(req2, "ok", caller="dave")

            replies = db.get_notifications(
                "carol", consume=True, notif_type="reply_ready"
            )
            self.assertEqual(len(replies), 1)
            self.assertEqual(replies[0]["notif_type"], "reply_ready")

            # The new_request for carol must remain — it belongs to her worker.
            remaining = db.get_notifications("carol", consume=False)
            self.assertEqual(len(remaining), 1)
            self.assertEqual(remaining[0]["notif_type"], "new_request")
        finally:
            db.close()


class TestWarpPendDoesNotEatNewRequest(unittest.TestCase):
    """warp-pend now passes notif_type='reply_ready' so it cannot silently
    consume a peer's new_request. This test simulates the same call shape
    warp-pend uses internally."""

    def test_warp_pend_call_shape(self):
        db = _fresh_db()
        try:
            req_id = db.ask("bob", "task X", caller="alice")

            # Simulate warp-pend bob: pend then consume reply_ready.
            replies = db.pend("bob", count=1)
            self.assertEqual(replies, [], "no replies yet")
            db.get_notifications("bob", consume=True, notif_type="reply_ready")

            # The worker for bob should still see the new_request waiting.
            notifs = db.get_notifications("bob", consume=False)
            types = [n["notif_type"] for n in notifs]
            self.assertIn("new_request", types,
                          "new_request must survive a warp-pend call on bob")

            # And the ask record is still pending and reachable.
            ask = db.get_request(req_id)
            self.assertIsNotNone(ask)
            self.assertEqual(ask["status"], "pending")
        finally:
            db.close()


class TestReplyWatcherDoesNotEatNewRequest(unittest.TestCase):
    """ReplyWatcher previously consumed all notif types and discarded
    anything that wasn't reply_ready. Now it filters at the query level."""

    def test_reply_watcher_preserves_new_request(self):
        db = _fresh_db()
        try:
            # A peer asks alice — this puts a new_request in alice's queue.
            db.ask("alice", "task for alice", caller="bob")

            # Alice runs a ReplyWatcher to wait for HER outbound asks'
            # replies. The watcher must not touch the new_request.
            os.environ["WARP_CCB_PROVIDER"] = "alice"
            watcher = ReplyWatcher(db_path=db.db_path, poll_interval=0.05)
            watcher.start()
            try:
                # Give the watcher loop several ticks to (incorrectly,
                # in the old code) consume all of alice's notifications.
                time.sleep(0.4)
            finally:
                watcher.close()

            unread = db.get_notifications("alice", consume=False)
            types = [n["notif_type"] for n in unread]
            self.assertIn("new_request", types,
                          "ReplyWatcher must not consume alice's new_request")
        finally:
            db.close()
            os.environ.pop("WARP_CCB_PROVIDER", None)


class TestOrphanAskSweep(unittest.TestCase):
    """Even when a notification is lost for any reason, the ask record is
    still in bus_messages. ccb-worker's _sweep_orphan_asks must pick it up.

    We don't spawn the full ccb-worker subprocess; instead we exercise the
    same query that the sweep uses, which is the contract that matters."""

    def _sweep(self, db, agent_name):
        return [
            row["req_id"]
            for row in db._conn.execute(
                """SELECT req_id FROM bus_messages
                   WHERE to_agent=? AND msg_type='ask' AND status='pending'
                   ORDER BY priority DESC, created_at ASC""",
                (agent_name,),
            ).fetchall()
        ]

    def test_sweep_finds_pending_when_notification_was_eaten(self):
        db = _fresh_db()
        try:
            req_id = db.ask("bob", "task", caller="alice")

            # Simulate the old buggy warp-pend that ate ALL of bob's notifs.
            db.get_notifications("bob", consume=True)  # ← no type filter

            # Notifications are gone — but the sweep can still recover.
            orphans = self._sweep(db, "bob")
            self.assertEqual(orphans, [req_id])
        finally:
            db.close()

    def test_sweep_ignores_completed_asks(self):
        db = _fresh_db()
        try:
            req_id = db.ask("bob", "task", caller="alice")
            db.reply(req_id, "done", caller="bob")
            self.assertEqual(self._sweep(db, "bob"), [],
                             "completed asks must not show up as orphans")
        finally:
            db.close()

    def test_sweep_ignores_delivered_asks(self):
        """Once mark_delivered runs, the worker owns the ask — sweep should
        not re-pick it up and risk double-handling."""
        db = _fresh_db()
        try:
            req_id = db.ask("bob", "task", caller="alice")
            db.mark_delivered(req_id)
            self.assertEqual(self._sweep(db, "bob"), [],
                             "delivered asks must not show up as orphans")
        finally:
            db.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)

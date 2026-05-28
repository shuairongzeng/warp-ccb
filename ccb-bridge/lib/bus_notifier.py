"""
bus_notifier.py — Watch-based push notification layer for SQLite bus

Provides two modes of push delivery:

1. Threaded watcher: a background thread polls bus_notifications and invokes
   a user-supplied callback for each new notification.

2. Subscription manager: agents register interest in specific event types
   (new_request, reply_ready, cancelled) and get callbacks only for those.

Both modes read from the same bus_notifications table in bus_sqlite.py.
The poll interval is configurable (default 0.5s) and SQLite WAL mode ensures
readers never block writers.
"""

import os
import sys
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))
from bus_sqlite import BusDB


class Notifier:
    """Watch bus_notifications and push events to subscribers."""

    def __init__(self, db_path=None, cwd=None, poll_interval=0.5):
        self.db = BusDB(db_path=db_path, cwd=cwd)
        self.poll_interval = poll_interval
        self._callbacks = {}  # agent_name -> list of callback fns
        self._type_callbacks = {}  # (agent_name, notif_type) -> list of callback fns
        self._threads = {}  # agent_name -> threading.Thread
        self._stop = threading.Event()

    def close(self):
        self._stop.set()
        for t in self._threads.values():
            t.join(timeout=5)
        self.db.close()

    def subscribe(self, agent_name, callback, notif_type=None):
        """Register a callback for an agent's notifications.

        Args:
            agent_name: The agent to watch notifications for.
            callback: callable(notification_dict) — invoked for each new notif.
            notif_type: If set, only invoke for this type ('new_request',
                        'reply_ready', 'cancelled'). If None, invoke for all.
        """
        if notif_type:
            key = (agent_name, notif_type)
            self._type_callbacks.setdefault(key, []).append(callback)
        else:
            self._callbacks.setdefault(agent_name, []).append(callback)

        if agent_name not in self._threads:
            t = threading.Thread(
                target=self._watch_loop,
                args=(agent_name,),
                daemon=True,
                name=f"bus-notifier-{agent_name}",
            )
            self._threads[agent_name] = t
            t.start()

    def unsubscribe(self, agent_name, callback=None, notif_type=None):
        """Remove a previously registered callback."""
        if notif_type and callback:
            key = (agent_name, notif_type)
            if key in self._type_callbacks:
                self._type_callbacks[key] = [
                    cb for cb in self._type_callbacks[key] if cb is not callback
                ]
        elif callback and agent_name in self._callbacks:
            self._callbacks[agent_name] = [
                cb for cb in self._callbacks[agent_name] if cb is not callback
            ]

    def _dispatch(self, agent_name, notif):
        """Send a notification to all matching callbacks."""
        for cb in self._callbacks.get(agent_name, []):
            try:
                cb(notif)
            except Exception:
                pass

        key = (agent_name, notif.get("notif_type"))
        for cb in self._type_callbacks.get(key, []):
            try:
                cb(notif)
            except Exception:
                pass

    def _watch_loop(self, agent_name):
        """Background loop that polls for new notifications."""
        while not self._stop.is_set():
            try:
                notifs = self.db.get_notifications(agent_name, consume=True)
                for n in notifs:
                    self._dispatch(agent_name, n)
            except Exception:
                pass
            self._stop.wait(self.poll_interval)


class ReplyWatcher:
    """Convenience: watch for reply_ready notifications and resolve futures."""

    def __init__(self, db_path=None, cwd=None, poll_interval=0.5):
        self.db = BusDB(db_path=db_path, cwd=cwd)
        self.poll_interval = poll_interval
        self._futures = {}  # req_id -> threading.Event
        self._results = {}  # req_id -> result dict
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._watch_loop, daemon=True, name="bus-reply-watcher"
            )
            self._thread.start()

    def close(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        self.db.close()

    def wait(self, req_id, timeout=300):
        """Block until the reply for req_id is ready, then return it."""
        with self._lock:
            if req_id in self._results:
                return self._results.pop(req_id)
            event = threading.Event()
            self._futures[req_id] = event

        self.start()

        if event.wait(timeout=timeout):
            with self._lock:
                return self._results.pop(req_id, None)
        else:
            with self._lock:
                self._futures.pop(req_id, None)
            return None

    def _watch_loop(self):
        caller = os.environ.get("WARP_CCB_PROVIDER") or "cli"
        while not self._stop.is_set():
            try:
                # Only consume reply_ready — new_request notifications for the
                # same agent belong to ccb-worker and must not be eaten here.
                notifs = self.db.get_notifications(
                    caller, consume=True, notif_type="reply_ready"
                )
                for n in notifs:
                    req_id = n.get("req_id")
                    reply = self.db.get_reply(req_id)
                    if reply:
                        with self._lock:
                            self._results[req_id] = reply
                            event = self._futures.pop(req_id, None)
                        if event:
                            event.set()
            except Exception:
                pass
            self._stop.wait(self.poll_interval)


if __name__ == "__main__":
    print("bus_notifier.py — use as a library. No standalone CLI.")

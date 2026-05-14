"""
bus_client.py — JSONL socket client for Warp LocalAgentBus

Connects to the Warp LocalAgentBus via TCP socket (Phase 1)
or Unix socket / Windows named pipe (future).
"""

import json
import os
import socket
import sys
import time

# Default bus address file location
if sys.platform == "win32":
    BUS_ADDRESS_FILE = os.path.join(
        os.environ.get("LOCALAPPDATA", os.path.expanduser("~\\AppData\\Local")),
        "warp-ccb",
        "bus-address.json",
    )
else:
    BUS_ADDRESS_FILE = os.path.expanduser("~/.warp-ccb/bus-address.json")
DEFAULT_TIMEOUT = 10  # seconds


def read_bus_address(path=None):
    """Read bus-address.json to discover the Warp LocalAgentBus endpoint."""
    path = path or BUS_ADDRESS_FILE
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Bus address file not found: {path}\n"
            "Make sure Warp is running with LocalAgentBus enabled."
        )
    with open(path, "r") as f:
        info = json.load(f)

    # Validate pid is still alive (Unix only)
    pid = info.get("pid")
    if pid and sys.platform != "win32":
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            raise ConnectionError(
                f"Warp process (pid={pid}) is no longer running. Stale bus-address.json?"
            )

    return info


def call_bus(command, bus_info=None, timeout=DEFAULT_TIMEOUT):
    """
    Send a command to the Warp LocalAgentBus and return the response.

    Args:
        command: dict with the bus command (must include 'type')
        bus_info: pre-loaded bus address info (optional, auto-discovered if not given)
        timeout: socket timeout in seconds

    Returns:
        dict with the bus response
    """
    if bus_info is None:
        bus_info = read_bus_address()

    auth_token = bus_info["auth_token"]
    protocol_version = bus_info.get("protocol_version", 1)

    # Build the request envelope
    request = {
        "v": protocol_version,
        "token": auth_token,
        **command,
    }

    # Connect to the bus
    # Phase 1: TCP loopback (socket_path contains host:port)
    socket_path = bus_info.get("socket_path", "")

    if socket_path.startswith("127.0.0.1:") or socket_path.startswith("localhost:"):
        # TCP connection
        host, port_str = socket_path.split(":")
        port = int(port_str)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect((host, port))
    elif socket_path.startswith("\\\\.\\pipe\\"):
        # Windows named pipe (future)
        raise NotImplementedError("Windows named pipe not yet supported in Phase 1")
    else:
        # Unix socket (future)
        sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        sock.connect(socket_path)

    try:
        # Send request as JSON line
        request_json = json.dumps(request) + "\n"
        sock.sendall(request_json.encode("utf-8"))

        # Read response line
        response_data = b""
        while True:
            chunk = sock.recv(4096)
            if not chunk:
                break
            response_data += chunk
            if b"\n" in response_data:
                break

        response_line = response_data.decode("utf-8").strip()
        if not response_line:
            raise ConnectionError("Empty response from bus")

        return json.loads(response_line)

    finally:
        sock.close()


def ask(provider, prompt, req_id=None, caller="cli", cwd=None, session_id=None, queue=False, bus_info=None):
    """Send an ask command to a CLI agent."""
    import uuid

    if req_id is None:
        ts = time.strftime("%Y%m%d-%H%M%S")
        req_id = f"{ts}-{uuid.uuid4().hex[:8]}"

    cmd = {
        "type": "ask",
        "provider": provider,
        "prompt": prompt,
        "req_id": req_id,
        "caller": caller,
        "queue": queue,
    }
    if cwd:
        cmd["cwd"] = cwd
    if session_id:
        cmd["session_id"] = session_id

    return call_bus(cmd, bus_info=bus_info), req_id


def pend(provider, count=1, req_id=None, cwd=None, session_id=None, chain_id=None, bus_info=None):
    """Query replies from a CLI agent."""
    cmd = {
        "type": "pend",
        "provider": provider,
        "count": count,
    }
    if cwd:
        cmd["cwd"] = cwd
    if session_id:
        cmd["session_id"] = session_id
    if req_id:
        cmd["req_id"] = req_id
    if chain_id:
        cmd["chain_id"] = chain_id

    return call_bus(cmd, bus_info=bus_info)


def ping(provider, cwd=None, session_id=None, bus_info=None):
    """Check if a CLI agent session is online."""
    cmd = {
        "type": "ping",
        "provider": provider,
    }
    if cwd:
        cmd["cwd"] = cwd
    if session_id:
        cmd["session_id"] = session_id

    return call_bus(cmd, bus_info=bus_info)


def list_sessions(cwd=None, bus_info=None):
    """List all active CLI agent sessions."""
    cmd = {"type": "list_sessions"}
    if cwd:
        cmd["cwd"] = cwd

    return call_bus(cmd, bus_info=bus_info)


def cancel(req_id, bus_info=None):
    """Cancel a pending request."""
    cmd = {
        "type": "cancel",
        "req_id": req_id,
    }

    return call_bus(cmd, bus_info=bus_info)


def launch(provider, prompt=None, cwd=None, bus_info=None, auto_tab=True):
    """Launch a new CLI agent session.

    If auto_tab=True and no idle terminal is available, automatically opens
    a new Warp tab and retries.
    """
    cmd = {
        "type": "launch",
        "provider": provider,
    }
    if prompt:
        cmd["prompt"] = prompt
    if cwd:
        cmd["cwd"] = cwd

    result = call_bus(cmd, bus_info=bus_info)

    # If no idle terminal, try opening a new Warp tab and retry.
    if auto_tab and not result.get("ok") and "no idle terminal" in result.get("message", ""):
        _open_new_warp_tab()
        time.sleep(3)
        result = call_bus(cmd, bus_info=bus_info)

    return result


def _open_new_warp_tab():
    """Open a new tab in the running Warp instance."""
    import subprocess
    import sys

    if sys.platform == "win32":
        # Use PowerShell to send Ctrl+Shift+T to Warp
        ps_cmd = (
            "Add-Type -AssemblyName System.Windows.Forms; "
            "$wshell = New-Object -ComObject WScript.Shell; "
            "$wshell.AppActivate('Warp') | Out-Null; "
            "Start-Sleep -Milliseconds 500; "
            "$wshell.SendKeys('^+t')"
        )
        try:
            subprocess.run(
                ["powershell", "-Command", ps_cmd],
                timeout=5,
                capture_output=True,
            )
        except Exception:
            pass


def chain(steps, caller=None, bus_info=None):
    """Execute a chain of agent steps sequentially."""
    cmd = {
        "type": "chain",
        "steps": steps,
    }
    if caller:
        cmd["caller"] = caller

    return call_bus(cmd, bus_info=bus_info)


def wait(req_id=None, timeout=300, bus_info=None):
    """Block until a request completes, then return the result.

    Args:
        req_id: The request ID to wait for (required)
        timeout: Maximum seconds to wait (default 300 = 5 minutes)
        bus_info: Pre-loaded bus address info (optional)

    Returns:
        dict with wait_result response including status and content
    """
    cmd = {
        "type": "wait",
        "timeout_ms": timeout * 1000,
    }
    if req_id:
        cmd["req_id"] = req_id

    return call_bus(cmd, bus_info=bus_info, timeout=timeout + 5)


if __name__ == "__main__":
    # Quick test
    import sys

    if len(sys.argv) < 2:
        print("Usage: bus_client.py <command> [args...]")
        print("Commands: ping <provider>, list")
        sys.exit(1)

    cmd = sys.argv[1]
    if cmd == "ping" and len(sys.argv) >= 3:
        result = ping(sys.argv[2])
        print(json.dumps(result, indent=2))
    elif cmd == "list":
        result = list_sessions()
        print(json.dumps(result, indent=2))
    else:
        print(f"Unknown command: {cmd}")
        sys.exit(1)

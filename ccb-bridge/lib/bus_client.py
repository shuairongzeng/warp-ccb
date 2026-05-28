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


def read_bus_address(path=None, cwd=None):
    """Read bus-address.json to discover the Warp LocalAgentBus endpoint.

    Multi-instance resolution order:
    1. WARP_BUS_PID env var → bus-address-{pid}.json
    2. Parent process tree → find warp.exe → bus-address-{pid}.json
    3. cwd matching → list_sessions(cwd) across all instances
    4. Default bus-address.json (backward compat)
    """
    path = path or BUS_ADDRESS_FILE
    bus_dir = os.path.dirname(path)

    # Strategy 1: Explicit env var
    env_pid = os.environ.get("WARP_BUS_PID")
    if env_pid:
        pid_path = os.path.join(bus_dir, f"bus-address-{env_pid}.json")
        if os.path.exists(pid_path):
            with open(pid_path, "r") as f:
                info = json.load(f)
            if _is_pid_alive(info.get("pid")):
                return info

    # Strategy 2: Find parent warp.exe via process tree
    warp_pid = _find_parent_warp_pid()
    if warp_pid:
        pid_path = os.path.join(bus_dir, f"bus-address-{warp_pid}.json")
        if os.path.exists(pid_path):
            with open(pid_path, "r") as f:
                info = json.load(f)
            if _is_pid_alive(info.get("pid")):
                return info

    # Strategy 3: cwd-based discovery
    if cwd:
        candidates = _discover_bus_instances(bus_dir)
        for info in candidates:
            if _instance_has_cwd(info, cwd):
                return info

    # Strategy 4: Default file
    if os.path.exists(path):
        with open(path, "r") as f:
            info = json.load(f)
        if _is_pid_alive(info.get("pid")):
            return info
        # Stale default — try to find any running instance
        try:
            os.remove(path)
        except OSError:
            pass

    # Last resort: newest running instance
    candidates = _discover_bus_instances(bus_dir)
    if candidates:
        # Write the newest as default for next time
        try:
            with open(path, "w") as f:
                json.dump(candidates[0], f)
        except OSError:
            pass
        return candidates[0]

    raise FileNotFoundError(
        f"No running Warp instance found in {bus_dir}\n"
        "Make sure Warp is running with LocalAgentBus enabled."
    )


def _discover_bus_instances(bus_dir):
    """Find all bus-address-*.json files and return valid (alive) instances."""
    if not os.path.isdir(bus_dir):
        return []

    instances = []
    for fname in os.listdir(bus_dir):
        if not fname.startswith("bus-address-") or not fname.endswith(".json"):
            continue
        fpath = os.path.join(bus_dir, fname)
        try:
            with open(fpath, "r") as f:
                info = json.load(f)
            if _is_pid_alive(info.get("pid")):
                instances.append(info)
            else:
                # Clean up stale file
                try:
                    os.remove(fpath)
                except OSError:
                    pass
        except (json.JSONDecodeError, OSError):
            continue

    # Sort by created_at descending (prefer newest)
    instances.sort(key=lambda x: x.get("created_at_ms", 0), reverse=True)
    return instances


def _is_pid_alive(pid):
    """Check if a process with the given PID is still running."""
    if not pid:
        return False
    try:
        if sys.platform == "win32":
            import ctypes
            kernel32 = ctypes.windll.kernel32
            handle = kernel32.OpenProcess(0x100000, False, pid)
            if handle:
                kernel32.CloseHandle(handle)
                return True
            return False
        else:
            os.kill(pid, 0)
            return True
    except (OSError, ProcessLookupError):
        return False


def _validate_pid(info):
    """Raise if the Warp process is no longer alive."""
    pid = info.get("pid")
    if pid and not _is_pid_alive(pid):
        raise ConnectionError(
            f"Warp process (pid={pid}) is no longer running. Stale bus-address.json?"
        )


def _find_parent_warp_pid():
    """Walk the parent process tree to find a warp.exe process.

    Returns the PID of the nearest warp.exe ancestor, or None.
    """
    try:
        if sys.platform == "win32":
            return _find_parent_warp_pid_windows()
        else:
            return _find_parent_warp_pid_unix()
    except Exception:
        return None


def _find_parent_warp_pid_windows():
    """Windows: use NtQueryInformationProcess to walk parent PIDs."""
    import ctypes
    from ctypes import wintypes

    ntdll = ctypes.windll.ntdll
    kernel32 = ctypes.windll.kernel32

    # NtQueryInformationProcess with ProcessBasicInformation
    class PROCESS_BASIC_INFORMATION(ctypes.Structure):
        _fields_ = [
            ("Reserved1", ctypes.c_void_p),
            ("PebBaseAddress", ctypes.c_void_p),
            ("Reserved2", ctypes.c_void_p * 2),
            ("UniqueProcessId", ctypes.c_void_p),
            ("InheritedFromUniqueProcessId", ctypes.c_void_p),
        ]

    # Open process flags
    PROCESS_QUERY_INFORMATION = 0x0400
    PROCESS_VM_READ = 0x0010

    seen = set()
    pid = os.getpid()

    for _ in range(20):  # Max depth
        if pid in seen:
            break
        seen.add(pid)

        # Check if this process is warp.exe
        try:
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            handle = kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if handle:
                buf = ctypes.create_unicode_buffer(260)
                # QueryFullProcessImageNameW
                size = wintypes.DWORD(260)
                ctypes.windll.kernel32.QueryFullProcessImageNameW(
                    handle, 0, buf, ctypes.byref(size)
                )
                kernel32.CloseHandle(handle)
                exe_name = buf.value.rsplit("\\", 1)[-1].lower()
                if exe_name == "warp.exe":
                    return pid
        except Exception:
            pass

        # Get parent PID
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

    return None


def _find_parent_warp_pid_unix():
    """Unix: walk /proc/{pid}/status to find parent warp process."""
    import re
    pid = os.getpid()
    seen = set()

    for _ in range(20):
        if pid in seen:
            break
        seen.add(pid)

        try:
            # Check exe name
            exe = os.readlink(f"/proc/{pid}/exe")
            if "warp" in exe.lower():
                return pid
        except OSError:
            pass

        try:
            with open(f"/proc/{pid}/status") as f:
                for line in f:
                    if line.startswith("PPid:"):
                        pid = int(line.split()[1])
                        break
                else:
                    break
        except OSError:
            break

    return None


def detect_caller():
    """Auto-detect the calling provider from the process tree.

    Walks parent processes looking for known CLI agent names:
    claude, codex, gemini, opencode, droid, kimi, goose.
    Returns the provider name or 'cli' if unknown.
    """
    known_agents = {
        "claude": "claude",
        "codex": "codex",
        "gemini": "gemini",
        "opencode": "opencode",
        "droid": "droid",
        "kimi": "kimi",
        "goose": "goose",
    }

    try:
        if sys.platform == "win32":
            return _detect_caller_windows(known_agents)
        else:
            return _detect_caller_unix(known_agents)
    except Exception:
        return "cli"


def _env_int(name):
    value = os.environ.get(name)
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def detect_caller_identity(caller=None):
    provider = caller or os.environ.get("WARP_CCB_PROVIDER") or detect_caller()
    return {
        "caller": provider,
        "caller_terminal_view_id": _env_int("WARP_CCB_TERMINAL_VIEW_ID"),
        "caller_session_id": os.environ.get("WARP_CCB_SESSION_ID"),
        "caller_cwd": os.environ.get("WARP_CCB_CWD") or os.getcwd(),
    }


def _detect_caller_windows(known_agents):
    """Windows: walk process tree via NtQueryInformationProcess."""
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
                # Check against known agent names (e.g. "claude.exe" -> "claude")
                exe_base = exe_name.rsplit(".", 1)[0] if "." in exe_name else exe_name
                if exe_base in known_agents:
                    return known_agents[exe_base]
        except Exception:
            pass

        # Get parent PID
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


def _detect_caller_unix(known_agents):
    """Unix: walk /proc tree looking for known agent process names."""
    pid = os.getpid()
    seen = set()

    for _ in range(20):
        if pid in seen:
            break
        seen.add(pid)

        try:
            with open(f"/proc/{pid}/cmdline", "rb") as f:
                cmdline = f.read().decode("utf-8", errors="replace")
            for agent_name in known_agents:
                if agent_name in cmdline.lower():
                    return known_agents[agent_name]
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


def _instance_has_cwd(info, cwd):
    """Quick check: ping the instance and ask for sessions matching cwd."""
    try:
        result = call_bus({"type": "list_sessions", "cwd": cwd}, bus_info=info, timeout=3)
        sessions = result.get("sessions", [])
        return len(sessions) > 0
    except Exception:
        return False


def call_bus(command, bus_info=None, timeout=DEFAULT_TIMEOUT, cwd=None):
    """
    Send a command to the Warp LocalAgentBus and return the response.

    Args:
        command: dict with the bus command (must include 'type')
        bus_info: pre-loaded bus address info (optional, auto-discovered if not given)
        timeout: socket timeout in seconds
        cwd: working directory for multi-instance discovery

    Returns:
        dict with the bus response
    """
    if bus_info is None:
        bus_info = read_bus_address(cwd=cwd)

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


def ask(provider, prompt, req_id=None, caller=None, cwd=None, session_id=None, queue=False, bus_info=None):
    """Send an ask command to a CLI agent."""
    import uuid

    if req_id is None:
        ts = time.strftime("%Y%m%d-%H%M%S")
        req_id = f"{ts}-{uuid.uuid4().hex[:8]}"

    caller_identity = detect_caller_identity(caller)

    cmd = {
        "type": "ask",
        "provider": provider,
        "prompt": prompt,
        "req_id": req_id,
        "caller": caller_identity["caller"],
        "queue": queue,
    }
    if caller_identity["caller_terminal_view_id"] is not None:
        cmd["caller_terminal_view_id"] = caller_identity["caller_terminal_view_id"]
    if caller_identity["caller_session_id"]:
        cmd["caller_session_id"] = caller_identity["caller_session_id"]
    if caller_identity["caller_cwd"]:
        cmd["caller_cwd"] = caller_identity["caller_cwd"]
    if cwd:
        cmd["cwd"] = cwd
    if session_id:
        cmd["session_id"] = session_id

    return call_bus(cmd, bus_info=bus_info, cwd=cwd), req_id


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

    return call_bus(cmd, bus_info=bus_info, cwd=cwd)


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

    return call_bus(cmd, bus_info=bus_info, cwd=cwd)


def list_sessions(cwd=None, bus_info=None):
    """List all active CLI agent sessions."""
    cmd = {"type": "list_sessions"}
    if cwd:
        cmd["cwd"] = cwd

    return call_bus(cmd, bus_info=bus_info, cwd=cwd)


def reply(req_id, content, caller=None, bus_info=None):
    """Submit a reply for a pending request via warp-reply.

    This is the explicit reply path — agent calls warp-reply instead of
    relying on terminal marker parsing.
    """
    caller_identity = detect_caller_identity(caller)

    caller_cwd = caller_identity["caller_cwd"]

    cmd = {
        "type": "reply",
        "req_id": req_id,
        "content": content,
        "caller": caller_identity["caller"],
    }
    if caller_identity["caller_terminal_view_id"] is not None:
        cmd["caller_terminal_view_id"] = caller_identity["caller_terminal_view_id"]
    if caller_identity["caller_session_id"]:
        cmd["caller_session_id"] = caller_identity["caller_session_id"]
    if caller_cwd:
        cmd["cwd"] = caller_cwd

    return call_bus(cmd, bus_info=bus_info, cwd=caller_cwd)


def cancel(req_id, bus_info=None):
    """Cancel a pending request."""
    cmd = {
        "type": "cancel",
        "req_id": req_id,
    }

    return call_bus(cmd, bus_info=bus_info)


def launch(provider, prompt=None, cwd=None, bus_info=None, auto_tab=True, alias=None):
    """Launch a new CLI agent session.

    If auto_tab=True and no idle terminal is available, automatically opens
    a new Warp tab and retries.
    """
    cmd = {
        "type": "launch",
        "provider": provider,
    }
    if alias:
        cmd["alias"] = alias
    if prompt:
        cmd["prompt"] = prompt
    if cwd:
        cmd["cwd"] = cwd

    result = call_bus(cmd, bus_info=bus_info, cwd=cwd)

    # If no idle terminal, try opening a new Warp tab and retry.
    if auto_tab and not result.get("ok") and "no idle terminal" in result.get("message", ""):
        _open_new_warp_tab()
        time.sleep(3)
        result = call_bus(cmd, bus_info=bus_info, cwd=cwd)

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


def wait(req_id=None, timeout=300, bus_info=None, cwd=None):
    """Block until a request completes, then return the result."""
    cmd = {
        "type": "wait",
        "timeout_ms": timeout * 1000,
    }
    if req_id:
        cmd["req_id"] = req_id

    return call_bus(cmd, bus_info=bus_info, timeout=timeout + 5, cwd=cwd)


def close_pane(view_id, bus_info=None):
    """Close a specific pane by its view ID."""
    cmd = {
        "type": "close_pane",
        "view_id": view_id,
    }

    return call_bus(cmd, bus_info=bus_info)


def close_session(provider, view_id, bus_info=None):
    """Close a pane and deregister its session."""
    cmd = {
        "type": "close_session",
        "provider": provider,
        "view_id": view_id,
    }

    return call_bus(cmd, bus_info=bus_info)


# ---------------------------------------------------------------------------
# Tab Config helpers
# ---------------------------------------------------------------------------

def _warp_tab_configs_dir():
    """Return the Warp tab_configs directory path.

    Resolution order:
    1. WARP_CCB_TAB_CONFIGS_DIR env var (explicit override)
    2. Detect from bus-address.json data_dir hint
    3. Platform default paths

    On Windows (Stable): %LOCALAPPDATA%\\dev.warp.Warp-Stable\\tab_configs\\
    On Windows (OSS):     %LOCALAPPDATA%\\warp\\WarpOss\\tab_configs\\
    On macOS/Linux:       ~/.warp/tab_configs/  (or ~/.warp-oss/tab_configs/)
    """
    env = os.environ.get("WARP_CCB_TAB_CONFIGS_DIR")
    if env:
        return env

    # Try known channel-specific defaults
    if sys.platform == "win32":
        local = os.environ.get("LOCALAPPDATA", os.path.expanduser("~\\AppData\\Local"))
        candidates = [
            os.path.join(local, "dev.warp.Warp-Stable", "tab_configs"),
            os.path.join(local, "warp", "WarpOss", "tab_configs"),
        ]
    else:
        home = os.path.expanduser("~")
        candidates = [
            os.path.join(home, ".warp", "tab_configs"),
            os.path.join(home, ".warp-oss", "tab_configs"),
        ]

    for c in candidates:
        if os.path.isdir(c):
            return c

    # Return first candidate even if it doesn't exist yet (will be created)
    return candidates[0]


def _templates_dir():
    """Return the bundled CCB tab config templates directory."""
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "tab_config_templates")


def generate_tab_config(name, panes, title=None, color=None, params=None, write=True):
    """Generate a Warp Tab Config TOML string from a programmatic pane description.

    Args:
        name: Display name for the tab config.
        panes: List of dicts describing pane layout. Each dict has:
            - id (str): Unique pane identifier.
            - For leaf panes: type ("terminal"/"agent"/"cloud"), directory (str), commands (list).
            - For split panes: split ("horizontal"/"vertical"), children (list of child id strings).
        title: Optional tab title template. Defaults to name.
        color: Optional tab color.
        params: Optional dict of {param_name: {"description": str, "default": str, "type": str}}.
        write: If True, write the TOML to the Warp tab_configs directory.

    Returns:
        The generated TOML string.
    """
    import io

    lines = []
    lines.append(f'name = "{name}"')
    lines.append(f'title = "{title or name}"')
    if color:
        lines.append(f'color = "{color}"')

    for pane in panes:
        lines.append("")
        lines.append("[[panes]]")
        lines.append(f'id = "{pane["id"]}"')

        if "split" in pane:
            # Split (branch) node
            lines.append(f'split = "{pane["split"]}"')
            children = pane["children"]
            children_str = ", ".join(f'"{c}"' for c in children)
            lines.append(f"children = [{children_str}]")
        else:
            # Leaf node
            pane_type = pane.get("type", "terminal")
            lines.append(f'type = "{pane_type}"')
            if "directory" in pane:
                lines.append(f'directory = "{pane["directory"]}"')
            if pane.get("is_focused"):
                lines.append("is_focused = true")
            commands = pane.get("commands", [])
            if commands:
                commands_str = ", ".join(f'"{c}"' for c in commands)
                lines.append(f"commands = [{commands_str}]")

    if params:
        for param_name, param_def in params.items():
            lines.append("")
            lines.append(f"[params.{param_name}]")
            if "type" in param_def:
                lines.append(f'type = "{param_def["type"]}"')
            if "description" in param_def:
                lines.append(f'description = "{param_def["description"]}"')
            if "default" in param_def:
                lines.append(f'default = "{param_def["default"]}"')

    toml_str = "\n".join(lines) + "\n"

    if write:
        config_dir = _warp_tab_configs_dir()
        os.makedirs(config_dir, exist_ok=True)
        safe_name = name.lower().replace(" ", "_")
        out_path = os.path.join(config_dir, f"{safe_name}.toml")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(toml_str)

    return toml_str


def install_tab_configs(force=False):
    """Install bundled CCB tab config templates into Warp's tab_configs directory.

    Copies .toml files from ccb-bridge/tab_config_templates/ to the directory
    Warp monitors (~/.warp/tab_configs/ on Unix, %LOCALAPPDATA%/.../tab_configs/ on Windows).
    Warp auto-detects changes via its filesystem watcher.

    Args:
        force: If True, overwrite existing files. If False, skip files that already exist.

    Returns:
        List of installed config names (filenames without .toml extension).
    """
    import shutil

    templates = _templates_dir()
    if not os.path.isdir(templates):
        return []

    config_dir = _warp_tab_configs_dir()
    os.makedirs(config_dir, exist_ok=True)

    installed = []
    for fname in sorted(os.listdir(templates)):
        if not fname.endswith(".toml"):
            continue
        src = os.path.join(templates, fname)
        dst = os.path.join(config_dir, fname)
        if os.path.exists(dst) and not force:
            continue
        shutil.copy2(src, dst)
        installed.append(fname[:-5])  # strip .toml

    return installed


def list_tab_configs(config_dir=None):
    """Scan a directory for Warp tab config .toml files and return metadata.

    Args:
        config_dir: Directory to scan. Defaults to Warp's tab_configs directory.

    Returns:
        List of dicts with keys: name, title, color, path.
        Only returns files with a valid 'name' field.
    """
    config_dir = config_dir or _warp_tab_configs_dir()
    if not os.path.isdir(config_dir):
        return []

    configs = []
    for fname in sorted(os.listdir(config_dir)):
        if not fname.endswith(".toml"):
            continue
        fpath = os.path.join(config_dir, fname)
        try:
            with open(fpath, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError:
            continue

        # Minimal parse: extract name, title, color from first lines
        meta = {"path": fpath, "filename": fname}
        for line in content.splitlines():
            stripped = line.strip()
            if stripped.startswith("name = "):
                meta["name"] = stripped.split("=", 1)[1].strip().strip('"')
            elif stripped.startswith("title = "):
                meta["title"] = stripped.split("=", 1)[1].strip().strip('"')
            elif stripped.startswith("color = "):
                meta["color"] = stripped.split("=", 1)[1].strip().strip('"')
            elif stripped.startswith("[[") or stripped.startswith("["):
                break  # past the header section

        if "name" in meta:
            configs.append(meta)

    return configs


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

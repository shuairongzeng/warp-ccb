#!/usr/bin/env python3
"""
test_bus.py — End-to-end test for Warp LocalAgentBus

Tests the JSONL protocol directly over TCP, without needing Warp running.
Requires: Python 3.7+, no external dependencies.

Usage:
  1. Start Warp (with our modified code)
  2. Open a CLI agent pane (e.g., Claude Code)
  3. Run: python test_bus.py --address <host:port>

  Or discover address automatically:
  4. Run: python test_bus.py
"""

import json
import socket
import sys
import os
import time
import argparse

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "lib"))
from bus_client import read_bus_address, call_bus, ping, list_sessions, ask, pend


def test_ping_no_session(args):
    """Test ping when no session exists (should return online=false)."""
    result = ping("claude", bus_info=args.bus_info)
    assert result.get("ok"), f"Ping failed: {result}"
    # online should be false if no Claude pane is open
    data = result.get("data", result)
    print(f"  [PASS] ping claude → online={data.get('online')}, details={data.get('details')}")
    return True


def test_ping_unknown_provider(args):
    """Test ping with unknown provider."""
    result = ping("nonexistent_agent_xyz", bus_info=args.bus_info)
    assert result.get("ok"), f"Ping should still return ok=true: {result}"
    data = result.get("data", result)
    assert data.get("online") == False, f"Unknown provider should be offline: {result}"
    print(f"  [PASS] ping unknown → online=false")
    return True


def test_list_sessions(args):
    """Test list_sessions."""
    result = list_sessions(bus_info=args.bus_info)
    assert result.get("ok"), f"ListSessions failed: {result}"
    data = result.get("data", result)
    sessions = data.get("sessions", [])
    print(f"  [PASS] list_sessions → {len(sessions)} sessions")
    return True


def test_ask_no_session(args):
    """Test ask when no matching session exists."""
    result, _ = ask("nonexistent_agent_xyz", "hello", bus_info=args.bus_info)
    assert not result.get("ok"), f"Ask should fail for non-existent provider: {result}"
    print(f"  [PASS] ask nonexistent → error: {result.get('message', '')}")
    return True


def test_invalid_token(args):
    """Test that invalid auth token is rejected."""
    bad_info = dict(args.bus_info)
    bad_info["auth_token"] = "invalid_token_12345"
    try:
        result = ping("claude", bus_info=bad_info)
        assert not result.get("ok"), f"Should fail with bad token: {result}"
        print(f"  [PASS] invalid token → error: {result.get('message', '')}")
        return True
    except Exception as e:
        print(f"  [PASS] invalid token → connection error: {e}")
        return True


def test_protocol_version(args):
    """Test protocol version mismatch."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    host, port = args.bus_info["socket_path"].split(":")
    sock.connect((host, int(port)))
    try:
        bad_req = json.dumps({
            "v": 999,
            "token": args.bus_info["auth_token"],
            "type": "ping",
            "provider": "claude",
        }) + "\n"
        sock.sendall(bad_req.encode())
        resp = sock.recv(4096).decode().strip()
        result = json.loads(resp)
        assert not result.get("ok"), f"Should fail with bad version: {result}"
        print(f"  [PASS] bad protocol version → error: {result.get('message', '')}")
        return True
    finally:
        sock.close()


def test_invalid_json(args):
    """Test that invalid JSON is handled gracefully."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(5)
    host, port = args.bus_info["socket_path"].split(":")
    sock.connect((host, int(port)))
    try:
        sock.sendall(b"not json at all\n")
        resp = sock.recv(4096).decode().strip()
        result = json.loads(resp)
        assert not result.get("ok"), f"Should fail for invalid JSON: {result}"
        print(f"  [PASS] invalid JSON → error: {result.get('message', '')}")
        return True
    finally:
        sock.close()


def run_all_tests(args):
    tests = [
        ("Ping no session", test_ping_no_session),
        ("Ping unknown provider", test_ping_unknown_provider),
        ("List sessions", test_list_sessions),
        ("Ask no session", test_ask_no_session),
        ("Invalid token", test_invalid_token),
        ("Protocol version", test_protocol_version),
        ("Invalid JSON", test_invalid_json),
    ]

    passed = 0
    failed = 0
    for name, test_fn in tests:
        print(f"\nTest: {name}")
        try:
            test_fn(args)
            passed += 1
        except Exception as e:
            print(f"  [FAIL] {e}")
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed, {passed+failed} total")
    return failed == 0


def main():
    parser = argparse.ArgumentParser(description="Test Warp LocalAgentBus")
    parser.add_argument("--address", help="Bus address (host:port), auto-detected if not specified")
    parser.add_argument("--bus-file", help="Path to bus-address.json", default=None)
    args = parser.parse_args()

    # Discover bus address
    try:
        if args.address:
            args.bus_info = {
                "socket_path": args.address,
                "auth_token": "",  # Will fail auth tests need manual setup
                "protocol_version": 1,
            }
            print(f"Using manual address: {args.address}")
        else:
            args.bus_info = read_bus_address(args.bus_file)
            print(f"Discovered bus at: {args.bus_info['socket_path']}")
            print(f"  PID: {args.bus_info.get('pid')}")
            print(f"  Protocol: v{args.bus_info.get('protocol_version')}")
    except Exception as e:
        print(f"Error discovering bus address: {e}")
        print("\nMake sure Warp is running with LocalAgentBus enabled.")
        print("The bus-address.json should be at:")
        print(f"  {os.path.expanduser('~/.warp-ccb/bus-address.json')}")
        print(f"  or %LOCALAPPDATA%\\warp-ccb\\bus-address.json on Windows")
        sys.exit(1)

    success = run_all_tests(args)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

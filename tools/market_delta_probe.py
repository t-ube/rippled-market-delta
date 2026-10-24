#!/usr/bin/env python3
"""Observe the local market-delta stream without making trading decisions.

Usage:
    python3 tools/market_delta_probe.py /run/xrpld/market-delta.sock

The server intentionally begins every connection with resync_required.
This observer verifies framing, monotonically increasing revisions, and
ledger transitions; it does NOT certify that the data equals an RPC snapshot.
"""
import argparse
import collections
import json
import select
import socket
import sys
import time


def observe(path: str, seconds: float) -> int:
    counts = collections.Counter()
    last_revision = None
    last_epoch = None
    errors = 0
    start = time.monotonic()
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.connect(path)
        buffer = b""
        while time.monotonic() - start < seconds:
            ready, _, _ = select.select(
                [sock], [], [], min(0.5, max(0.0, seconds - (time.monotonic() - start)))
            )
            if not ready:
                continue
            chunk = sock.recv(65536)
            if not chunk:
                print("Connection closed: resynchronization required", file=sys.stderr)
                break
            buffer += chunk
            if len(buffer) > 2_000_000:
                errors += 1
                print("FRAME_TOO_LARGE", file=sys.stderr)
                break
            while b"\n" in buffer:
                frame, buffer = buffer.split(b"\n", 1)
                now = time.monotonic()
                try:
                    message = json.loads(frame)
                except (ValueError, UnicodeError) as exc:
                    errors += 1
                    print(f"INVALID_FRAME: {exc}", file=sys.stderr)
                    return 1
                if message.get("schema") != 1:
                    errors += 1
                    print(f"INVALID_SCHEMA: {message.get('schema')}", file=sys.stderr)
                    break
                kind = message.get("kind")
                counts[kind] += 1
                if kind in ("resync_required", "ledger_reset"):
                    last_revision = None
                    last_epoch = None
                    print(f"{now - start:7.3f}s {kind}: must resync")
                    continue
                if kind != "market_delta":
                    errors += 1
                    print(f"INVALID_KIND: {kind}", file=sys.stderr)
                    break
                try:
                    revision = int(message["revision"])
                except (ValueError, TypeError, KeyError):
                    errors += 1
                    print("INVALID_REVISION", file=sys.stderr)
                    break
                if last_revision is not None and revision != last_revision + 1:
                    errors += 1
                    print(f"REVISION_GAP: {last_revision} -> {revision}", file=sys.stderr)
                epoch = (message.get("parentHash"), message.get("openLedger"))
                if last_epoch is not None and epoch != last_epoch:
                    errors += 1
                    print(f"UNSIGNALED_EPOCH: {last_epoch} -> {epoch}", file=sys.stderr)
                last_revision, last_epoch = revision, epoch
                changes = message.get("changes")
                if not isinstance(changes, list):
                    errors += 1
                    print("INVALID_CHANGES", file=sys.stderr)
                    break
                counts["entries"] += len(changes)
                if counts["market_delta"] % 50 == 1:
                    print(
                        f"{now - start:7.3f}s ledger={epoch[1]} revision={revision}"
                        f" changes={len(changes)} queued_us={message.get('queueUs')}"
                    )
    print("Counts:", dict(counts), "errors:", errors)
    return 1 if errors else 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("socket", help="UNIX socket path configured in xrpld")
    parser.add_argument("--seconds", type=float, default=30)
    args = parser.parse_args()
    return observe(args.socket, args.seconds)


if __name__ == "__main__":
    raise SystemExit(main())

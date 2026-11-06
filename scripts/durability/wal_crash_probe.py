#!/usr/bin/env python3
"""Measure which acknowledged line-protocol rows survive an Arc process crash."""

from __future__ import annotations

import argparse
import json
import os
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from collections import Counter
from pathlib import Path


def free_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def request(url: str, method: str = "GET", body: bytes | None = None, headers: dict[str, str] | None = None):
    req = urllib.request.Request(url, data=body, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as error:
        return error.code, error.read()


def wait_ready(process: subprocess.Popen, base_url: str, log_path: Path, timeout: float = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            tail = log_path.read_text(errors="replace").splitlines()[-40:]
            raise RuntimeError("Arc exited before becoming ready:\n" + "\n".join(tail))
        try:
            status, _ = request(base_url + "/ready")
            if status == 200:
                return
        except (OSError, TimeoutError):
            pass
        time.sleep(0.1)
    process.kill()
    process.wait()
    tail = log_path.read_text(errors="replace").splitlines()[-40:]
    raise RuntimeError("Arc did not become ready:\n" + "\n".join(tail))


def stop(process: subprocess.Popen) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def run(args: argparse.Namespace) -> dict[str, object]:
    binary = args.binary.resolve(strict=True)
    port = free_port()
    base_url = f"http://127.0.0.1:{port}"
    database = "durability_probe"
    with tempfile.TemporaryDirectory(prefix="arc-wal-crash-") as temp:
        root = Path(temp)
        data_dir = root / "data"
        wal_dir = root / "wal"
        log_path = root / "arc.log"
        env = {key: value for key, value in os.environ.items() if not key.startswith("ARC_")}
        env.update(
            {
                "ARC_SERVER_HOST": "127.0.0.1",
                "ARC_SERVER_PORT": str(port),
                "ARC_AUTH_ENABLED": "false",
                "ARC_TELEMETRY_ENABLED": "false",
                "ARC_STORAGE_BACKEND": "local",
                "ARC_STORAGE_LOCAL_PATH": str(data_dir),
                "ARC_BACKUP_LOCAL_PATH": str(root / "backups"),
                "ARC_WAL_DIRECTORY": str(wal_dir),
                "ARC_WAL_ENABLED": str(args.wal_enabled).lower(),
                "ARC_WAL_SYNC_MODE": args.sync_mode,
                "ARC_WAL_ACK_MODE": args.ack_mode,
                "ARC_INGEST_MAX_BUFFER_SIZE": "50000",
                "ARC_INGEST_MAX_BUFFER_AGE_MS": str(args.max_buffer_age_ms),
                "ARC_COMPACTION_HOURLY_ENABLED": "false",
                "ARC_COMPACTION_DAILY_ENABLED": "false",
            }
        )

        def start() -> subprocess.Popen:
            with log_path.open("ab") as log_file:
                process = subprocess.Popen(
                    [str(binary)],
                    cwd=root,
                    env=env,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,
                )
            try:
                wait_ready(process, base_url, log_path)
            except BaseException:
                stop(process)
                raise
            return process

        process = start()
        try:
            run_id = uuid.uuid4().hex
            now_ns = time.time_ns()
            lines = [
                f"probe,run={run_id} row_id={row_id}i {now_ns + row_id}"
                for row_id in range(args.rows)
            ]
            payload = ("\n".join(lines) + "\n").encode()
            started = time.monotonic()
            status, response = request(
                base_url + "/api/v1/write/line-protocol?precision=ns",
                "POST",
                payload,
                {"X-Arc-Database": database, "Content-Type": "text/plain"},
            )
            acknowledged_at = time.monotonic()
            if status != 204:
                raise RuntimeError(f"write returned HTTP {status}: {response.decode(errors='replace')}")

            time.sleep(args.pause_after_ack)
            process.kill()  # Deliberately bypasses Arc shutdown and buffer flushing.
            process.wait(timeout=20)
            process = start()

            query_body = json.dumps({"sql": "SELECT row_id FROM probe ORDER BY row_id"}).encode()
            query_status, query_response = request(
                base_url + "/api/v1/query",
                "POST",
                query_body,
                {"X-Arc-Database": database, "Content-Type": "application/json"},
            )
            recovered_at = time.monotonic()
            if query_status != 200:
                raise RuntimeError(
                    f"query returned HTTP {query_status}: {query_response.decode(errors='replace')}"
                )
            result = json.loads(query_response)
            if not result.get("success"):
                raise RuntimeError(f"query failed: {result}")
            ids = [int(row[0]) for row in result.get("data", [])]
            counts = Counter(ids)
            missing = args.rows - len(set(range(args.rows)) & counts.keys())
            duplicates = sum(count - 1 for count in counts.values() if count > 1)
            return {
                "wal_enabled": args.wal_enabled,
                "sync_mode": args.sync_mode,
                "ack_mode": args.ack_mode,
                "rows_sent": args.rows,
                "ack_latency_ms": round((acknowledged_at - started) * 1000, 3),
                "pause_after_ack_ms": round(args.pause_after_ack * 1000, 3),
                "rows_recovered": len(ids),
                "unique_rows_recovered": len(counts),
                "missing_rows": missing,
                "duplicate_rows": duplicates,
                "restart_and_query_seconds": round(recovered_at - acknowledged_at - args.pause_after_ack, 3),
                "max_buffer_age_ms": args.max_buffer_age_ms,
            }
        finally:
            stop(process)


def run_many(args: argparse.Namespace) -> dict[str, object]:
    # run owns its temporary directory and processes, including on failure.
    # A failed request or query raises: it is not an acknowledged data-loss run.
    results = [run(args) for _ in range(args.runs)]
    losses = [result["missing_rows"] for result in results]
    return {
        "runs": results,
        "runs_completed": len(results),
        "runs_with_loss": sum(count > 0 for count in losses),
        "lost_rows_per_run": losses,
        "runs_with_duplicates": sum(result["duplicate_rows"] > 0 for result in results),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True, help="Path to a locally built Arc binary")
    parser.add_argument("--wal-enabled", action="store_true", help="Enable the WAL for this run")
    parser.add_argument("--sync-mode", choices=("fdatasync", "fsync", "async"), default="fdatasync")
    # auto is Arc's default since #1499: durable with the WAL on, queued without.
    parser.add_argument("--ack-mode", choices=("auto", "queued", "durable"), default="auto")
    parser.add_argument("--runs", type=int, default=1, help="Independent runs, each with a fresh data directory")
    parser.add_argument("--rows", type=int, default=10000)
    parser.add_argument("--pause-after-ack", type=float, default=0)
    parser.add_argument("--max-buffer-age-ms", type=int, default=5000)
    args = parser.parse_args()
    if args.runs < 1 or args.rows < 1 or args.max_buffer_age_ms < 1 or args.pause_after_ack < 0:
        parser.error("runs, rows and max-buffer-age-ms must be positive; pause-after-ack cannot be negative")
    print(json.dumps(run_many(args), sort_keys=True))


if __name__ == "__main__":
    main()

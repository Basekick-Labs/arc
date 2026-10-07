"""Deterministic smoke regressions; no Docker or enterprise license required."""
import contextlib
import http.server
import json
import os
from pathlib import Path
import re
import sqlite3
import subprocess
import sys
import tempfile
import threading
import unittest

from smoke_coverage import check, count

ROOT = Path(__file__).resolve().parent


class CoverageTest(unittest.TestCase):
    def test_counts_fail_closed(self):
        for response in [{}, {"data": []}, {"data": [[0]], "columns": ["wrong"]},
                         {"data": [{"n": "0"}]}, {"data": [{"n": -1}]},
                         {"data": [{"n": True}]}, {"data": [{"n": 1}, {"n": 2}]}]:
            with self.subTest(response=response), self.assertRaises((ValueError, KeyError)):
                count(response, "n")
        self.assertEqual(count({"data": [{"n": 0}]}, "n"), 0)
        self.assertEqual(count({"data": [[3, 2]], "columns": ["other", "n"]}, "n"), 2)

    def test_bounded_coverage_queries(self):
        with contextlib.closing(sqlite3.connect(":memory:")) as db:
            db.execute("CREATE TABLE cpu(host TEXT)")
            start = 2**63 - 10000
            db.executemany("INSERT INTO cpu VALUES (?)", ((f"server{i}",) for i in range(start, start + 2501)))
            queries = []
            def query(sql):
                self.assertLessEqual(len(sql), 10000, "Arc SQL length limit exceeded")
                queries.append(sql)
                result = db.execute(sql)
                return {"columns": [col[0] for col in result.description], "data": result.fetchall()}
            self.assertEqual(check(query, "cpu", [(start, 2501)], 2501, "leader-crash"), (2501, 2501))
            self.assertGreater(len(queries), 1)
            db.execute("DELETE FROM cpu WHERE host = ?", (f"server{start + 2500}",))
            with self.assertRaisesRegex(ValueError, "acknowledged records missing"):
                check(query, "cpu", [(start, 2501)], 2501, "leader-crash")

    def test_no_acknowledgements_rejected(self):
        with self.assertRaisesRegex(ValueError, "no acknowledged"):
            check(lambda _: {"data": [{"n": 0, "unique_hosts": 0}]}, "cpu", [], 400, "leader-crash")


class HarnessTest(unittest.TestCase):
    def run_smoke(self, scenario, mode="replay", records=400):
        # Real HTTP and SQL exercise the script's batch bookkeeping, query
        # generation and exit status. Only Docker and sleeps are substituted.
        with tempfile.TemporaryDirectory() as tmp, contextlib.closing(sqlite3.connect(":memory:", check_same_thread=False)) as db:
            db.execute("CREATE TABLE cpu(host TEXT)")
            batch = 0
            class Handler(http.server.BaseHTTPRequestHandler):
                def log_message(self, *_):
                    pass

                def reply(self, status, data):
                    self.send_response(status)
                    self.end_headers()
                    self.wfile.write(json.dumps(data).encode())

                def do_GET(self):
                    self.reply(200, {})

                def do_POST(self):
                    nonlocal batch
                    body = self.rfile.read(int(self.headers.get("Content-Length", 0)))
                    if self.path.endswith("/query"):
                        result = db.execute(json.loads(body)["sql"])
                        self.reply(200, {"columns": [col[0] for col in result.description], "data": result.fetchall()})
                    elif self.path.endswith("/line-protocol"):
                        hosts = re.findall(r"host=(server\d+)", body.decode())
                        # Masking case: acknowledged batch 0 is lost, failed
                        # batch 1 persists instead. Global distinct counts match.
                        if not (mode in ("masked-loss", "visible-loss") and batch == 0):
                            if not (mode == "visible-loss" and batch == 1):
                                copies = 2 if mode == "replay" and scenario != "base" else 1
                                db.executemany("INSERT INTO cpu VALUES (?)", [(h,) for h in hosts] * copies)
                        status = 500 if batch == 1 and scenario != "base" else 200
                        batch += 1
                        self.reply(status, {})
                    else:
                        self.reply(200, {})
            server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
            thread = threading.Thread(target=lambda: server.serve_forever(poll_interval=0.01), daemon=True)
            thread.start()
            try:
                bindir = Path(tmp)
                docker = bindir / "docker"
                docker.write_text(f'''#!{sys.executable}
import json, os, sys
args = sys.argv[1:]
if args[0] == 'logs': print('  Admin API token: test-token')
if args[0] in ('kill', 'start'):
    with open(os.environ['EVENTS'], 'a') as out: out.write(args[0] + '\\n')
if args[0] == 'exec':
    if '/api/v1/cluster' in ' '.join(args): print(json.dumps({{'raft': {{'is_leader': args[1] == 'arc-writer1'}}}}))
    elif '/metrics' in ' '.join(args): print('arc_ingest_records_total ' + ('0' if os.environ['MODE'] == 'idle' else '100'))
    elif '/api/v1/databases' in ' '.join(args): print('200')
''')
                docker.chmod(0o755)
                sleep = bindir / "sleep"
                sleep.write_text("#!/bin/sh\nexit 0\n")
                sleep.chmod(0o755)
                events = bindir / "events"
                env = dict(os.environ, PATH=tmp + os.pathsep + os.environ["PATH"],
                           ARC_LICENSE_KEY="test", ARC_CLUSTER_SHARED_SECRET="test",
                           SCENARIO=scenario, RECORDS=str(records), FLUSH_WAIT_S="0",
                           TRAEFIK_URL=f"http://127.0.0.1:{server.server_port}",
                           MODE=mode, EVENTS=str(events), PYTHONDONTWRITEBYTECODE="1")
                result = subprocess.run(["bash", str(ROOT / "smoke.sh")], env=env,
                                        capture_output=True, text=True, timeout=30)
                return result, events.read_text() if events.exists() else ""
            finally:
                server.shutdown()
                server.server_close()
                thread.join()

    def test_replay_duplicates_and_failed_request_extras_pass(self):
        for scenario in ("leader-crash", "non-leader-crash"):
            with self.subTest(scenario=scenario):
                result, events = self.run_smoke(scenario)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(events, "kill\nstart\n")
                self.assertIn("all 300 acknowledged hosts", result.stderr)

    def test_missing_acknowledged_records_fail_even_when_totals_match(self):
        for scenario in ("leader-crash", "non-leader-crash"):
            for mode in ("masked-loss", "visible-loss"):
                with self.subTest(scenario=scenario, mode=mode):
                    result, _ = self.run_smoke(scenario, mode)
                    self.assertNotEqual(result.returncode, 0)
                    self.assertIn("durability violation", result.stderr)

    def test_base_exits_successfully(self):
        result, events = self.run_smoke("base")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("exact count match", result.stderr)
        self.assertEqual(events, "")

    def test_idle_target_rejected(self):
        result, events = self.run_smoke("non-leader-crash", "idle")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("refusing an idle-writer", result.stderr)
        self.assertEqual(events, "")

    def test_untriggered_crash_rejected(self):
        result, events = self.run_smoke("leader-crash", records=100)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("crash was never triggered", result.stderr)
        self.assertEqual(events, "")


if __name__ == "__main__":
    unittest.main()

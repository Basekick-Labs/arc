"""Deterministic tests; no Arc server or crash timing is needed."""
import argparse
import contextlib
import io
import unittest
from unittest.mock import patch

import wal_crash_probe as probe


class RepeatedRunsTest(unittest.TestCase):
    def test_loss_and_duplicate_counts_preserve_each_run(self):
        results = [
            {"missing_rows": 0, "duplicate_rows": 0},
            {"missing_rows": 100000, "duplicate_rows": 0},
            {"missing_rows": 3, "duplicate_rows": 2},
        ]
        args = argparse.Namespace(runs=3)
        with patch.object(probe, "run", side_effect=results) as run:
            summary = probe.run_many(args)
        self.assertEqual(run.call_count, 3)
        self.assertEqual(summary["runs"], results)
        self.assertEqual(summary["lost_rows_per_run"], [0, 100000, 3])
        self.assertEqual(summary["runs_with_loss"], 2)
        self.assertEqual(summary["runs_with_duplicates"], 1)
        self.assertEqual(summary["runs_completed"], 3)

    def test_failed_run_is_not_reported_as_acknowledged_loss(self):
        with patch.object(probe, "run", side_effect=RuntimeError("write returned HTTP 500")):
            with self.assertRaisesRegex(RuntimeError, "HTTP 500"):
                probe.run_many(argparse.Namespace(runs=3))

    def test_http_failure_stops_child_without_crash_or_restart(self):
        child = unittest.mock.Mock()
        child.poll.return_value = None
        args = argparse.Namespace(binary=probe.Path(__file__), wal_enabled=True,
            sync_mode="fsync", ack_mode="durable", max_buffer_age_ms=5000, rows=2)
        with patch.object(probe, "free_port", return_value=12345), patch.object(probe.subprocess, "Popen", return_value=child) as start:
            with patch.object(probe, "wait_ready"), patch.object(probe, "request", return_value=(500, b"refused")):
                with self.assertRaisesRegex(RuntimeError, "HTTP 500: refused"):
                    probe.run(args)
        start.assert_called_once()
        child.kill.assert_not_called()
        child.terminate.assert_called_once()

    def test_successful_repeats_use_fresh_directories(self):
        roots = []
        def start(*args, **kwargs):
            roots.append(kwargs["cwd"])
            child = unittest.mock.Mock()
            child.poll.return_value = None
            return child
        args = argparse.Namespace(binary=probe.Path(__file__), wal_enabled=True,
            sync_mode="fsync", ack_mode="durable", max_buffer_age_ms=5000,
            rows=2, runs=2, pause_after_ack=0)
        responses = [(204, b""), (200, b'{"success": true, "data": [[0], [1]]}')] * 2
        with patch.object(probe, "free_port", return_value=12345), patch.object(probe.subprocess, "Popen", side_effect=start):
            with patch.object(probe, "wait_ready"), patch.object(probe, "request", side_effect=responses):
                result = probe.run_many(args)
        self.assertEqual(result["lost_rows_per_run"], [0, 0])
        self.assertEqual(roots[0], roots[1])  # crash restart uses the same directory
        self.assertEqual(roots[2], roots[3])
        self.assertNotEqual(roots[0], roots[2])  # next run starts fresh
        self.assertTrue(all(not root.exists() for root in roots))

    def test_nonpositive_runs_rejected_before_start(self):
        for count in ("0", "-1"):
            with self.subTest(count=count), patch("sys.argv", ["probe", "--binary", "/unused", "--runs", count]):
                with patch.object(probe, "run") as run, contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as error:
                        probe.main()
                self.assertEqual(error.exception.code, 2)
                run.assert_not_called()

    def test_start_failure_stops_child_and_removes_fresh_directories(self):
        roots = []
        children = []
        def start(*args, **kwargs):
            roots.append(kwargs["cwd"])
            child = unittest.mock.Mock()
            child.poll.return_value = None
            children.append(child)
            return child
        args = argparse.Namespace(binary=probe.Path(__file__), wal_enabled=True,
            sync_mode="fsync", ack_mode="durable", max_buffer_age_ms=5000)
        with patch.object(probe, "free_port", return_value=12345), patch.object(probe.subprocess, "Popen", side_effect=start):
            with patch.object(probe, "wait_ready", side_effect=RuntimeError("not ready")):
                for _ in range(2):
                    with self.assertRaisesRegex(RuntimeError, "not ready"):
                        probe.run(args)
        self.assertNotEqual(roots[0], roots[1])
        for root, child in zip(roots, children):
            self.assertFalse(root.exists())
            child.terminate.assert_called_once()
            child.wait.assert_called_once_with(timeout=20)


if __name__ == "__main__":
    unittest.main()

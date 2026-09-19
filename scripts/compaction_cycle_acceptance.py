#!/usr/bin/env python3
"""Native Arc acceptance test for compaction cycle budgets (no containers).

Build with `go build -tags=duckdb_arrow -o /tmp/arc ./cmd/arc`, then run:
  python3 scripts/compaction_cycle_acceptance.py --arc /tmp/arc --output /tmp/arc-cycle-run
The output directory must be new. All generated data, configuration, process logs,
and timestamped results remain there for inspection. Only loopback is used.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
import re
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request


def utc():
    return dt.datetime.now(dt.timezone.utc).isoformat()


class Arc:
    def __init__(self, binary, root):
        self.binary, self.root = binary, root
        self.data = root / 'data' / 'arc'
        self.process = None
        self.start_count = 0
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            self.port = sock.getsockname()[1]
        self.base = f'http://127.0.0.1:{self.port}'

    def start(self, timeout):
        self.start_count += 1
        # Each restart changes only cycle_timeout. Resources stay fixed.
        (self.root / 'arc.toml').write_text(f'''[server]
host = "127.0.0.1"
port = {self.port}
[log]
level = "info"
format = "json"
[database]
memory_limit = "512MB"
thread_count = 2
max_connections = 4
[auth]
enabled = false
[storage]
backend = "local"
local_path = "./data/arc"
[ingest]
max_buffer_size = 1000000
max_buffer_age_ms = 60000
flush_workers = 2
[compaction]
enabled = true
hourly_enabled = true
hourly_schedule = "0 0 1 1 *"
hourly_min_files = 2
hourly_min_age_hours = 1
daily_enabled = false
max_concurrent = 1
max_files_per_batch = 7
memory_limit = "512MB"
threads = 2
cycle_timeout = "{timeout}"
[telemetry]
enabled = false
[retention]
enabled = false
[continuous_query]
enabled = false
[cache]
enabled = false
''')
        env = {k: v for k, v in os.environ.items() if not k.startswith('ARC_')}
        self.log = open(self.root / f'arc-{self.start_count}.log', 'w')
        self.process = subprocess.Popen([str(self.binary)], cwd=self.root, env=env,
                                        stdout=self.log, stderr=subprocess.STDOUT)
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                raise AssertionError(f'Arc startup failed; see {self.log.name}')
            try:
                self.request('/health')
                return
            except (OSError, urllib.error.URLError):
                time.sleep(.05)
        raise AssertionError('Arc did not become healthy')

    def stop(self):
        if self.process is None:
            return
        self.process.send_signal(signal.SIGTERM)
        try:
            self.process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            self.process.kill()
            self.process.wait()
            raise AssertionError('Arc failed to stop gracefully')
        finally:
            exit_code = self.process.returncode
            self.log.close()
            self.process = None
        assert exit_code == 0, f'Arc exited with {exit_code}'

    def request(self, path, body=None, headers=None):
        if isinstance(body, dict):
            body = json.dumps(body).encode()
            headers = {'Content-Type': 'application/json', **(headers or {})}
        req = urllib.request.Request(self.base + path, data=body, headers=headers or {})
        with urllib.request.urlopen(req, timeout=60) as response:
            payload = response.read()
            return json.loads(payload) if payload else None

    def files(self, table):
        return sorted((self.data / 'acceptance' / table).rglob('*.parquet'))

    def seed(self, table, files=14, rows=500, offset=0):
        # Old event time ensures hourly-tier eligibility without altering ages.
        base = int(dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc).timestamp()) * 10**9
        for batch in range(files):
            previous = len(self.files(table))
            lines = []
            for row in range(rows):
                value = offset + batch * rows + row
                lines.append(f'{table},source=synthetic value={value}i {base + value * 1000}')
            self.request('/api/v1/write/line-protocol', '\n'.join(lines).encode(),
                         {'Content-Type': 'text/plain', 'x-arc-database': 'acceptance'})
            self.request('/api/v1/write/line-protocol/flush', b'')
            deadline = time.monotonic() + 10
            while len(self.files(table)) <= previous:
                assert time.monotonic() < deadline, 'flush failed to produce a file'
                time.sleep(.01)

        # Hourly safety checks both event partition age and the flush timestamp
        # embedded in filenames. Age ONLY these isolated test fixtures so this
        # acceptance test need not idle for an hour after producing each file.
        for path in self.files(table):
            name = re.sub(r'_\d{8}_\d{6}_', '_20260101_000000_', path.name)
            if name != path.name:
                path.rename(path.with_name(name))

    def contents(self, table):
        result = self.request('/api/v1/query', {'sql': f'SELECT epoch_us(time), source, value, count(*) FROM acceptance.{table} GROUP BY time, source, value ORDER BY time, source, value'})
        assert result.get('success', True), result
        return result['data']

    def cycle(self, table):
        before = self.request('/api/v1/compaction/stats')['current_cycle_id']
        started = utc()
        self.request('/api/v1/compaction/trigger?' + urllib.parse.urlencode(
            {'database': 'acceptance', 'measurement': table, 'tier': 'hourly'}), b'')
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            assert self.process.poll() is None, 'Arc exited during compaction'
            stats = self.request('/api/v1/compaction/stats')
            outcome = stats['last_cycle']
            if outcome['cycle_id'] > before and not stats['cycle_running']:
                return {'started_utc': started, 'finished_utc': utc(), **outcome}
            time.sleep(.02)
        raise AssertionError('cycle did not terminate')


def cgroup_events():
    try:
        group = next(line.split(':', 2)[2] for line in Path('/proc/self/cgroup').read_text().splitlines() if line.startswith('0::'))
        path = Path('/sys/fs/cgroup') / group.lstrip('/') / 'memory.events'
        return {'path': str(path), 'events': dict(line.split() for line in path.read_text().splitlines())}
    except (OSError, StopIteration):
        return None


def run(binary, root):
    root.mkdir(parents=True, exist_ok=False)
    arc = Arc(binary, root)
    report = {'started_utc': utc(), 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'resources': {'database_memory': '512MB', 'compaction_memory': '512MB', 'threads': 2, 'concurrency': 1, 'batch_files': 7},
              'cgroup_before': cgroup_events(), 'cycles': [],
              'environment': 'native local process; hourly tier triggered manually; no Kubernetes/container limit guarantee'}
    try:
        arc.start('30s')
        for index in range(3):
            table = f'cycle_{index}'
            arc.seed(table)
            expected = arc.contents(table)
            count_before = len(arc.files(table))
            outcome = arc.cycle(table)
            count_after = len(arc.files(table))
            assert outcome['status'] == 'completed', outcome
            assert outcome['failed_batches'] == outcome['discovery_errors'] == outcome['interrupted_batches'] == 0, outcome
            assert count_after < count_before, (count_before, count_after)
            assert arc.contents(table) == expected, 'compaction changed logical rows'
            report['cycles'].append({'table': table, 'files_before': count_before, 'files_after': count_after, **outcome})

        # Build an interruption workload while changing no resource settings.
        arc.seed('deadline', files=42, rows=2000)
        expected = arc.contents('deadline')
        count_before = len(arc.files('deadline'))
        arc.stop()
        arc.start('100ms')
        interrupted = arc.cycle('deadline')
        assert interrupted['status'] == 'timed_out', interrupted
        assert interrupted['started_batches'] > 0, 'deadline expired before exercising a batch'
        assert interrupted['failed_batches'] == 0, interrupted
        arc.stop()
        arc.start('30s')
        recovered = arc.cycle('deadline')
        assert recovered['status'] == 'completed', recovered
        assert recovered['failed_batches'] == recovered['discovery_errors'] == 0, recovered
        assert arc.contents('deadline') == expected, 'deadline/recovery lost or duplicated rows'
        assert len(arc.files('deadline')) < count_before
        report['deadline_and_retry'] = {'interrupted': interrupted, 'retry': recovered,
                                        'files_before': count_before, 'files_after': len(arc.files('deadline'))}

        # Simulate the durable post-upload boundary with REAL Parquet files:
        # one compacted output plus its matching raw inputs and manifest.
        arc.seed('recovery', files=7)
        raw = [(p.relative_to(arc.data).as_posix(), p.read_bytes()) for p in arc.files('recovery')]
        expected = arc.contents('recovery')
        assert arc.cycle('recovery')['status'] == 'completed'
        output = arc.files('recovery')[0]
        arc.stop()
        for key, data in raw:
            path = arc.data / key
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        manifest_path = arc.data / '_compaction_state/hourly/acceptance/acceptance-recovery.json'
        manifest_path.parent.mkdir(parents=True, exist_ok=True)
        manifest_path.write_text(json.dumps({'database': 'acceptance', 'measurement': 'recovery',
            'tier': 'hourly', 'job_id': 'acceptance-recovery', 'status': 'pending', 'created_at': utc(),
            'partition_path': 'acceptance/recovery/2026/01/01/00', 'input_files': [k for k, _ in raw],
            'output_path': output.relative_to(arc.data).as_posix(), 'output_size': output.stat().st_size}))
        arc.start('30s')
        unrelated = arc.cycle('cycle_0')
        assert manifest_path.exists(), 'scoped cycle recovered an unrelated measurement'
        assert all((arc.data / key).exists() for key, _ in raw)
        recovery = arc.cycle('recovery')
        assert recovery['status'] == 'completed', recovery
        assert not manifest_path.exists()
        assert all(not (arc.data / key).exists() for key, _ in raw)
        assert arc.contents('recovery') == expected, 'manifest recovery lost or duplicated rows'
        report['real_parquet_recovery'] = {'unrelated_cycle': unrelated, 'recovery_cycle': recovery,
                                          'logical_rows': len(expected), 'raw_inputs_recovered': len(raw)}
        assert arc.process.poll() is None
        report['planned_process_starts'] = arc.start_count
        report['unexpected_process_exits'] = 0
        after = cgroup_events()
        before = report['cgroup_before']
        if before and after and before['path'] == after['path']:
            deltas = {key: int(after['events'].get(key, 0)) - int(before['events'].get(key, 0))
                      for key in ('max', 'oom', 'oom_kill')}
            report['observed_cgroup_event_deltas'] = deltas
            assert all(value == 0 for value in deltas.values()), deltas
        report['status'] = 'passed'
    except Exception as error:
        report['status'] = 'failed'
        report['error'] = repr(error)
        raise
    finally:
        try:
            arc.stop()
        except Exception as error:
            report['status'] = 'failed'
            report['shutdown_error'] = repr(error)
            raise
        finally:
            report['finished_utc'] = utc()
            report['cgroup_after'] = cgroup_events()
            (root / 'results.json').write_text(json.dumps(report, indent=2))
            print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--arc', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    run(args.arc.resolve(), args.output.resolve())

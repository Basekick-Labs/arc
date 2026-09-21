#!/usr/bin/env python3
"""Native Arc acceptance test for range-independent field binding (#914, no containers).

Build with `go build -tags=duckdb_arrow -o /tmp/arc ./cmd/arc`, then run:
  python3 scripts/range_schema_acceptance.py --arc /tmp/arc --output /tmp/arc-range-schema-run
The output directory must be new. All generated data, configuration, process logs,
and timestamped results remain there for inspection. Only loopback is used.

The fixtures come from a field report against 26.09.1, where a projection of a
field that no Parquet file in the selected time range carried failed with
`Binder Error: Referenced column ... not found` while a wider range succeeded
with NULLs, so a dashboard panel worked or broke with the zoom level. The
scenarios reproduce that report: a late field inside one day, a field first
written 59 days after the earlier day was compacted, a field that stops being
written, and the same queries after daily compaction and process restarts.
Expectations are the 26.09.2 contract: a registered field binds as a typed
NULL over any range and an unknown field still raises a Binder Error. The
final phase restarts Arc with `query.stable_schema = false` and requires the
26.09.1 Binder Error back, which proves the assertions observe the feature.
"""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

JAN = int(dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc).timestamp()) * 10**9
MAR = int(dt.datetime(2026, 3, 1, tzinfo=dt.timezone.utc).timestamp()) * 10**9
HOUR = 3600 * 10**9
SECOND = 10**9
JAN_ONLY = "time >= '2026-01-01T00:00:00Z' AND time < '2026-01-02T00:00:00Z'"
MAR_ONLY = "time >= '2026-03-01T00:00:00Z' AND time < '2026-03-02T00:00:00Z'"
SPAN = "time >= '2026-01-01T00:00:00Z' AND time < '2026-03-02T00:00:00Z'"
EARLY_HOUR = "time >= '2026-01-01T00:00:00Z' AND time < '2026-01-01T01:00:00Z'"
LATE_HOUR = "time >= '2026-01-01T01:00:00Z' AND time < '2026-01-01T02:00:00Z'"


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

    def start(self, compaction=True, stable_schema=True):
        self.start_count += 1
        # Restarts change only the two settings under test. Resources stay fixed.
        # The hourly tier stays off so the only candidates are the daily ones the
        # scenarios create; the daily schedule never fires on its own. The
        # fixture days are months old, so the daily tier skips the flush-age
        # check on their freshly written files (the default, made explicit).
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
[query]
stable_schema = {str(stable_schema).lower()}
[compaction]
enabled = {str(compaction).lower()}
hourly_enabled = false
daily_enabled = true
daily_schedule = "0 0 1 1 *"
daily_min_files = 12
daily_min_age_hours = 24
daily_skip_file_age_check_days = 7
max_concurrent = 1
memory_limit = "512MB"
threads = 2
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

    def restart(self, **settings):
        self.stop()
        self.start(**settings)

    def request(self, path, body=None, headers=None):
        if isinstance(body, dict):
            body = json.dumps(body).encode()
            headers = {'Content-Type': 'application/json', **(headers or {})}
        req = urllib.request.Request(self.base + path, data=body, headers=headers or {})
        with urllib.request.urlopen(req, timeout=60) as response:
            payload = response.read()
            return json.loads(payload) if payload else None

    def files(self, database, measurement):
        return sorted((self.data / database / measurement).rglob('*.parquet'))

    def write(self, database, measurement, lines):
        # One synchronous flush per call keeps each call a distinct Parquet file,
        # the same shape as the `arcli import lp` runs in the field report.
        previous = len(self.files(database, measurement))
        self.request('/api/v1/write/line-protocol', '\n'.join(lines).encode(),
                     {'Content-Type': 'text/plain', 'x-arc-database': database})
        self.request('/api/v1/write/line-protocol/flush', b'')
        deadline = time.monotonic() + 10
        while len(self.files(database, measurement)) <= previous:
            assert time.monotonic() < deadline, 'flush failed to produce a file'
            time.sleep(.01)

    def write_day(self, database, measurement, tag, fields, base, count=12):
        # Twelve files in one day reach the daily tier's minimum; one second
        # between rows keeps every row distinct through compaction.
        for i in range(1, count + 1):
            self.write(database, measurement,
                       [f'{measurement},source={tag} stable={i}i{fields} {base + i * SECOND}'])

    def query(self, sql):
        result = self.request('/api/v1/query', {'sql': sql})
        assert result.get('success', True), result
        return {'columns': result['columns'], 'data': result['data']}

    def query_error(self, sql):
        try:
            result = self.request('/api/v1/query', {'sql': sql})
        except urllib.error.HTTPError as error:
            body = json.loads(error.read())
            assert error.code == 500 and not body.get('success', True), body
            return body['error']
        raise AssertionError(f'query succeeded but an error was expected: {result}')

    def schema(self, database, measurement):
        return self.request(f'/api/v1/databases/{database}/measurements/{measurement}/schema')

    def candidates(self, database):
        found = self.request('/api/v1/compaction/candidates')['candidates']
        return [c for c in found if c['database'] == database]

    def compact_day(self, database, measurement, day):
        found = [c for c in self.candidates(database) if c['measurement'] == measurement]
        assert [c['partition_path'].endswith(f'{measurement}/{day}') for c in found] == [True], found
        candidate = found[0]
        assert candidate['tier'] == 'daily' and candidate['file_count'] == 12, candidate
        before = self.request('/api/v1/compaction/stats')['current_cycle_id']
        self.request('/api/v1/compaction/trigger?' + urllib.parse.urlencode(
            {'database': database, 'measurement': measurement, 'tier': 'daily'}), b'')
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            assert self.process.poll() is None, 'Arc exited during compaction'
            stats = self.request('/api/v1/compaction/stats')
            if stats['last_cycle']['cycle_id'] > before and not stats['cycle_running']:
                break
            time.sleep(.02)
        else:
            raise AssertionError('cycle did not terminate')
        assert stats['last_cycle']['status'] == 'completed', stats['last_cycle']
        jobs = self.request('/api/v1/compaction/history?limit=50')['recent_jobs']
        job = [j for j in jobs if j.get('partition_path', '').endswith(f'{measurement}/{day}')]
        assert job and job[-1]['success'] and job[-1]['files_compacted'] == 12, jobs
        assert self.candidates(database) == [], 'the compacted day is still a candidate'
        return {'candidate': candidate, 'job': job[-1], **stats['last_cycle']}


def binder_error(message, field):
    assert 'Binder Error' in message and field in message, message


def late_field_in_one_day(arc, report):
    # Twelve files on 2026-01-01: the first, in hour 00, carries only `stable`;
    # the other eleven, in hour 01, add `late_only`.
    db, meas = 'range_schema_repro', 'range_schema'
    early, late = EARLY_HOUR, LATE_HOUR
    arc.write(db, meas, [f'{meas},source=control stable=1i {JAN}'])
    for i in range(1, 12):
        arc.write(db, meas, [f'{meas},source=control stable={i + 1}i,late_only=42i {JAN + HOUR + i * SECOND}'])
    assert len(arc.files(db, meas)) == 12
    assert len([p for p in arc.files(db, meas) if p.parent.name == '00']) == 1

    def observe():
        star = arc.query(f'SELECT * FROM {db}.{meas} WHERE {early} LIMIT 1')
        assert 'late_only' in star['columns'], star
        assert star['data'][0][star['columns'].index('late_only')] is None, star
        assert arc.query(f'SELECT late_only, typeof(late_only) FROM {db}.{meas} WHERE {early} LIMIT 1')['data'] == [[None, 'BIGINT']]
        assert arc.query(f'SELECT late_only FROM {db}.{meas} WHERE {late} LIMIT 1')['data'] == [[42]]
        # An empty range falls back to the whole measurement unless the
        # experimental query.empty_range_anchor_scan is on, so this held before
        # 26.09.2 too; it is here because the report's dashboards issue it.
        empty = arc.query(f'SELECT * FROM {db}.{meas} WHERE time < \'2025-12-31T00:00:00Z\'')
        assert empty['data'] == [] and empty['columns'] == star['columns'], empty
        binder_error(arc.query_error(f'SELECT never_written FROM {db}.{meas} WHERE {early} LIMIT 1'), 'never_written')
        return {'early_star': star, 'rows': arc.query(f'SELECT count(*), count(late_only) FROM {db}.{meas}')['data']}

    before = observe()
    assert before['rows'] == [[12, 11]], before
    fields = {f['name']: f['type'] for f in arc.schema(db, meas)['fields']}
    assert fields.get('late_only') == 'BIGINT', fields
    # The field report enabled compaction by restarting over the same data.
    arc.restart(compaction=True)
    cycle = arc.compact_day(db, meas, '2026/01/01')
    assert len(arc.files(db, meas)) == 1
    after = observe()
    assert after == before, 'daily compaction changed what the early range binds'
    report['late_field_in_one_day'] = {'before_compaction': before, 'compaction': cycle, 'after_compaction': after}


def field_added_59_days_later(arc, report):
    # January is compacted before `weeks_later` exists anywhere; March, 59 days
    # later, introduces it and is compacted separately.
    db, meas = 'range_schema_multiday', 'multiday_schema'
    arc.write_day(db, meas, 'early', '', JAN)
    january = arc.compact_day(db, meas, '2026/01/01')
    january_file = arc.files(db, meas)
    assert len(january_file) == 1
    january_size = january_file[0].stat().st_size
    binder_error(arc.query_error(f'SELECT weeks_later FROM {db}.{meas} WHERE {JAN_ONLY} LIMIT 1'), 'weeks_later')

    arc.write_day(db, meas, 'late', ',weeks_later=99i', MAR)
    march = arc.compact_day(db, meas, '2026/03/01')
    assert len(arc.files(db, meas)) == 2
    assert january_file[0].exists() and january_file[0].stat().st_size == january_size, \
        'March compaction rewrote the January output'

    star = arc.query(f'SELECT * FROM {db}.{meas} WHERE {JAN_ONLY} LIMIT 1')
    assert 'weeks_later' in star['columns'], star
    assert arc.query(f'SELECT weeks_later, typeof(weeks_later) FROM {db}.{meas} WHERE {JAN_ONLY} ORDER BY time LIMIT 1')['data'] == [[None, 'BIGINT']]
    assert arc.query(f'SELECT count(*), count(weeks_later) FROM {db}.{meas} WHERE {JAN_ONLY}')['data'] == [[12, 0]]
    assert arc.query(f'SELECT weeks_later FROM {db}.{meas} WHERE {MAR_ONLY} ORDER BY time LIMIT 1')['data'] == [[99]]
    grouped = f'FROM {db}.{meas} WHERE {SPAN} GROUP BY source ORDER BY source'
    assert arc.query(f'SELECT source, count(*), count(weeks_later), min(weeks_later) {grouped}')['data'] == \
        [['early', 12, 0, None], ['late', 12, 12, 99]]
    assert arc.query(f'SELECT source, min(COALESCE(weeks_later, 0)) {grouped}')['data'] == [['early', 0], ['late', 99]]
    assert arc.query(f'SELECT source, count(try_cast(weeks_later AS BIGINT)) {grouped}')['data'] == [['early', 0], ['late', 12]]
    span_star = arc.query(f'SELECT * FROM {db}.{meas} WHERE {SPAN} ORDER BY time LIMIT 1')
    assert span_star['columns'] == star['columns'], (span_star['columns'], star['columns'])
    binder_error(arc.query_error(f'SELECT never_written FROM {db}.{meas} WHERE {SPAN} LIMIT 1'), 'never_written')
    report['field_added_59_days_later'] = {'january': january, 'march': march, 'january_only_star': star}


def field_removed_59_days_later(arc, report):
    # The mirror image: `retired_field` is written in January and never again.
    db, meas = 'field_removal_schema', 'field_removed'
    arc.write_day(db, meas, 'early', ',retired_field=77i', JAN)
    january = arc.compact_day(db, meas, '2026/01/01')
    arc.write_day(db, meas, 'late', '', MAR)
    march = arc.compact_day(db, meas, '2026/03/01')
    assert arc.query(f'SELECT count(*), count(retired_field), min(retired_field) FROM {db}.{meas} WHERE {JAN_ONLY}')['data'] == [[12, 12, 77]]
    assert arc.query(f'SELECT retired_field, typeof(retired_field) FROM {db}.{meas} WHERE {MAR_ONLY} ORDER BY time LIMIT 1')['data'] == [[None, 'BIGINT']]
    assert arc.query(f'SELECT count(*), count(retired_field) FROM {db}.{meas} WHERE {MAR_ONLY}')['data'] == [[12, 0]]
    assert arc.query(f'SELECT source, count(*), count(retired_field), min(retired_field) FROM {db}.{meas} WHERE {SPAN} GROUP BY source ORDER BY source')['data'] == \
        [['early', 12, 12, 77], ['late', 12, 0, None]]
    report['field_removed_59_days_later'] = {'january': january, 'march': march}


def restarts(arc, report):
    # Stored anchors survive a restart, and disabling the feature brings the
    # 26.09.1 Binder Errors back: the assertions above observe the feature.
    narrow = {'added': f'SELECT weeks_later FROM range_schema_multiday.multiday_schema WHERE {JAN_ONLY} LIMIT 1',
              'removed': f'SELECT retired_field FROM field_removal_schema.field_removed WHERE {MAR_ONLY} LIMIT 1',
              'same_day': f'SELECT late_only FROM range_schema_repro.range_schema WHERE {EARLY_HOUR} LIMIT 1'}
    star = f'SELECT * FROM range_schema_multiday.multiday_schema WHERE {JAN_ONLY} LIMIT 1'
    arc.restart(compaction=False, stable_schema=False)
    disabled = {name: arc.query_error(sql) for name, sql in narrow.items() if name != 'same_day'}
    for name, message in disabled.items():
        binder_error(message, narrow[name].split()[1])
    disabled['star'] = arc.query(star)['columns']
    assert 'weeks_later' not in disabled['star'], disabled
    # Daily compaction merged that day into one file whose union schema carries
    # `late_only`, so 26.09.1 answers this one as well (step 05 of the report).
    disabled['same_day'] = arc.query(narrow['same_day'])['data']
    assert disabled['same_day'] == [[None]], disabled
    arc.restart(compaction=False, stable_schema=True)
    enabled = {name: arc.query(sql)['data'] for name, sql in narrow.items()}
    assert all(data == [[None]] for data in enabled.values()), enabled
    enabled['star'] = arc.query(star)['columns']
    assert 'weeks_later' in enabled['star'], enabled
    report['restarts'] = {'stable_schema_disabled': disabled, 'stable_schema_enabled': enabled}


def run(binary, root):
    root.mkdir(parents=True, exist_ok=False)
    arc = Arc(binary, root)
    report = {'started_utc': utc(), 'binary_sha256': hashlib.sha256(binary.read_bytes()).hexdigest(),
              'environment': 'native local process; daily tier triggered manually; hourly tier disabled'}
    try:
        arc.start(compaction=False)
        late_field_in_one_day(arc, report)
        field_added_59_days_later(arc, report)
        field_removed_59_days_later(arc, report)
        restarts(arc, report)
        assert arc.process.poll() is None
        report['planned_process_starts'] = arc.start_count
        report['unexpected_process_exits'] = 0
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
            (root / 'results.json').write_text(json.dumps(report, indent=2))
            print(json.dumps(report, indent=2))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--arc', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    run(args.arc.resolve(), args.output.resolve())

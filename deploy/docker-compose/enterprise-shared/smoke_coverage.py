#!/usr/bin/env python3
"""Check acknowledged smoke hosts without relying on global row cardinality."""
import json
import os
import sys
import urllib.error
import urllib.request


def count(response, name):
    rows = response.get("data")
    if not isinstance(rows, list) or len(rows) != 1:
        raise ValueError("expected exactly one aggregate row")
    row = rows[0]
    value = row[name] if isinstance(row, dict) else row[response["columns"].index(name)]
    if type(value) is not int or value < 0:
        raise ValueError(f"invalid {name} count")
    return value


def check(query, measurement, ranges, expected, scenario):
    totals = query(f"SELECT COUNT(*) AS n, COUNT(DISTINCT host) AS unique_hosts FROM {measurement}")
    actual, distinct = count(totals, "n"), count(totals, "unique_hosts")
    if scenario == "base":
        if actual != expected:
            raise ValueError(f"base record count mismatch: {actual} != {expected}")
    else:
        acknowledged = [f"server{i}" for start, size in ranges for i in range(start, start + size)]
        if not acknowledged:
            raise ValueError("no acknowledged records; crash recovery was not exercised")
        # Batches of 250 fit Arc's 10,000-character SQL limit, including
        # host IDs with 64-bit integer suffixes. Aggregates avoid row limits.
        # Only IDs from successful requests can contribute to these counts.
        for offset in range(0, len(acknowledged), 250):
            hosts = acknowledged[offset:offset + 250]
            values = ",".join(f"'{host}'" for host in hosts)
            result = query(f"SELECT COUNT(DISTINCT host) AS covered FROM {measurement} WHERE host IN ({values})")
            covered = count(result, "covered")
            if covered != len(hosts):
                raise ValueError(f"acknowledged records missing: {len(hosts) - covered} in coverage chunk {offset // 250}; durability violation")
    return actual, distinct


def main():
    url, database, measurement, scenario, expected, ranges_path = sys.argv[1:]
    def query(sql):
        request = urllib.request.Request(
            url + "/api/v1/query", data=json.dumps({"sql": sql}).encode(),
            headers={"Authorization": "Bearer " + os.environ["ADMIN_TOKEN"],
                     "x-arc-database": database, "Content-Type": "application/json"})
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                return json.load(response)
        except urllib.error.HTTPError as error:
            detail = error.read(1000).decode(errors="replace")
            raise ValueError(f"query HTTP {error.code}: {detail}") from None
    with open(ranges_path) as source:
        ranges = [tuple(map(int, line.split())) for line in source]
    print(*check(query, measurement, ranges, int(expected), scenario), sep="\t")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        sys.exit(f"coverage check failed: {error}")

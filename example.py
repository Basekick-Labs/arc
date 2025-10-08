"""
Complete Arc example: Write data, then query it
"""
import msgpack
import requests
from datetime import datetime
import os

# Get or create API token
token = os.getenv("ARC_TOKEN")
if not token:
    from api.auth import AuthManager
    auth = AuthManager(db_path='./data/historian.db')
    token = auth.create_token(name='example', description='Complete example')
    print(f"✓ Token created: {token}")
    print(f"  Save it: export ARC_TOKEN='{token}'")
    print()

print("=" * 60)
print("STEP 1: Writing data...")
print("=" * 60)

# Prepare data in MessagePack binary format
data = {
    "batch": [
        {
            "m": "cpu",
            "t": int(datetime.now().timestamp() * 1000),
            "h": "server01",
            "tags": {"region": "us-east", "dc": "aws"},
            "fields": {"usage_idle": 95.0, "usage_user": 3.2, "usage_system": 1.8}
        },
        {
            "m": "cpu",
            "t": int(datetime.now().timestamp() * 1000),
            "h": "server02",
            "tags": {"region": "us-west", "dc": "gcp"},
            "fields": {"usage_idle": 85.0, "usage_user": 10.5, "usage_system": 4.5}
        },
        {
            "m": "mem",
            "t": int(datetime.now().timestamp() * 1000),
            "h": "server01",
            "fields": {"used_percent": 45.2, "available": 8192}
        }
    ]
}

# Write via MessagePack
response = requests.post(
    "http://localhost:8000/write/v2/msgpack",
    headers={
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/msgpack"
    },
    data=msgpack.packb(data)
)

if response.status_code == 204:
    print(f"✓ Successfully wrote {len(data['batch'])} measurements!")
else:
    print(f"✗ Write failed: {response.status_code} - {response.text}")
    exit(1)

print()
print("=" * 60)
print("STEP 2: Querying data...")
print("=" * 60)

# Query 1: Simple SELECT
print("\n1. Simple SELECT from cpu table:")
response = requests.post(
    "http://localhost:8000/query",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "sql": "SELECT * FROM cpu ORDER BY timestamp DESC LIMIT 10",
        "format": "json"
    }
)

data = response.json()
if data.get('success'):
    print(f"   ✓ Found {data['row_count']} rows")
    print(f"   Columns: {data['columns']}")
    for row in data['data']:
        print(f"   {row}")
else:
    print(f"   ✗ Query failed: {data.get('error')}")

# Query 2: Filter by host
print("\n2. Filter by host:")
response = requests.post(
    "http://localhost:8000/query",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "sql": "SELECT * FROM cpu WHERE host = 'server01'",
        "format": "json"
    }
)

data = response.json()
if data.get('success'):
    print(f"   ✓ Found {data['row_count']} rows for server01")
    for row in data['data']:
        print(f"   {row}")

# Query 3: Aggregation
print("\n3. Aggregation by host:")
response = requests.post(
    "http://localhost:8000/query",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "sql": "SELECT host, AVG(usage_idle) as avg_idle FROM cpu GROUP BY host",
        "format": "json"
    }
)

data = response.json()
if data.get('success'):
    print(f"   ✓ Aggregated {data['row_count']} hosts")
    for row in data['data']:
        print(f"   {row}")

# Query 4: List all measurements
print("\n4. Show all measurements (tables):")
response = requests.post(
    "http://localhost:8000/query",
    headers={"Authorization": f"Bearer {token}"},
    json={
        "sql": "SHOW TABLES",
        "format": "json"
    }
)

data = response.json()
if data.get('success'):
    print(f"   ✓ Found {data['row_count']} tables")
    for row in data['data']:
        print(f"   - {row[0]}")

print()
print("=" * 60)
print("✓ Example complete!")
print("=" * 60)

#!/usr/bin/env python3
"""
Copy Parquet file directly to MinIO for Arc to read.
This is much faster than ingesting through the API.
"""

import os
import boto3
from botocore.client import Config

# MinIO connection
s3_client = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

# Source Parquet file
parquet_file = os.getenv("PARQUET_FILE",
    "/Users/nacho/dev/exydata.ventures/historian_product/benchmarks/clickbench/data/hits.parquet")

# Arc storage path: s3://arc/{database}/{measurement}/{partition}/data.parquet
# Arc uses "default" as the database name by default
database = "default"
measurement = "hits"
partition = "2024-01-01"  # Use a single partition since we don't have timestamps

s3_key = f"{database}/{measurement}/{partition}/hits.parquet"

print(f"Copying {parquet_file} to s3://arc/{s3_key}")
print(f"File size: {os.path.getsize(parquet_file) / 1024 / 1024 / 1024:.2f} GB")

# Upload the file
s3_client.upload_file(
    parquet_file,
    'arc',
    s3_key,
    Callback=lambda bytes_transferred: print(f"Uploaded {bytes_transferred / 1024 / 1024:.2f} MB", end='\r')
)

print(f"\n✓ File uploaded to s3://arc/{s3_key}")
print(f"\nYou can now query with: SELECT COUNT(*) FROM hits")

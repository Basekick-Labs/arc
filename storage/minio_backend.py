import asyncio
import aiofiles
from pathlib import Path
from typing import List, Optional
import boto3
from botocore.client import Config
import logging
import duckdb

logger = logging.getLogger(__name__)

class MinIOBackend:
    def __init__(self, endpoint_url: str, access_key: str, secret_key: str, bucket: str, prefix: str = ""):
        self.bucket = bucket
        self.prefix = prefix.rstrip('/') + '/' if prefix else ''
        
        # Configure S3 client for MinIO
        self.s3_client = boto3.client(
            's3',
            endpoint_url=endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(signature_version='s3v4'),
            region_name='us-east-1'  # MinIO doesn't care about region
        )
        
        # Create bucket if it doesn't exist
        try:
            self.s3_client.head_bucket(Bucket=bucket)
        except self.s3_client.exceptions.NoSuchBucket:
            try:
                self.s3_client.create_bucket(Bucket=bucket)
                logger.info(f"Created MinIO bucket: {bucket}")
            except Exception as e:
                logger.warning(f"Bucket creation warning: {e}")
        except Exception as e:
            logger.warning(f"Error checking bucket existence: {e}")
    
    def _set_duckdb_credentials_secure(self, duckdb_conn: duckdb.DuckDBPyConnection, access_key: str, secret_key: str) -> None:
        """Securely set DuckDB S3 credentials for MinIO"""
        try:
            # Use DuckDB's secure parameter setting instead of SQL string interpolation
            duckdb_conn.execute("SET s3_access_key_id = ?", [access_key])
            duckdb_conn.execute("SET s3_secret_access_key = ?", [secret_key])
            logger.info("MinIO S3 credentials configured securely for DuckDB")
        except Exception as e:
            # Fallback to the old method if parameterized queries don't work
            logger.warning("Parameterized credential setting failed, using fallback method")
            access_key_masked = f"{access_key[:4]}{'*' * (len(access_key) - 8)}{access_key[-4:]}" if len(access_key) > 8 else "****"
            try:
                duckdb_conn.execute(f"SET s3_access_key_id='{access_key}'")
                duckdb_conn.execute(f"SET s3_secret_access_key='{secret_key}'")
                logger.info(f"MinIO S3 credentials configured for DuckDB (access_key: {access_key_masked})")
            except Exception as fallback_error:
                logger.error(f"Failed to set MinIO S3 credentials: {fallback_error}")
                raise
    
    async def upload_file(self, local_path: Path, s3_key: str) -> bool:
        """Upload single file to MinIO asynchronously"""
        try:
            full_key = f"{self.prefix}{s3_key}"
            logger.info(f"Upload key: {s3_key} -> full key: {full_key}")
            
            # Read file asynchronously
            async with aiofiles.open(local_path, 'rb') as f:
                file_data = await f.read()
            
            # Upload to MinIO in executor
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None,
                lambda: self.s3_client.put_object(
                    Bucket=self.bucket,
                    Key=full_key,
                    Body=file_data,
                    ContentType='application/octet-stream'
                )
            )
            
            logger.info(f"Uploaded {local_path} to minio://{self.bucket}/{full_key}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            return False
    
    async def upload_parquet_files(self, local_dir: Path, measurement: str) -> int:
        """Upload all parquet files for a measurement with partitioned structure"""
        # Look for partitioned files in measurement subdirectories
        parquet_files = list(local_dir.glob(f"{measurement}/**/*.parquet"))
        
        # Also look for files with measurement prefix in root directory (legacy format)
        root_files = list(local_dir.glob(f"{measurement}_*.parquet"))
        parquet_files.extend(root_files)
        
        # Debug: show all files in directory
        all_files = list(local_dir.rglob("*.parquet"))
        logger.info(f"All parquet files in {local_dir}: {[str(f.relative_to(local_dir)) for f in all_files]}")
        logger.info(f"Looking for measurement: '{measurement}'")
        logger.info(f"Pattern 1: {measurement}/**/*.parquet")
        logger.info(f"Pattern 2: {measurement}_*.parquet") 
        logger.info(f"Found matching files: {[str(f.relative_to(local_dir)) for f in parquet_files]}")
        
        # Additional debug: check if directories exist
        measurement_dir = local_dir / measurement
        logger.info(f"Measurement directory exists: {measurement_dir.exists()}")
        if measurement_dir.exists():
            logger.info(f"Contents of {measurement}: {list(measurement_dir.rglob('*'))}")
        
        if not parquet_files:
            logger.warning(f"No parquet files found for {measurement}")
            return 0
        
        logger.info(f"Uploading {len(parquet_files)} files for {measurement}")
        
        # Upload files with high concurrency (20 concurrent uploads)
        semaphore = asyncio.Semaphore(20)
        
        async def upload_with_semaphore(file_path):
            async with semaphore:
                # Preserve the relative path structure from local_dir
                s3_key = str(file_path.relative_to(local_dir))
                logger.info(f"Uploading {file_path} as {s3_key}")
                return await self.upload_file(file_path, s3_key)
        
        # Execute uploads concurrently
        tasks = [upload_with_semaphore(file_path) for file_path in parquet_files]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        success_count = sum(1 for r in results if r is True)
        logger.info(f"Uploaded {success_count}/{len(parquet_files)} files to MinIO")
        
        return success_count
    
    def get_s3_path(self, measurement: str, year: int, month: int, day: int, hour: int = None) -> str:
        """Generate S3 path for flattened structure"""
        if hour is not None:
            return f"s3://{self.bucket}/{measurement}_{year}_{month:02d}_{day:02d}_{hour:02d}.parquet"
        else:
            return f"s3://{self.bucket}/{measurement}_{year}_{month:02d}_*.parquet"
    
    def list_objects(self) -> List[str]:
        """List all objects in the bucket to discover structure"""
        try:
            objects = []
            paginator = self.s3_client.get_paginator('list_objects_v2')
            
            # List all objects in bucket
            for page in paginator.paginate(Bucket=self.bucket):
                if 'Contents' in page:
                    for obj in page['Contents']:
                        key = obj['Key']
                        objects.append(key)
            
            logger.info(f"Found {len(objects)} objects in bucket")
            logger.info(f"Sample objects: {objects[:5]}")
            return objects
            
        except Exception as e:
            logger.error(f"Failed to list objects: {e}")
            return []
    
    async def configure_duckdb_s3(self, duckdb_conn):
        """Configure DuckDB for MinIO S3 access"""
        try:
            # Configure S3 settings for MinIO
            endpoint_host = self.s3_client._endpoint.host.replace('http://', '').replace('https://', '')
            duckdb_conn.execute(f"SET s3_endpoint='{endpoint_host}'")
            
            # Use secure credential setting
            access_key = self.s3_client._request_signer._credentials.access_key
            secret_key = self.s3_client._request_signer._credentials.secret_key
            self._set_duckdb_credentials_secure(duckdb_conn, access_key, secret_key)
            
            duckdb_conn.execute("SET s3_use_ssl=false")  # Set to true if using HTTPS
            duckdb_conn.execute("SET s3_url_style='path'")  # MinIO uses path-style URLs
            logger.info("MinIO S3 configuration applied to DuckDB")
        except Exception as e:
            logger.error(f"Failed to configure DuckDB for MinIO: {e}")
import asyncio
import aiofiles
from pathlib import Path
from typing import List, Optional
import boto3
from botocore.exceptions import ClientError
import logging
import os
import duckdb

logger = logging.getLogger(__name__)

class S3Backend:
    def __init__(self, bucket: str, region: str = "us-east-1", database: str = "default",
                 access_key: str = None, secret_key: str = None, use_directory_bucket: bool = False,
                 availability_zone: str = None):
        self.bucket = bucket
        self.region = region
        self.database = database
        self.use_directory_bucket = use_directory_bucket
        self.availability_zone = availability_zone or f"{region}a"
        
        # Use the actual bucket name for both standard and S3 Express
        self.s3_path_bucket = bucket
        
        # Store credentials for DuckDB configuration
        self._access_key = access_key
        self._secret_key = secret_key
        
        # Configure S3 client with optional credentials
        session_kwargs = {'region_name': region}
        if access_key and secret_key:
            session_kwargs.update({
                'aws_access_key_id': access_key,
                'aws_secret_access_key': secret_key
            })
        
        # Use directory bucket configuration if enabled
        if use_directory_bucket:
            # For S3 Express, create a session with explicit configuration
            import boto3
            from botocore.config import Config
            
            session = boto3.Session(
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name=region
            )
            
            config = Config(
                region_name=region,
                s3={
                    'addressing_style': 'path',
                    'use_virtual_host_bucket': False,
                    'use_accelerate_endpoint': False,
                    'use_dualstack_endpoint': False
                }
            )
            
            self.s3_client = session.client('s3', config=config)
            logger.info(f"Using S3 Express Directory Bucket configuration for AZ: {self.availability_zone}")
        else:
            # Standard S3 configuration
            from botocore.config import Config
            session_kwargs['config'] = Config(s3={'addressing_style': 'virtual'})
            self.s3_client = boto3.client('s3', **session_kwargs)
        
        # S3 client is created above based on bucket type
        bucket_type = "Directory Bucket" if use_directory_bucket else "Standard Bucket"
        logger.info(f"S3Backend initialized for {bucket_type}: {bucket}, region: {region}")
        logger.info(f"Credentials available: {bool(access_key and secret_key)}")
    
    def _set_duckdb_credentials_secure(self, duckdb_conn: duckdb.DuckDBPyConnection, access_key: str, secret_key: str) -> None:
        """Securely set DuckDB S3 credentials using direct API calls instead of SQL strings"""
        try:
            # Use DuckDB's secure parameter setting instead of SQL string interpolation
            # This prevents credentials from appearing in query logs
            duckdb_conn.execute("SET s3_access_key_id = ?", [access_key])
            duckdb_conn.execute("SET s3_secret_access_key = ?", [secret_key])
            logger.info("AWS S3 credentials configured securely for DuckDB")
        except Exception as e:
            # Fallback to the old method if parameterized queries don't work
            logger.warning("Parameterized credential setting failed, using fallback method")
            # Redact credentials in logs
            access_key_masked = f"{access_key[:4]}{'*' * (len(access_key) - 8)}{access_key[-4:]}" if len(access_key) > 8 else "****"
            try:
                duckdb_conn.execute(f"SET s3_access_key_id='{access_key}'")
                duckdb_conn.execute(f"SET s3_secret_access_key='{secret_key}'")
                logger.info(f"AWS S3 credentials configured for DuckDB (access_key: {access_key_masked})")
            except Exception as fallback_error:
                logger.error(f"Failed to set S3 credentials: {fallback_error}")
                raise
        
    async def upload_file(self, local_path: Path, s3_key: str, timeout: int = 300) -> bool:
        """
        Upload single file to S3 asynchronously with timeout

        Args:
            local_path: Local file path to upload
            s3_key: S3 object key
            timeout: Upload timeout in seconds (default: 300s / 5 minutes)

        Returns:
            True if upload succeeded, False otherwise
        """
        try:
            full_key = f"{self.database}/{s3_key}"

            # Read file asynchronously
            async with aiofiles.open(local_path, 'rb') as f:
                file_data = await f.read()

            # Upload to S3 in executor with timeout
            loop = asyncio.get_event_loop()
            await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    lambda: self.s3_client.put_object(
                        Bucket=self.bucket,
                        Key=full_key,
                        Body=file_data,
                        ContentType='application/octet-stream'
                    )
                ),
                timeout=timeout
            )

            logger.info(f"Uploaded {local_path} to s3://{self.bucket}/{full_key}")
            return True

        except asyncio.TimeoutError:
            logger.error(f"Upload timeout after {timeout}s: {local_path}")
            return False
        except Exception as e:
            logger.error(f"Failed to upload {local_path}: {e}")
            return False
    
    async def upload_parquet_files(self, local_dir: Path, measurement: str) -> int:
        """Upload all parquet files for a measurement (matches MinIO backend interface)"""
        logger.info(f"Starting S3 upload for measurement: {measurement}")
        logger.info(f"All parquet files in {local_dir}: {list(local_dir.rglob('*.parquet'))}")
        
        # Find all parquet files in the measurement directory
        parquet_files = list(local_dir.rglob("*.parquet"))
        measurement_files = [f for f in parquet_files if measurement in str(f)]
        
        logger.info(f"Looking for pattern: {measurement}_*.parquet")
        logger.info(f"Found matching files: {[f.name for f in measurement_files]}")
        
        if not measurement_files:
            logger.warning(f"No parquet files found for {measurement}")
            return 0
        
        logger.info(f"Uploading {len(measurement_files)} files for {measurement}")
        
        # Upload files with proper S3 keys
        tasks = []
        for file_path in measurement_files:
            # Create S3 key preserving the partitioned structure
            relative_path = file_path.relative_to(local_dir)
            s3_key = str(relative_path)
            
            logger.info(f"Uploading {file_path} as {s3_key}")
            logger.info(f"Upload key: {s3_key} -> full key: {self.database}/{s3_key}")
            
            task = self.upload_file(file_path, s3_key)
            tasks.append(task)
        
        # Execute uploads concurrently (limit to 10 concurrent uploads)
        semaphore = asyncio.Semaphore(10)
        
        async def upload_with_semaphore(task):
            async with semaphore:
                return await task
        
        results = await asyncio.gather(*[upload_with_semaphore(task) for task in tasks], return_exceptions=True)
        
        success_count = sum(1 for r in results if r is True)
        logger.info(f"Uploaded {success_count}/{len(measurement_files)} files to S3")
        
        return success_count
    
    async def list_files(self, measurement: str) -> List[str]:
        """List files in S3 for a measurement"""
        try:
            prefix = f"{self.database}/{measurement}/"
            
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None,
                lambda: self.s3_client.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix=prefix
                )
            )
            
            files = []
            if 'Contents' in response:
                files = [obj['Key'] for obj in response['Contents']]
            
            return files
            
        except Exception as e:
            logger.error(f"Failed to list files: {e}")
            return []
    
    async def configure_duckdb_s3(self, duckdb_conn):
        """Configure DuckDB for S3 access (matches MinIO backend interface)"""
        try:
            # Configure S3 settings for DuckDB
            duckdb_conn.execute(f"SET s3_region='{self.region}'")
            duckdb_conn.execute("SET s3_url_style='path'")
            
            # Configure directory bucket settings if enabled
            if self.use_directory_bucket:
                # For S3 Express with DuckDB 1.3.2+, let DuckDB auto-detect
                try:
                    duckdb_conn.execute("RESET s3_endpoint")
                except:
                    pass
                duckdb_conn.execute(f"SET s3_region='{self.region}'")
                logger.info(f"DuckDB 1.3.2+ configured for S3 Directory Bucket (auto-detection enabled)")
            
            # Use credentials from the backend initialization
            if hasattr(self, '_access_key') and hasattr(self, '_secret_key') and self._access_key and self._secret_key:
                self._set_duckdb_credentials_secure(duckdb_conn, self._access_key, self._secret_key)
                logger.info("AWS S3 credentials configured for DuckDB from storage connection")
            else:
                # Fallback to environment variables
                access_key = os.getenv('AWS_ACCESS_KEY_ID')
                secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
                
                if access_key and secret_key:
                    self._set_duckdb_credentials_secure(duckdb_conn, access_key, secret_key)
                    logger.info("AWS S3 credentials configured for DuckDB from environment")
                else:
                    logger.warning("No AWS credentials found for DuckDB S3 access")
            
            bucket_type = "Directory Bucket" if self.use_directory_bucket else "Standard Bucket"
            logger.info(f"AWS S3 {bucket_type} configuration applied to DuckDB (region: {self.region})")
            
        except Exception as e:
            logger.error(f"Failed to configure DuckDB for S3: {e}")
            raise
    
    def list_objects(self, prefix: str = "") -> List[str]:
        """List objects in S3 bucket with database scope"""
        try:
            full_prefix = f"{self.database}/{prefix}" if prefix else self.database
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket,
                Prefix=full_prefix
            )
            
            objects = []
            if 'Contents' in response:
                objects = [obj['Key'] for obj in response['Contents']]
            
            return objects
            
        except Exception as e:
            logger.error(f"Failed to list S3 objects: {e}")
            return []
    
    def get_s3_path(self, measurement: str, year: int, month: int, day: int, hour: int = None) -> str:
        """Generate S3 path for querying with database scope

        Path structure: {bucket}/{database}/{measurement}/{year}/{month}/{day}/{hour}/file.parquet
        """
        if hour is not None:
            return f"s3://{self.s3_path_bucket}/{self.database}/{measurement}/{year}/{month:02d}/{day:02d}_{hour:02d}.parquet"
        else:
            return f"s3://{self.s3_path_bucket}/{self.database}/{measurement}/{year}/{month:02d}/*.parquet"
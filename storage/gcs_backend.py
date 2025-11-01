import asyncio
import aiofiles
import json
import os
import tempfile
from pathlib import Path
from typing import List, Optional, Dict, Any
from google.cloud import storage
from google.oauth2 import service_account
from google.auth.exceptions import DefaultCredentialsError
import logging

logger = logging.getLogger(__name__)

class GCSBackend:
    def __init__(self, bucket: str, database: str = "default",
                 project_id: str = None, credentials_json: str = None,
                 credentials_file: str = None, hmac_key_id: str = None,
                 hmac_secret: str = None):
        self.bucket_name = bucket
        self.database = database
        self.project_id = project_id
        
        # Store credentials for DuckDB configuration
        self._credentials_json = credentials_json
        self._credentials_file = credentials_file
        self._hmac_key_id = hmac_key_id
        self._hmac_secret = hmac_secret
        
        # Initialize GCS client
        self.client = self._initialize_client()
        self.bucket = self.client.bucket(self.bucket_name)

        logger.info(f"GCS Backend initialized: gs://{self.bucket_name}/{self.database}")
    
    def _initialize_client(self):
        """Initialize GCS client with proper authentication"""
        try:
            if self._credentials_json:
                # Use JSON credentials string
                credentials_info = json.loads(self._credentials_json)
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_info,
                    scopes=['https://www.googleapis.com/auth/cloud-platform']
                )
                return storage.Client(
                    project=self.project_id or credentials_info.get('project_id'),
                    credentials=credentials
                )
            elif self._credentials_file:
                # Use credentials file
                credentials = service_account.Credentials.from_service_account_file(
                    self._credentials_file,
                    scopes=['https://www.googleapis.com/auth/cloud-platform']
                )
                with open(self._credentials_file, 'r') as f:
                    cred_info = json.load(f)
                return storage.Client(
                    project=self.project_id or cred_info.get('project_id'),
                    credentials=credentials
                )
            else:
                # Use default credentials (ADC)
                return storage.Client(project=self.project_id)
                
        except (DefaultCredentialsError, Exception) as e:
            logger.error(f"Failed to initialize GCS client: {e}")
            raise
    
    def test_connection(self) -> Dict[str, Any]:
        """Test the GCS connection"""
        try:
            # Check bucket existence and access
            bucket_exists = self.bucket.exists()
            if not bucket_exists:
                return {
                    "success": False,
                    "error": f"Bucket '{self.bucket_name}' does not exist or is not accessible"
                }

            # Try to list objects (with limit to avoid large responses)
            blobs = list(self.client.list_blobs(self.bucket_name, prefix=f"{self.database}/", max_results=1))

            return {
                "success": True,
                "bucket": self.bucket_name,
                "database": self.database,
                "project": self.client.project,
                "message": f"Successfully connected to GCS bucket gs://{self.bucket_name}"
            }
            
        except Exception as e:
            logger.error(f"GCS connection test failed: {e}")
            return {
                "success": False,
                "error": f"Connection failed: {str(e)}"
            }
    
    async def upload_file(self, local_path: Path, remote_path: str, timeout: int = 300) -> bool:
        """Upload a file to GCS with timeout

        Args:
            local_path: Local file path to upload
            remote_path: Remote path within GCS bucket
            timeout: Upload timeout in seconds (default: 300s / 5 minutes)

        Returns:
            True if upload succeeded, False otherwise
        """
        try:
            blob_name = f"{self.database}/{remote_path}"
            blob = self.bucket.blob(blob_name)

            # Upload file in executor with timeout to prevent hanging
            loop = asyncio.get_event_loop()
            await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    blob.upload_from_filename,
                    str(local_path)
                ),
                timeout=timeout
            )

            logger.debug(f"Uploaded {local_path} to gs://{self.bucket_name}/{blob_name}")
            return True

        except asyncio.TimeoutError:
            logger.error(f"Upload timeout after {timeout}s: {local_path}")
            return False
        except Exception as e:
            logger.error(f"Failed to upload {local_path} to GCS: {e}")
            return False
    
    async def upload_files(self, files: List[tuple]) -> int:
        """Upload multiple files concurrently"""
        tasks = []
        for local_path, remote_path in files:
            tasks.append(self.upload_file(local_path, remote_path))
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        successful_uploads = sum(1 for result in results if result is True)
        
        logger.info(f"Uploaded {successful_uploads}/{len(files)} files to GCS")
        return successful_uploads
    
    async def upload_parquet_files(self, local_dir: Path, measurement: str) -> int:
        """Upload all parquet files for a measurement (matches S3/MinIO backend interface)"""
        logger.info(f"Starting GCS upload for measurement: {measurement}")
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
        
        # Upload files with proper GCS keys
        tasks = []
        for file_path in measurement_files:
            # Create GCS key preserving the partitioned structure
            relative_path = file_path.relative_to(local_dir)
            gcs_key = str(relative_path)
            
            logger.info(f"Uploading {file_path} as {gcs_key}")
            logger.info(f"Upload key: {gcs_key} -> full key: {self.database}/{gcs_key}")
            
            task = self.upload_file(file_path, gcs_key)
            tasks.append(task)
        
        # Execute uploads concurrently (limit to 10 concurrent uploads)
        semaphore = asyncio.Semaphore(10)
        
        async def upload_with_semaphore(task):
            async with semaphore:
                return await task
        
        results = await asyncio.gather(*[upload_with_semaphore(task) for task in tasks], return_exceptions=True)
        
        success_count = sum(1 for r in results if r is True)
        logger.info(f"Uploaded {success_count}/{len(measurement_files)} files to GCS")
        
        return success_count
    
    async def download_file(self, remote_path: str, local_path: Path, timeout: int = 300) -> bool:
        """Download a file from GCS with timeout

        Args:
            remote_path: Remote path within GCS bucket
            local_path: Local file path to download to
            timeout: Download timeout in seconds (default: 300s / 5 minutes)

        Returns:
            True if download succeeded, False otherwise
        """
        try:
            blob_name = f"{self.database}/{remote_path}"
            blob = self.bucket.blob(blob_name)

            if not blob.exists():
                logger.warning(f"File gs://{self.bucket_name}/{blob_name} does not exist")
                return False

            # Download file in executor with timeout to prevent hanging
            loop = asyncio.get_event_loop()
            await asyncio.wait_for(
                loop.run_in_executor(
                    None,
                    blob.download_to_filename,
                    str(local_path)
                ),
                timeout=timeout
            )

            logger.debug(f"Downloaded gs://{self.bucket_name}/{blob_name} to {local_path}")
            return True

        except asyncio.TimeoutError:
            logger.error(f"Download timeout after {timeout}s: {remote_path}")
            return False
        except Exception as e:
            logger.error(f"Failed to download {remote_path} from GCS: {e}")
            return False
    
    async def list_files(self, path: str = "", pattern: str = "*") -> List[str]:
        """List files in GCS with optional pattern matching"""
        try:
            prefix = f"{self.database}/{path}".rstrip('/')
            if prefix and not prefix.endswith('/'):
                prefix += '/'

            blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)

            files = []
            db_prefix = f"{self.database}/"
            for blob in blobs:
                # Remove the database prefix to get relative path
                relative_path = blob.name[len(db_prefix):] if blob.name.startswith(db_prefix) else blob.name

                # Simple pattern matching (basic wildcard support)
                if pattern == "*" or pattern in relative_path:
                    files.append(relative_path)

            return sorted(files)
            
        except Exception as e:
            logger.error(f"Failed to list files from GCS: {e}")
            return []
    
    async def delete_file(self, remote_path: str) -> bool:
        """Delete a file from GCS"""
        try:
            blob_name = f"{self.database}/{remote_path}"
            blob = self.bucket.blob(blob_name)

            if blob.exists():
                blob.delete()
                logger.debug(f"Deleted gs://{self.bucket_name}/{blob_name}")
                return True
            else:
                logger.warning(f"File gs://{self.bucket_name}/{blob_name} does not exist")
                return False
                
        except Exception as e:
            logger.error(f"Failed to delete {remote_path} from GCS: {e}")
            return False
    
    def get_s3_compatible_config(self) -> Dict[str, str]:
        """
        Get DuckDB configuration for GCS access.
        Note: DuckDB doesn't have native GCS support, but we can use HTTPFS
        with signed URLs or export to S3-compatible interface if available.
        """
        logger.warning("DuckDB doesn't have native GCS support. Consider using GCS's S3-compatible interface or signed URLs.")

        # For now, return empty config - this would need to be implemented
        # based on specific requirements (signed URLs, S3-compatible interface, etc.)
        return {
            "provider": "gcs",
            "bucket": self.bucket_name,
            "database": self.database,
            "project_id": self.project_id or self.client.project,
            "note": "GCS access via DuckDB requires additional configuration"
        }
    
    def generate_signed_url(self, blob_path: str, expiration_minutes: int = 60) -> str:
        """Generate a signed URL for accessing a GCS object via HTTPS"""
        try:
            from datetime import timedelta
            blob_name = f"{self.database}/{blob_path}"
            blob = self.bucket.blob(blob_name)

            # Generate signed URL valid for specified time
            signed_url = blob.generate_signed_url(
                expiration=timedelta(minutes=expiration_minutes),
                method='GET'
            )
            return signed_url
            
        except Exception as e:
            logger.error(f"Failed to generate signed URL for {blob_path}: {e}")
            return ""
    
    def get_signed_urls_for_pattern(self, pattern: str, expiration_minutes: int = 60) -> List[str]:
        """Get signed URLs for all files matching a pattern"""
        try:
            # List files matching pattern
            files = asyncio.run(self.list_files(pattern=pattern))
            
            signed_urls = []
            for file_path in files:
                signed_url = self.generate_signed_url(file_path, expiration_minutes)
                if signed_url:
                    signed_urls.append(signed_url)
            
            return signed_urls
            
        except Exception as e:
            logger.error(f"Failed to get signed URLs for pattern {pattern}: {e}")
            return []

    def configure_duckdb(self, conn) -> bool:
        """
        Configure DuckDB for native GCS access using HMAC keys or service account.
        """
        try:
            # Install httpfs extension for GCS support
            conn.execute("INSTALL httpfs")
            conn.execute("LOAD httpfs")
            
            # Configure GCS access
            if self._hmac_key_id and self._hmac_secret:
                # Use HMAC keys for native GCS access (preferred method)
                conn.execute(f"""
                    CREATE SECRET gcs_secret (
                        TYPE GCS,
                        KEY_ID '{self._hmac_key_id}',
                        SECRET '{self._hmac_secret}'
                    )
                """)
                logger.info("DuckDB configured with GCS HMAC keys for native gs:// access")
                
            elif self._credentials_json:
                # Try to extract HMAC-like credentials from service account
                import json
                try:
                    creds = json.loads(self._credentials_json)
                    # Service account JSON doesn't contain HMAC keys
                    # Log a warning and continue without SECRET
                    logger.warning("Service account JSON provided but DuckDB GCS requires HMAC keys. Consider creating HMAC keys in GCS settings.")
                    logger.info("DuckDB configured with HTTPFS for GCS access (no authentication)")
                except Exception as e:
                    logger.error(f"Failed to parse service account JSON: {e}")
                    
            else:
                # No authentication - will only work with public buckets
                logger.warning("No HMAC keys provided. GCS access will only work with public buckets.")
                logger.info("DuckDB configured with HTTPFS for GCS access (no authentication)")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to configure DuckDB for GCS: {e}")
            return False
    
    def get_duckdb_query_urls(self, path_pattern: str) -> str:
        """
        Get a DuckDB-compatible URL pattern for querying GCS data.
        Returns native gs:// URLs that work with DuckDB's GCS support.
        Path structure: gs://{bucket}/{database}/{measurement}/{year}/{month}/{day}/{hour}/file.parquet
        """
        # Use native gs:// URLs with DuckDB's GCS support
        gs_url = f"gs://{self.bucket_name}/{self.database}/{path_pattern}"
        return f"'{gs_url}'"
    
    def get_gs_url(self, path: str = "") -> str:
        """Get the GCS URL for a path within the database"""
        if path:
            return f"gs://{self.bucket_name}/{self.database}/{path}".rstrip('/')
        return f"gs://{self.bucket_name}/{self.database}".rstrip('/')

    def __str__(self):
        return f"GCSBackend(bucket=gs://{self.bucket_name}, database={self.database})"
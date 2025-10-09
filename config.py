from pydantic import BaseModel
from typing import Optional
from pathlib import Path

class InfluxDBConfig(BaseModel):
    # InfluxDB 1.x
    host: str = "localhost"
    port: int = 8086
    username: str = ""
    password: str = ""
    database: str = ""
    
    # InfluxDB 2.x
    url: str = "http://localhost:8086"
    token: str = ""
    org: str = ""
    bucket: str = ""

class StorageConfig(BaseModel):
    backend: str = "local"  # local, s3, minio
    s3_bucket: str = ""
    s3_region: str = "us-east-1"
    database: str = "default"
    # MinIO specific
    minio_endpoint: str = "http://localhost:9000"
    minio_access_key: str = ""
    minio_secret_key: str = ""
    minio_bucket: str = "arc"

class ExportConfig(BaseModel):
    output_path: Path = Path("./data")
    chunk_hours: int = 24
    batch_size: int = 100000
    retention_days: int = 30

    class Config:
        arbitrary_types_allowed = True

class WriteConfig(BaseModel):
    """Configuration for write ingestion (Telegraf, OpenTelemetry, etc.)"""
    buffer_size: int = 10000  # Max records per measurement before flush
    buffer_age_seconds: int = 60  # Max seconds before flush
    compression: str = "snappy"  # Parquet compression: snappy, gzip, zstd
    enable_write_api: bool = True  # Enable Line Protocol write endpoints

class ArcConfig(BaseModel):
    influxdb: InfluxDBConfig
    export: ExportConfig = ExportConfig()
    storage: StorageConfig = StorageConfig()
    write: WriteConfig = WriteConfig()

    @classmethod
    def from_env(cls):
        """Load config from environment variables"""
        import os
        return cls(
            influxdb=InfluxDBConfig(
                # InfluxDB 1.x
                host=os.getenv("INFLUX_HOST", "localhost"),
                port=int(os.getenv("INFLUX_PORT", "8086")),
                username=os.getenv("INFLUX_USER", ""),
                password=os.getenv("INFLUX_PASSWORD", ""),
                database=os.getenv("INFLUX_DATABASE", ""),
                # InfluxDB 2.x
                url=os.getenv("INFLUX_URL", "http://localhost:8086"),
                token=os.getenv("INFLUX_TOKEN", ""),
                org=os.getenv("INFLUX_ORG", ""),
                bucket=os.getenv("INFLUX_BUCKET", "")
            ),
            export=ExportConfig(
                output_path=Path(os.getenv("EXPORT_OUTPUT_PATH", "./data"))
            ),
            write=WriteConfig(
                buffer_size=int(os.getenv("WRITE_BUFFER_SIZE", "10000")),
                buffer_age_seconds=int(os.getenv("WRITE_BUFFER_AGE", "60")),
                compression=os.getenv("WRITE_COMPRESSION", "snappy"),
                enable_write_api=os.getenv("ENABLE_WRITE_API", "true").lower() == "true"
            ),
            storage=StorageConfig(
                backend=os.getenv("STORAGE_BACKEND", "local"),
                s3_bucket=os.getenv("S3_BUCKET", ""),
                s3_region=os.getenv("S3_REGION", "us-east-1"),
                database=os.getenv("STORAGE_DATABASE", "default"),
                minio_endpoint=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
                minio_access_key=os.getenv("MINIO_ACCESS_KEY", ""),
                minio_secret_key=os.getenv("MINIO_SECRET_KEY", ""),
                minio_bucket=os.getenv("MINIO_BUCKET", "arc")
            )
        )

# Backward compatibility alias
HistorianConfig = ArcConfig
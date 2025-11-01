"""
Pydantic models for input validation and API contracts
"""
from pydantic import BaseModel, Field, field_validator, model_validator, RootModel
from typing import Optional, List, Dict, Union, Literal
from datetime import datetime
from enum import Enum
import re

# Pre-compiled regex patterns for performance (Issue #15)
# Compiling patterns once at module load prevents repeated compilation on every request
HOSTNAME_PATTERN = re.compile(r'^[a-zA-Z0-9.-]+$')
BUCKET_NAME_PATTERN = re.compile(r'^[a-zA-Z0-9.-]+$')
TIME_DURATION_PATTERN = re.compile(r'^\d+[mhd]$')
SQL_DANGEROUS_KEYWORDS = ['DROP', 'DELETE', 'INSERT', 'UPDATE', 'ALTER', 'CREATE', 'TRUNCATE']
# Pre-compile SQL keyword patterns with word boundaries
SQL_KEYWORD_PATTERNS = {
    keyword: re.compile(r'\b' + keyword + r'\b')
    for keyword in SQL_DANGEROUS_KEYWORDS
}

class InfluxVersionEnum(str, Enum):
    v1x = "1x"
    v2x = "2x"
    v3x = "3x"

class StorageBackendEnum(str, Enum):
    minio = "minio"
    s3 = "s3"
    gcs = "gcs"


class JobTypeEnum(str, Enum):
    measurement = "measurement"
    database = "database"

class InitialExportModeEnum(str, Enum):
    full = "full"
    from_date = "from_date"
    chunked = "chunked"
    retention_policy = "retention_policy"

# Base Models
class TimestampMixin(BaseModel):
    """Mixin for created_at/updated_at timestamps"""
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

# InfluxDB Connection Models
class InfluxDBConnectionBase(BaseModel):
    """Base InfluxDB connection model"""
    name: str = Field(..., min_length=1, max_length=100, description="Connection name")
    version: InfluxVersionEnum = Field(..., description="InfluxDB version")
    host: str = Field(..., min_length=1, max_length=255, description="InfluxDB host")
    port: int = Field(..., ge=1, le=65535, description="InfluxDB port")
    ssl: bool = Field(default=False, description="Use SSL/TLS")
    is_active: bool = Field(default=False, description="Set as active connection")

    @field_validator('host')
    @classmethod
    def validate_host(cls, v):
        # Basic hostname/IP validation using pre-compiled pattern
        if not HOSTNAME_PATTERN.match(v):
            raise ValueError('Invalid hostname format')
        return v

class InfluxDB1xConnection(InfluxDBConnectionBase):
    """InfluxDB 1.x connection model"""
    version: Literal["1x"] = "1x"
    database_name: str = Field(..., min_length=1, max_length=100, description="Database name")
    username: Optional[str] = Field(default="", max_length=100, description="Username")
    password: Optional[str] = Field(default="", max_length=200, description="Password")

class InfluxDB2xConnection(InfluxDBConnectionBase):
    """InfluxDB 2.x connection model"""
    version: Literal["2x"] = "2x"
    url: str = Field(..., description="InfluxDB URL")
    token: str = Field(..., min_length=1, description="Authentication token")
    org: str = Field(..., min_length=1, max_length=100, description="Organization")
    bucket: str = Field(..., min_length=1, max_length=100, description="Bucket name")

    @field_validator('url')
    @classmethod
    def validate_url(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('URL must start with http:// or https://')
        return v

class InfluxDB3xConnection(InfluxDBConnectionBase):
    """InfluxDB 3.x connection model"""
    version: Literal["3x"] = "3x"
    token: str = Field(..., min_length=1, description="Authentication token")
    org: str = Field(..., min_length=1, max_length=100, description="Organization")
    database: str = Field(..., min_length=1, max_length=100, description="Database name")

class InfluxDBConnectionCreate(RootModel[Union[InfluxDB1xConnection, InfluxDB2xConnection, InfluxDB3xConnection]]):
    """Union model for creating InfluxDB connections"""
    root: Union[InfluxDB1xConnection, InfluxDB2xConnection, InfluxDB3xConnection] = Field(..., discriminator='version')

class InfluxDBConnectionResponse(TimestampMixin):
    """InfluxDB connection response model"""
    id: int
    name: str
    version: str
    host: str
    port: int
    database_name: Optional[str] = None
    username: Optional[str] = None
    # password is never returned in responses
    token: Optional[str] = None  # Masked in actual responses
    org: Optional[str] = None
    bucket: Optional[str] = None
    database: Optional[str] = None  # For InfluxDB 3.x
    ssl: bool
    is_active: bool

# Storage Connection Models
class StorageConnectionBase(BaseModel):
    """Base storage connection model"""
    name: str = Field(..., min_length=1, max_length=100, description="Connection name")
    backend: StorageBackendEnum = Field(..., description="Storage backend type")
    access_key: str = Field(..., min_length=1, description="Access key")
    secret_key: str = Field(..., min_length=1, description="Secret key")
    bucket: str = Field(..., min_length=1, max_length=100, description="Bucket name")
    prefix: str = Field(default="", max_length=200, description="Key prefix")
    ssl: bool = Field(default=True, description="Use SSL/TLS")
    is_active: bool = Field(default=False, description="Set as active connection")

    @field_validator('bucket')
    @classmethod
    def validate_bucket_name(cls, v):
        # Basic bucket name validation using pre-compiled pattern
        if not BUCKET_NAME_PATTERN.match(v):
            raise ValueError('Invalid bucket name format')
        return v

class MinIOConnection(StorageConnectionBase):
    """MinIO storage connection model"""
    backend: Literal["minio"] = "minio"
    endpoint: str = Field(..., description="MinIO endpoint URL")
    ssl: bool = Field(default=False, description="Use SSL/TLS")

    @field_validator('endpoint')
    @classmethod
    def validate_endpoint(cls, v):
        if not v.startswith(('http://', 'https://')):
            raise ValueError('Endpoint must start with http:// or https://')
        return v

class S3Connection(StorageConnectionBase):
    """S3 storage connection model"""
    backend: Literal["s3"] = "s3"
    region: str = Field(default="us-east-1", max_length=50, description="AWS region")
    use_directory_bucket: bool = Field(default=False, description="Use S3 Directory Bucket")
    availability_zone: Optional[str] = Field(default=None, max_length=20, description="Availability zone for directory buckets")

    @model_validator(mode='before')
    @classmethod
    def validate_directory_bucket_config(cls, values):
        if isinstance(values, dict):
            use_directory_bucket = values.get('use_directory_bucket', False)
            availability_zone = values.get('availability_zone')
            bucket = values.get('bucket', '')
            
            if use_directory_bucket:
                if not availability_zone:
                    raise ValueError('availability_zone is required when use_directory_bucket is True')
                if not bucket.endswith('--x-s3'):
                    raise ValueError('Directory bucket name must end with --x-s3')
        
        return values

class GCSConnection(BaseModel):
    """Google Cloud Storage connection model"""
    name: str = Field(..., min_length=1, max_length=100, description="Connection name")
    backend: Literal["gcs"] = "gcs"
    bucket: str = Field(..., min_length=1, max_length=100, description="GCS bucket name")
    prefix: str = Field(default="", max_length=200, description="Object prefix")
    project_id: Optional[str] = Field(default=None, max_length=100, description="Google Cloud Project ID")
    # Authentication options (HMAC keys preferred for DuckDB)
    hmac_key_id: Optional[str] = Field(default=None, description="GCS HMAC Access Key ID (preferred for DuckDB)")
    hmac_secret: Optional[str] = Field(default=None, description="GCS HMAC Secret Key (preferred for DuckDB)")
    # Fallback service account auth
    credentials_json: Optional[str] = Field(default=None, description="Service account JSON credentials (fallback)")
    credentials_file: Optional[str] = Field(default=None, description="Path to service account JSON file (fallback)")
    is_active: bool = Field(default=False, description="Set as active connection")

    @model_validator(mode='before')
    @classmethod
    def validate_credentials(cls, values):
        if isinstance(values, dict):
            credentials_json = values.get('credentials_json')
            credentials_file = values.get('credentials_file')
            
            if not credentials_json and not credentials_file:
                # Allow default credentials (ADC) if no explicit credentials provided
                values['_use_default_credentials'] = True
        
        return values

class StorageConnectionCreate(RootModel[Union[MinIOConnection, S3Connection, GCSConnection]]):
    """Union model for creating storage connections"""
    root: Union[MinIOConnection, S3Connection, GCSConnection] = Field(..., discriminator='backend')

class StorageConnectionResponse(TimestampMixin):
    """Storage connection response model"""
    id: int
    name: str
    backend: str
    endpoint: Optional[str] = None
    # access_key and secret_key are masked in responses
    access_key_masked: Optional[str] = None
    bucket: str
    prefix: str
    region: Optional[str] = None
    ssl: Optional[bool] = None
    use_directory_bucket: Optional[bool] = None
    availability_zone: Optional[str] = None
    # GCS specific fields
    project_id: Optional[str] = None
    credentials_masked: Optional[str] = None  # Shows "configured" if credentials are set
    is_active: bool


# Export Job Models
class ExportJobBase(BaseModel):
    """Base export job model"""
    name: str = Field(..., min_length=1, max_length=100, description="Job name")
    job_type: JobTypeEnum = Field(..., description="Job type")
    influx_connection_id: int = Field(..., gt=0, description="Source InfluxDB connection ID")
    storage_connection_id: int = Field(..., gt=0, description="Destination storage connection ID")
    cron_schedule: str = Field(..., description="Cron schedule expression")
    chunk_size: str = Field(default="1d", description="Time chunk size (1h, 1d, 1w)")
    overlap_buffer: str = Field(default="5m", description="Overlap buffer")
    initial_export_mode: InitialExportModeEnum = Field(default="full", description="Initial export mode")
    max_retries: int = Field(default=3, ge=0, le=10, description="Maximum retry attempts")
    retry_delay: int = Field(default=300, ge=60, le=3600, description="Retry delay in seconds")
    is_active: bool = Field(default=True, description="Job is active")

    @field_validator('cron_schedule')
    @classmethod
    def validate_cron_schedule(cls, v):
        # Basic cron validation (5 parts: minute hour day month day_of_week)
        parts = v.split()
        if len(parts) != 5:
            raise ValueError('Cron schedule must have 5 parts: minute hour day month day_of_week')
        return v

    @field_validator('chunk_size', 'overlap_buffer')
    @classmethod
    def validate_time_duration(cls, v):
        # Validate time duration format using pre-compiled pattern (e.g., 1h, 30m, 1d)
        if not TIME_DURATION_PATTERN.match(v):
            raise ValueError('Time duration must be in format: number + unit (m/h/d)')
        return v

class MeasurementExportJob(ExportJobBase):
    """Measurement-specific export job"""
    job_type: Literal["measurement"] = "measurement"
    measurement: str = Field(..., min_length=1, max_length=100, description="Measurement name")

class DatabaseExportJob(ExportJobBase):
    """Database-wide export job"""
    job_type: Literal["database"] = "database"
    # measurement field is not needed for database exports

# Additional fields for retention policy mode
class RetentionPolicyExportJob(ExportJobBase):
    """Retention policy export job"""
    initial_export_mode: Literal["retention_policy"] = "retention_policy"
    retention_days: int = Field(default=365, ge=1, le=36500, description="Retention period in days")
    export_buffer_days: int = Field(default=7, ge=0, le=30, description="Export buffer days")

class ExportJobCreate(RootModel[Union[MeasurementExportJob, DatabaseExportJob]]):
    """Union model for creating export jobs"""
    root: Union[MeasurementExportJob, DatabaseExportJob] = Field(..., discriminator='job_type')

class ExportJobResponse(TimestampMixin):
    """Export job response model"""
    id: int
    name: str
    job_type: str
    measurement: Optional[str] = None
    influx_connection_id: int
    storage_connection_id: int
    cron_schedule: str
    chunk_size: str
    overlap_buffer: str
    initial_export_mode: str
    initial_start_date: Optional[str] = None
    initial_chunk_duration: Optional[str] = None
    retention_days: Optional[int] = None
    export_buffer_days: Optional[int] = None
    max_retries: int
    retry_delay: int
    is_active: bool

# Query Models
class QueryRequest(BaseModel):
    """SQL query request model"""
    sql: str = Field(..., min_length=1, max_length=10000, description="SQL query")
    limit: int = Field(default=1000, ge=1, description="Maximum rows to return (use responsibly)")
    format: Literal["json", "csv"] = Field(default="json", description="Response format")

    @field_validator('sql')
    @classmethod
    def validate_sql(cls, v):
        # Basic SQL injection prevention using pre-compiled patterns with word boundaries
        # This prevents false positives like "drop_in" or "update_time" column names
        sql_upper = v.upper()
        for keyword in SQL_DANGEROUS_KEYWORDS:
            # Use pre-compiled patterns for performance
            if SQL_KEYWORD_PATTERNS[keyword].search(sql_upper):
                raise ValueError(f'SQL keyword "{keyword}" is not allowed')
        return v

class QueryResponse(BaseModel):
    """SQL query response model"""
    success: bool = True
    columns: List[str]
    data: List[List]
    row_count: int
    execution_time_ms: float
    timestamp: datetime
    error: Optional[str] = None

# Connection Test Models
class ConnectionTestRequest(BaseModel):
    """Connection test request model"""
    connection_type: Literal["influx", "storage"]
    connection_data: Dict

class ConnectionTestResponse(BaseModel):
    """Connection test response model"""
    success: bool
    message: str
    details: Optional[Dict] = None
    response_time_ms: Optional[float] = None

# Health Check Models
class HealthResponse(BaseModel):
    """Health check response model"""
    status: Literal["healthy", "unhealthy"]
    service: str
    version: str
    timestamp: datetime

class ReadinessResponse(BaseModel):
    """Readiness check response model"""
    status: Literal["ready", "not_ready"]
    checks: Dict[str, bool]
    details: Dict[str, Union[bool, int, str, None]]
    message: str
    timestamp: datetime


# Error Models
class ErrorResponse(BaseModel):
    """Standard error response model"""
    error: str
    detail: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.now)

# Configuration Models
class APIConfigResponse(BaseModel):
    """API configuration response"""
    cors_origins: List[str]
    max_query_limit: int
    default_query_limit: int
    supported_influx_versions: List[str]
    supported_storage_backends: List[str]
# Token Management Models
# Simple Token Models with Permissions
class TokenCreateRequest(BaseModel):
    """Create token request model"""
    name: str = Field(..., min_length=1, max_length=100, description="Token name")
    description: Optional[str] = Field(default=None, max_length=500, description="Token description")
    expires_at: Optional[datetime] = Field(default=None, description="Token expiration date (optional)")
    permissions: str = Field(
        default="read,write",
        description="Comma-separated permissions: read, write, delete, admin (default: read,write)"
    )

    @field_validator('permissions')
    @classmethod
    def validate_permissions(cls, v):
        """Validate permissions format and values"""
        if not v:
            return "read,write"

        # Split and clean permissions
        perms = [p.strip().lower() for p in v.split(',')]

        # Valid permission values
        valid_perms = {'read', 'write', 'delete', 'admin'}

        # Check all permissions are valid
        for perm in perms:
            if perm not in valid_perms:
                raise ValueError(f"Invalid permission '{perm}'. Valid permissions: {', '.join(valid_perms)}")

        # Return cleaned permissions
        return ','.join(perms)


class TokenUpdateRequest(BaseModel):
    """Update token request model"""
    name: Optional[str] = Field(default=None, min_length=1, max_length=100, description="Token name")
    description: Optional[str] = Field(default=None, max_length=500, description="Token description")
    expires_at: Optional[datetime] = Field(default=None, description="Token expiration date")
    permissions: Optional[str] = Field(
        default=None,
        description="Comma-separated permissions: read, write, delete, admin"
    )

    @field_validator('permissions')
    @classmethod
    def validate_permissions(cls, v):
        """Validate permissions format and values"""
        if v is None:
            return None

        # Split and clean permissions
        perms = [p.strip().lower() for p in v.split(',')]

        # Valid permission values
        valid_perms = {'read', 'write', 'delete', 'admin'}

        # Check all permissions are valid
        for perm in perms:
            if perm not in valid_perms:
                raise ValueError(f"Invalid permission '{perm}'. Valid permissions: {', '.join(valid_perms)}")

        # Return cleaned permissions
        return ','.join(perms)


class TokenResponse(BaseModel):
    """Token response model (includes actual token only on creation)"""
    id: int
    name: str
    description: Optional[str] = None
    created_at: str
    last_used_at: Optional[str] = None
    enabled: bool = True
    permissions: List[str] = Field(default_factory=lambda: ["read", "write"], description="List of permissions")
    token: Optional[str] = Field(default=None, description="Actual token (only returned on creation)")


class TokenListResponse(BaseModel):
    """Token list response model"""
    tokens: List[TokenResponse]
    count: int

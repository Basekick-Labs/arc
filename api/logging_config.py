"""
Structured logging configuration for Historian API
"""
import logging
import json
import sys
import os
from datetime import datetime
from typing import Dict, Any, Optional
import uuid
from contextvars import ContextVar
import asyncio

# Context variable for request tracking
request_id_context: ContextVar[Optional[str]] = ContextVar('request_id', default=None)

class StructuredFormatter(logging.Formatter):
    """Custom formatter for structured JSON logging"""
    
    def __init__(self, service_name: str = "historian-api", include_trace: bool = False):
        super().__init__()
        self.service_name = service_name
        self.include_trace = include_trace
    
    def format(self, record: logging.LogRecord) -> str:
        """Format log record as structured JSON"""
        # Get request ID from context if available
        request_id = request_id_context.get()
        
        # Base log structure
        log_entry = {
            "timestamp": datetime.utcfromtimestamp(record.created).isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "service": self.service_name,
        }
        
        # Add request ID if available
        if request_id:
            log_entry["request_id"] = request_id
        
        # Add thread and process info for debugging
        if self.include_trace:
            log_entry.update({
                "thread": record.thread,
                "thread_name": record.threadName,
                "process": record.process,
                "filename": record.filename,
                "function": record.funcName,
                "line_number": record.lineno,
            })
        
        # Add exception info if present
        if record.exc_info:
            log_entry["exception"] = {
                "type": record.exc_info[0].__name__ if record.exc_info[0] else None,
                "message": str(record.exc_info[1]) if record.exc_info[1] else None,
                "traceback": self.formatException(record.exc_info) if record.exc_info else None
            }
        
        # Add any extra fields from the log record
        extra_fields = {}
        for key, value in record.__dict__.items():
            if key not in ['name', 'msg', 'args', 'levelname', 'levelno', 'pathname', 
                          'filename', 'module', 'exc_info', 'exc_text', 'stack_info',
                          'lineno', 'funcName', 'created', 'msecs', 'relativeCreated',
                          'thread', 'threadName', 'processName', 'process', 'message']:
                extra_fields[key] = value
        
        if extra_fields:
            log_entry["extra"] = extra_fields
        
        return json.dumps(log_entry, default=str)

class RequestIdMiddleware:
    """Middleware to add request IDs to logging context"""
    
    def __init__(self, app):
        self.app = app
    
    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            # Generate or extract request ID
            request_id = self._get_or_generate_request_id(scope)
            
            # Set request ID in context
            request_id_context.set(request_id)
            
            # Add request ID to response headers
            async def send_with_request_id(message):
                if message["type"] == "http.response.start":
                    headers = list(message.get("headers", []))
                    headers.append([b"X-Request-ID", request_id.encode()])
                    message["headers"] = headers
                await send(message)
            
            await self.app(scope, receive, send_with_request_id)
        else:
            await self.app(scope, receive, send)
    
    def _get_or_generate_request_id(self, scope) -> str:
        """Get request ID from headers or generate new one"""
        headers = dict(scope.get("headers", []))
        
        # Check for existing request ID in headers
        request_id = headers.get(b"x-request-id")
        if request_id:
            return request_id.decode()
        
        # Generate new UUID-based request ID
        return str(uuid.uuid4())

def setup_logging(
    service_name: str = "historian-api",
    level: str = "INFO",
    structured: bool = True,
    include_trace: bool = False
) -> None:
    """Configure application logging"""
    
    # Get log level from environment or parameter
    log_level = os.getenv("LOG_LEVEL", level).upper()
    
    # Clear existing handlers
    root_logger = logging.getLogger()
    root_logger.handlers.clear()
    
    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    
    if structured:
        # Use structured JSON formatter
        formatter = StructuredFormatter(service_name=service_name, include_trace=include_trace)
    else:
        # Use traditional formatter for development
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
    
    handler.setFormatter(formatter)
    
    # Configure root logger
    root_logger.addHandler(handler)
    root_logger.setLevel(getattr(logging, log_level))
    
    # Configure specific loggers to reduce noise
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)

def get_logger(name: str) -> logging.Logger:
    """Get a logger with the given name"""
    return logging.getLogger(name)

# Structured logging helpers
def log_api_call(logger: logging.Logger, method: str, endpoint: str, 
                status_code: int, duration_ms: float, **kwargs):
    """Log API call with structured data"""
    logger.info(
        f"API call completed",
        extra={
            "event_type": "api_call",
            "http_method": method,
            "endpoint": endpoint,
            "status_code": status_code,
            "duration_ms": duration_ms,
            **kwargs
        }
    )

def log_database_operation(logger: logging.Logger, operation: str, 
                          duration_ms: float, success: bool, **kwargs):
    """Log database operation with structured data"""
    logger.info(
        f"Database {operation} completed",
        extra={
            "event_type": "database_operation", 
            "operation": operation,
            "duration_ms": duration_ms,
            "success": success,
            **kwargs
        }
    )

# REMOVED: log_export_job (moved to external importers)

def log_query_execution(logger: logging.Logger, sql: str, duration_ms: float,
                       row_count: int, success: bool, **kwargs):
    """Log SQL query execution with structured data"""
    # Truncate long queries for logging
    sql_snippet = sql[:200] + "..." if len(sql) > 200 else sql
    
    logger.info(
        f"Query executed",
        extra={
            "event_type": "query_execution",
            "sql_snippet": sql_snippet,
            "duration_ms": duration_ms,
            "row_count": row_count,
            "success": success,
            **kwargs
        }
    )

def log_connection_test(logger: logging.Logger, connection_type: str,
                       connection_name: str, success: bool, 
                       duration_ms: float = None, error: str = None):
    """Log connection test with structured data"""
    extra_data = {
        "event_type": "connection_test",
        "connection_type": connection_type,
        "connection_name": connection_name,
        "success": success,
    }
    
    if duration_ms is not None:
        extra_data["duration_ms"] = duration_ms
    if error:
        extra_data["error"] = error
    
    if success:
        logger.info("Connection test succeeded", extra=extra_data)
    else:
        logger.error("Connection test failed", extra=extra_data)

class LoggingContextManager:
    """Context manager for adding structured logging context"""
    
    def __init__(self, **context_data):
        self.context_data = context_data
        self.old_factory = None
    
    def __enter__(self):
        # Store the old factory
        self.old_factory = logging.getLogRecordFactory()
        
        # Create new factory that adds context
        def record_factory(*args, **kwargs):
            record = self.old_factory(*args, **kwargs)
            for key, value in self.context_data.items():
                setattr(record, key, value)
            return record
        
        logging.setLogRecordFactory(record_factory)
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        # Restore the old factory
        logging.setLogRecordFactory(self.old_factory)

# Convenience function for adding context to logs
def with_logging_context(**context_data):
    """Decorator or context manager for adding logging context"""
    return LoggingContextManager(**context_data)
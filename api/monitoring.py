"""
Monitoring and metrics collection for Historian API
"""
import time
import psutil
import os
import gc
import sys
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from collections import defaultdict, deque
import threading
import logging

logger = logging.getLogger(__name__)

@dataclass
class MetricSample:
    """Single metric sample with timestamp"""
    timestamp: datetime
    value: float
    labels: Dict[str, str] = None

@dataclass
class SystemMetrics:
    """System-level metrics"""
    timestamp: datetime
    cpu_percent: float
    memory_percent: float
    memory_used_mb: float
    memory_available_mb: float
    disk_percent: float
    disk_used_gb: float
    disk_free_gb: float
    network_bytes_sent: int
    network_bytes_recv: int
    process_count: int
    load_average: List[float]

@dataclass
class ApplicationMetrics:
    """Application-level metrics"""
    timestamp: datetime
    active_connections: int
    active_influx_connections: int
    active_storage_connections: int
    total_export_jobs: int
    active_export_jobs: int
    query_engine_status: bool
    scheduler_status: bool

@dataclass
class APIMetrics:
    """API performance metrics"""
    timestamp: datetime
    requests_per_minute: float
    avg_response_time_ms: float
    error_rate_percent: float
    active_requests: int
    total_requests: int
    successful_requests: int
    failed_requests: int

class MetricsCollector:
    """Collects and stores application metrics"""
    
    def __init__(self, retention_minutes: int = 60, sample_interval_seconds: int = 10):
        self.retention_minutes = retention_minutes
        self.sample_interval_seconds = sample_interval_seconds

        # Metric storage (using deque for efficient rotation)
        self.system_metrics: deque = deque(maxlen=retention_minutes * 6)  # 10-second intervals
        self.api_metrics: deque = deque(maxlen=retention_minutes * 6)
        self.application_metrics: deque = deque(maxlen=retention_minutes * 6)

        # API request tracking
        self.request_counter = 0
        self.error_counter = 0
        self.response_times = deque(maxlen=1000)  # Last 1000 requests

        # Metrics collection error tracking
        self.metrics_collection_errors = 0
        self.last_collection_error = None
        self.last_collection_error_time = None
        self.active_requests = 0
        self.requests_by_endpoint = defaultdict(int)
        self.request_times = deque(maxlen=1000)  # For requests per minute calculation
        
        # Threading
        self._stop_event = threading.Event()
        self._collection_thread: Optional[threading.Thread] = None
        
        # External dependencies
        self.connection_manager = None
        self.export_scheduler = None
        self.query_engine = None
    
    def set_dependencies(self, connection_manager=None, export_scheduler=None, query_engine=None):
        """Set external dependencies for metrics collection"""
        self.connection_manager = connection_manager
        self.export_scheduler = export_scheduler
        self.query_engine = query_engine
    
    def start_collection(self):
        """Start background metrics collection"""
        if self._collection_thread and self._collection_thread.is_alive():
            logger.warning("Metrics collection already running")
            return
        
        self._stop_event.clear()
        self._collection_thread = threading.Thread(target=self._collection_loop, daemon=True)
        self._collection_thread.start()
        logger.info("Started metrics collection")
    
    def stop_collection(self):
        """Stop background metrics collection"""
        if self._collection_thread:
            self._stop_event.set()
            self._collection_thread.join(timeout=5)
            logger.info("Stopped metrics collection")
    
    def _collection_loop(self):
        """Main collection loop"""
        while not self._stop_event.is_set():
            try:
                now = datetime.now()
                
                # Collect system metrics
                system_metrics = self._collect_system_metrics(now)
                self.system_metrics.append(system_metrics)
                
                # Collect application metrics
                app_metrics = self._collect_application_metrics(now)
                self.application_metrics.append(app_metrics)
                
                # Collect API metrics
                api_metrics = self._collect_api_metrics(now)
                self.api_metrics.append(api_metrics)

                # Reset error counter on successful collection
                if self.metrics_collection_errors > 0:
                    logger.info(f"Metrics collection recovered after {self.metrics_collection_errors} errors")
                    self.metrics_collection_errors = 0

            except Exception as e:
                # Track metrics collection errors
                self.metrics_collection_errors += 1
                self.last_collection_error = str(e)
                self.last_collection_error_time = now

                # Log error with context
                logger.error(f"Error collecting metrics: {e} (total errors: {self.metrics_collection_errors})")

                # Alert if sustained failures (10+ errors in a row)
                if self.metrics_collection_errors >= 10:
                    logger.critical(
                        f"ALERT: Metrics collection failing continuously! "
                        f"Errors: {self.metrics_collection_errors}, Last error: {self.last_collection_error}"
                    )

            # Wait for next collection cycle
            self._stop_event.wait(self.sample_interval_seconds)
    
    def _collect_system_metrics(self, timestamp: datetime) -> SystemMetrics:
        """Collect system-level metrics"""
        try:
            # CPU and memory
            cpu_percent = psutil.cpu_percent(interval=0.1)
            memory = psutil.virtual_memory()
            
            # Disk usage for root partition
            disk = psutil.disk_usage('/')
            
            # Network I/O
            network = psutil.net_io_counters()
            
            # System load
            try:
                load_avg = list(psutil.getloadavg())
            except AttributeError:
                # Windows doesn't have getloadavg
                load_avg = [0.0, 0.0, 0.0]
            
            return SystemMetrics(
                timestamp=timestamp,
                cpu_percent=cpu_percent,
                memory_percent=memory.percent,
                memory_used_mb=memory.used / (1024 * 1024),
                memory_available_mb=memory.available / (1024 * 1024),
                disk_percent=disk.percent,
                disk_used_gb=disk.used / (1024 * 1024 * 1024),
                disk_free_gb=disk.free / (1024 * 1024 * 1024),
                network_bytes_sent=network.bytes_sent,
                network_bytes_recv=network.bytes_recv,
                process_count=len(psutil.pids()),
                load_average=load_avg
            )
        except Exception as e:
            logger.error(f"Error collecting system metrics: {e}")
            return SystemMetrics(
                timestamp=timestamp,
                cpu_percent=0.0, memory_percent=0.0, memory_used_mb=0.0,
                memory_available_mb=0.0, disk_percent=0.0, disk_used_gb=0.0,
                disk_free_gb=0.0, network_bytes_sent=0, network_bytes_recv=0,
                process_count=0, load_average=[0.0, 0.0, 0.0]
            )
    
    def _collect_application_metrics(self, timestamp: datetime) -> ApplicationMetrics:
        """Collect application-specific metrics"""
        try:
            active_influx = 0
            active_storage = 0
            
            if self.connection_manager:
                try:
                    influx_connections = self.connection_manager.get_influx_connections()
                    storage_connections = self.connection_manager.get_storage_connections()
                    active_influx = len([c for c in influx_connections if c.get('is_active')])
                    active_storage = len([c for c in storage_connections if c.get('is_active')])
                except Exception:
                    pass
            
            total_jobs = 0
            active_jobs = 0
            scheduler_running = False
            
            if self.export_scheduler:
                try:
                    jobs = self.export_scheduler.get_jobs()
                    total_jobs = len(jobs)
                    active_jobs = len([j for j in jobs if j.get('is_active')])
                    scheduler_running = self.export_scheduler.is_running()
                except Exception:
                    pass
            
            
            query_engine_ready = self.query_engine is not None
            
            return ApplicationMetrics(
                timestamp=timestamp,
                active_connections=active_influx + active_storage,
                active_influx_connections=active_influx,
                active_storage_connections=active_storage,
                total_export_jobs=total_jobs,
                active_export_jobs=active_jobs,
                query_engine_status=query_engine_ready,
                scheduler_status=scheduler_running
            )
        except Exception as e:
            logger.error(f"Error collecting application metrics: {e}")
            return ApplicationMetrics(
                timestamp=timestamp,
                active_connections=0,
                active_influx_connections=0,
                active_storage_connections=0,
                total_export_jobs=0,
                active_export_jobs=0,
                query_engine_status=False,
                scheduler_status=False
            )
    
    def _collect_api_metrics(self, timestamp: datetime) -> APIMetrics:
        """Collect API performance metrics"""
        try:
            # Calculate requests per minute
            cutoff_time = timestamp - timedelta(minutes=1)
            recent_requests = [t for t in self.request_times if t > cutoff_time]
            requests_per_minute = len(recent_requests)
            
            # Calculate average response time
            avg_response_time = sum(self.response_times) / len(self.response_times) if self.response_times else 0.0
            
            # Calculate error rate
            total_requests = self.request_counter
            error_rate = (self.error_counter / total_requests * 100) if total_requests > 0 else 0.0
            
            return APIMetrics(
                timestamp=timestamp,
                requests_per_minute=float(requests_per_minute),
                avg_response_time_ms=avg_response_time,
                error_rate_percent=error_rate,
                active_requests=self.active_requests,
                total_requests=total_requests,
                successful_requests=total_requests - self.error_counter,
                failed_requests=self.error_counter
            )
        except Exception as e:
            logger.error(f"Error collecting API metrics: {e}")
            return APIMetrics(
                timestamp=timestamp,
                requests_per_minute=0.0,
                avg_response_time_ms=0.0,
                error_rate_percent=0.0,
                active_requests=0,
                total_requests=0,
                successful_requests=0,
                failed_requests=0
            )
    
    # API request tracking methods
    def record_request_start(self, endpoint: str):
        """Record the start of an API request"""
        self.active_requests += 1
        self.request_counter += 1
        self.requests_by_endpoint[endpoint] += 1
        self.request_times.append(datetime.now())
    
    def record_request_end(self, duration_ms: float, success: bool = True):
        """Record the end of an API request"""
        self.active_requests = max(0, self.active_requests - 1)
        self.response_times.append(duration_ms)
        
        if not success:
            self.error_counter += 1
    
    def get_current_metrics(self) -> Dict[str, Any]:
        """Get current metrics snapshot"""
        now = datetime.now()
        
        # Get latest metrics (or create default if none collected yet)
        latest_system = list(self.system_metrics)[-1] if self.system_metrics else self._collect_system_metrics(now)
        latest_app = list(self.application_metrics)[-1] if self.application_metrics else self._collect_application_metrics(now)
        latest_api = list(self.api_metrics)[-1] if self.api_metrics else self._collect_api_metrics(now)
        
        return {
            "timestamp": now.isoformat(),
            "system": asdict(latest_system),
            "application": asdict(latest_app),
            "api": asdict(latest_api),
            "collection_status": {
                "running": self._collection_thread and self._collection_thread.is_alive(),
                "samples_collected": len(self.system_metrics),
                "retention_minutes": self.retention_minutes
            }
        }
    
    def get_time_series(self, metric_type: str, duration_minutes: int = 30) -> List[Dict[str, Any]]:
        """Get time series data for a specific metric type"""
        cutoff_time = datetime.now() - timedelta(minutes=duration_minutes)
        
        if metric_type == "system":
            data = [asdict(m) for m in self.system_metrics if m.timestamp > cutoff_time]
        elif metric_type == "application":
            data = [asdict(m) for m in self.application_metrics if m.timestamp > cutoff_time]
        elif metric_type == "api":
            data = [asdict(m) for m in self.api_metrics if m.timestamp > cutoff_time]
        else:
            return []
        
        return data
    
    def get_endpoint_stats(self) -> Dict[str, int]:
        """Get request count by endpoint"""
        return dict(self.requests_by_endpoint)

    def health_check(self) -> Dict[str, Any]:
        """
        Health check for metrics collection

        Returns status of metrics collection including error rates and warnings.
        This allows monitoring systems to detect when metrics collection is failing.

        Returns:
            Dict with health status, error counts, and warnings
        """
        is_healthy = True
        warnings = []

        # Check if collection thread is running
        if not (self._collection_thread and self._collection_thread.is_alive()):
            is_healthy = False
            warnings.append("Metrics collection thread is not running")

        # Check for sustained failures
        if self.metrics_collection_errors > 0:
            if self.metrics_collection_errors >= 10:
                is_healthy = False
                warnings.append(
                    f"CRITICAL: Metrics collection has failed {self.metrics_collection_errors} times consecutively"
                )
            elif self.metrics_collection_errors >= 3:
                warnings.append(
                    f"WARNING: Metrics collection has failed {self.metrics_collection_errors} times consecutively"
                )

        # Check if samples are being collected
        samples_count = len(self.system_metrics)
        if samples_count == 0 and self._collection_thread and self._collection_thread.is_alive():
            warnings.append("No metrics samples collected yet (collection may have just started)")

        # Calculate time since last error
        time_since_last_error = None
        if self.last_collection_error_time:
            time_since_last_error = (datetime.now() - self.last_collection_error_time).total_seconds()

        return {
            "healthy": is_healthy,
            "collection_running": self._collection_thread and self._collection_thread.is_alive(),
            "consecutive_errors": self.metrics_collection_errors,
            "last_error": self.last_collection_error,
            "last_error_time": self.last_collection_error_time.isoformat() if self.last_collection_error_time else None,
            "time_since_last_error_seconds": time_since_last_error,
            "samples_collected": samples_count,
            "warnings": warnings
        }

# Global metrics collector instance
metrics_collector = MetricsCollector()

def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector instance"""
    return metrics_collector


def get_memory_profile() -> Dict[str, Any]:
    """
    Get detailed memory profiling for the Arc API process.

    Returns detailed breakdown of memory usage including:
    - Process memory (RSS, VMS, shared, etc.)
    - Python heap statistics
    - Garbage collector stats
    - Top memory consumers by type
    """
    process = psutil.Process(os.getpid())

    # Process memory info
    mem_info = process.memory_info()
    mem_percent = process.memory_percent()

    # Python GC stats
    gc_stats = gc.get_stats()
    gc_counts = gc.get_count()

    # Object counts by type (top 20)
    try:
        import tracemalloc
        tracemalloc_enabled = tracemalloc.is_tracing()
        if tracemalloc_enabled:
            snapshot = tracemalloc.take_snapshot()
            top_stats = snapshot.statistics('lineno')[:20]
            tracemalloc_stats = [
                {
                    'file': str(stat.traceback),
                    'size_mb': stat.size / (1024 * 1024),
                    'count': stat.count
                }
                for stat in top_stats
            ]
        else:
            tracemalloc_stats = None
    except Exception:
        tracemalloc_stats = None

    # Count objects by type
    import collections
    type_counts = collections.Counter()
    for obj in gc.get_objects():
        type_counts[type(obj).__name__] += 1

    return {
        'timestamp': datetime.now().isoformat(),
        'process': {
            'pid': process.pid,
            'memory_percent': round(mem_percent, 2),
            'rss_mb': round(mem_info.rss / (1024 * 1024), 2),  # Resident Set Size
            'vms_mb': round(mem_info.vms / (1024 * 1024), 2),  # Virtual Memory Size
            'shared_mb': round(mem_info.shared / (1024 * 1024), 2) if hasattr(mem_info, 'shared') else None,
            'num_threads': process.num_threads(),
            'num_fds': process.num_fds() if hasattr(process, 'num_fds') else None,
            'cpu_percent': process.cpu_percent(interval=0.1),
        },
        'python': {
            'version': sys.version,
            'gc_enabled': gc.isenabled(),
            'gc_thresholds': gc.get_threshold(),
            'gc_counts': {
                'gen0': gc_counts[0],
                'gen1': gc_counts[1],
                'gen2': gc_counts[2],
            },
            'total_objects': len(gc.get_objects()),
            'top_object_types': dict(type_counts.most_common(20)),
        },
        'tracemalloc': {
            'enabled': tracemalloc_enabled if 'tracemalloc_enabled' in locals() else False,
            'top_allocations': tracemalloc_stats if tracemalloc_stats else []
        },
        'recommendations': _get_memory_recommendations(mem_info, gc_counts, type_counts)
    }


def _get_memory_recommendations(mem_info, gc_counts, type_counts) -> List[str]:
    """Generate memory optimization recommendations"""
    recommendations = []

    rss_mb = mem_info.rss / (1024 * 1024)

    if rss_mb > 500:
        recommendations.append("High memory usage (>500MB). Consider reducing WRITE_BUFFER_SIZE or DUCKDB_POOL_SIZE.")

    if gc_counts[2] > 100:
        recommendations.append(f"High Gen2 GC count ({gc_counts[2]}). May indicate memory fragmentation.")

    # Check for buffer accumulation
    if type_counts.get('dict', 0) > 50000:
        recommendations.append(f"Large number of dicts ({type_counts['dict']:,}). Check if write buffers are flushing correctly.")

    if type_counts.get('list', 0) > 50000:
        recommendations.append(f"Large number of lists ({type_counts['list']:,}). Possible buffer accumulation.")

    if not recommendations:
        recommendations.append("Memory usage looks healthy!")

    return recommendations
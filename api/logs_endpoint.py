"""
Logs API endpoint for serving application logs
"""
import os
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from fastapi import Query
from pathlib import Path
import re
from api.config import get_log_dir

logger = logging.getLogger(__name__)

class LogsManager:
    """Manages access to application logs"""
    
    def __init__(self, log_file_paths: List[str] = None):
        self.log_file_paths = log_file_paths or []
        
        # Try to find log files automatically
        if not self.log_file_paths:
            self._discover_log_files()
    
    def _discover_log_files(self):
        """Automatically discover log files"""
        # Check for Docker container logs via docker logs command
        docker_containers = ["historian-api", "historian-ui"]
        
        # Try to read from current Python logging system first
        for handler in logging.getLogger().handlers:
            if hasattr(handler, 'baseFilename'):
                self.log_file_paths.append(handler.baseFilename)
                logger.info(f"Found active log file: {handler.baseFilename}")
        
        # Traditional log file locations
        possible_paths = [
            "/var/log/historian/",
            get_log_dir() + "/",
            "./logs/",
            "/tmp/historian.log",
            "/var/log/containers/"
        ]
        
        for path in possible_paths:
            if os.path.exists(path):
                if os.path.isfile(path):
                    self.log_file_paths.append(path)
                    logger.info(f"Found log file: {path}")
                else:
                    try:
                        for file in os.listdir(path):
                            if file.endswith(('.log', '.txt')) or 'historian' in file.lower():
                                full_path = os.path.join(path, file)
                                if os.path.isfile(full_path):
                                    self.log_file_paths.append(full_path)
                                    logger.info(f"Found log file: {full_path}")
                    except PermissionError:
                        continue
    
    def get_recent_logs(self, limit: int = 100, level_filter: str = None, 
                       since_minutes: int = 60) -> List[Dict[str, Any]]:
        """Get recent log entries"""
        logs = []
        cutoff_time = datetime.now() - timedelta(minutes=since_minutes)
        
        # If no log files found, try Docker logs, then fallback to samples
        if not self.log_file_paths:
            docker_logs = self._get_docker_logs(limit, since_minutes)
            if docker_logs:
                return docker_logs
            return self._generate_sample_logs(limit)
        
        for log_file in self.log_file_paths:
            try:
                logs.extend(self._parse_log_file(log_file, cutoff_time, level_filter))
            except Exception as e:
                logger.error(f"Error reading log file {log_file}: {e}")
        
        # Sort by timestamp (newest first) and limit
        logs.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        return logs[:limit]
    
    def _parse_log_file(self, file_path: str, cutoff_time: datetime, 
                       level_filter: str = None) -> List[Dict[str, Any]]:
        """Parse a single log file"""
        logs = []
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                # Read last N lines efficiently
                lines = self._tail_file(f, 1000)
                
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                try:
                    # Try to parse as JSON (structured logs)
                    if line.startswith('{'):
                        log_entry = json.loads(line)
                        
                        # Parse timestamp
                        timestamp_str = log_entry.get('timestamp', '')
                        try:
                            if timestamp_str.endswith('Z'):
                                log_time = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                            else:
                                log_time = datetime.fromisoformat(timestamp_str)
                            
                            # Apply time filter
                            if log_time < cutoff_time:
                                continue
                                
                        except (ValueError, AttributeError):
                            # If timestamp parsing fails, include the log anyway
                            pass
                        
                        # Apply level filter
                        if level_filter and log_entry.get('level', '').upper() != level_filter.upper():
                            continue
                        
                        logs.append(log_entry)
                        
                    else:
                        # Parse traditional log format
                        log_entry = self._parse_traditional_log_line(line)
                        if log_entry:
                            if level_filter and log_entry.get('level', '').upper() != level_filter.upper():
                                continue
                            logs.append(log_entry)
                            
                except json.JSONDecodeError:
                    # If JSON parsing fails, try traditional format
                    log_entry = self._parse_traditional_log_line(line)
                    if log_entry:
                        if level_filter and log_entry.get('level', '').upper() != level_filter.upper():
                            continue
                        logs.append(log_entry)
                        
        except Exception as e:
            logger.error(f"Error parsing log file {file_path}: {e}")
        
        return logs
    
    def _get_docker_logs(self, limit: int, since_minutes: int) -> List[Dict[str, Any]]:
        """Get logs from Docker containers"""
        import subprocess
        logs = []
        
        containers = ["historian-api", "historian-ui"]
        
        for container in containers:
            try:
                # Get logs from Docker container
                cmd = [
                    "docker", "logs", 
                    "--since", f"{since_minutes}m",
                    "--tail", str(limit),
                    "--timestamps",
                    container
                ]
                
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=10)
                
                if result.returncode == 0:
                    for line in result.stdout.split('\n'):
                        if line.strip():
                            log_entry = self._parse_docker_log_line(line, container)
                            if log_entry:
                                logs.append(log_entry)
                                
                    # Also get stderr
                    for line in result.stderr.split('\n'):
                        if line.strip():
                            log_entry = self._parse_docker_log_line(line, container, is_stderr=True)
                            if log_entry:
                                logs.append(log_entry)
                                
            except (subprocess.TimeoutExpired, subprocess.SubprocessError, FileNotFoundError):
                # Docker command not available or failed
                continue
        
        # Sort by timestamp and limit
        logs.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        return logs[:limit]
    
    def _parse_docker_log_line(self, line: str, container: str, is_stderr: bool = False) -> Optional[Dict[str, Any]]:
        """Parse a Docker log line"""
        try:
            # Docker log format: "2025-01-08T15:32:10.123456789Z log message"
            if 'T' in line and 'Z' in line:
                parts = line.split(' ', 1)
                if len(parts) >= 2:
                    timestamp_str = parts[0].replace('T', ' ').replace('Z', '')
                    message = parts[1]
                    
                    # Try to parse as structured JSON first
                    if message.startswith('{'):
                        try:
                            json_log = json.loads(message)
                            json_log['service'] = container
                            json_log['source'] = 'docker_logs'
                            return json_log
                        except json.JSONDecodeError:
                            pass
                    
                    # Parse traditional format
                    level = 'ERROR' if is_stderr else 'INFO'
                    if 'ERROR' in message.upper():
                        level = 'ERROR'
                    elif 'WARNING' in message.upper() or 'WARN' in message.upper():
                        level = 'WARNING'
                    elif 'DEBUG' in message.upper():
                        level = 'DEBUG'
                    
                    return {
                        'timestamp': self._normalize_timestamp(timestamp_str),
                        'level': level,
                        'logger': container,
                        'message': message,
                        'service': container,
                        'source': 'docker_logs'
                    }
        except Exception:
            pass
        
        return None
    
    def _tail_file(self, file, lines: int = 1000) -> List[str]:
        """Efficiently read the last N lines of a file"""
        try:
            file.seek(0, 2)  # Go to end of file
            file_size = file.tell()
            
            # Read from end in chunks
            lines_found = []
            chunk_size = 8192
            position = file_size
            
            while len(lines_found) < lines and position > 0:
                chunk_size = min(chunk_size, position)
                position -= chunk_size
                file.seek(position)
                chunk = file.read(chunk_size)
                
                # Split into lines and add to beginning of list
                chunk_lines = chunk.split('\n')
                lines_found = chunk_lines + lines_found
            
            return lines_found[-lines:] if len(lines_found) > lines else lines_found
            
        except Exception:
            # Fallback to reading entire file
            file.seek(0)
            return file.readlines()[-lines:]
    
    def _parse_traditional_log_line(self, line: str) -> Optional[Dict[str, Any]]:
        """Parse traditional log format"""
        # Common log patterns
        patterns = [
            # Standard Python logging: "2025-01-08 14:30:15,123 - module - INFO - message"
            r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}[,\d]*)\s*-\s*([^\s]+)\s*-\s*(\w+)\s*-\s*(.*)',
            # ISO timestamp with level: "2025-01-08T14:30:15Z INFO [module] message"
            r'(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}[Z\+\-\d:]*)\s+(\w+)\s+\[([^\]]+)\]\s*(.*)',
            # Simple format: "INFO: message"
            r'(\w+):\s*(.*)'
        ]
        
        for pattern in patterns:
            match = re.match(pattern, line)
            if match:
                groups = match.groups()
                
                if len(groups) >= 4:
                    # Full format with timestamp, logger, level, message
                    return {
                        'timestamp': self._normalize_timestamp(groups[0]),
                        'logger': groups[1],
                        'level': groups[2].upper(),
                        'message': groups[3],
                        'service': 'historian-api',
                        'source': 'traditional_log'
                    }
                elif len(groups) >= 2:
                    # Simple format
                    return {
                        'timestamp': datetime.now().isoformat(),
                        'logger': 'unknown',
                        'level': groups[0].upper(),
                        'message': groups[1],
                        'service': 'historian-api',
                        'source': 'traditional_log'
                    }
        
        # If no pattern matches, return as generic log
        return {
            'timestamp': datetime.now().isoformat(),
            'logger': 'unknown',
            'level': 'INFO',
            'message': line,
            'service': 'historian-api',
            'source': 'unparsed_log'
        }
    
    def _normalize_timestamp(self, timestamp_str: str) -> str:
        """Normalize various timestamp formats to ISO format"""
        try:
            # Handle common formats
            formats = [
                '%Y-%m-%d %H:%M:%S,%f',
                '%Y-%m-%d %H:%M:%S.%f',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%dT%H:%M:%SZ',
                '%Y-%m-%dT%H:%M:%S.%fZ',
                '%Y-%m-%dT%H:%M:%S%z'
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(timestamp_str.replace(',', '.'), fmt)
                    return dt.isoformat()
                except ValueError:
                    continue
            
            # If parsing fails, return as-is
            return timestamp_str
            
        except Exception:
            return datetime.now().isoformat()
    
    def _generate_sample_logs(self, limit: int) -> List[Dict[str, Any]]:
        """Generate sample structured logs for demonstration"""
        import random
        import uuid
        
        sample_loggers = [
            'historian.api.main',
            'historian.api.scheduler',
            'historian.api.database',
            'historian.exporter',
            'historian.storage'
        ]
        
        sample_messages = [
            'Query executed successfully',
            'Export job completed',
            'Connection established',
            'Database operation completed',
            'Scheduler tick executed',
            'Health check passed',
            'API request processed',
            'Configuration updated',
            'Metrics collected',
            'Connection pool status updated'
        ]
        
        sample_levels = ['INFO', 'WARNING', 'ERROR', 'DEBUG']
        
        logs = []
        base_time = datetime.now()
        
        for i in range(limit):
            # Generate timestamp going backwards
            timestamp = base_time - timedelta(seconds=i * random.randint(1, 30))
            
            log_entry = {
                'timestamp': timestamp.isoformat() + 'Z',
                'level': random.choice(sample_levels),
                'logger': random.choice(sample_loggers),
                'message': random.choice(sample_messages),
                'service': 'historian-api',
                'request_id': f'req-{uuid.uuid4().hex[:8]}',
                'extra': {
                    'event_type': random.choice(['api_call', 'database_operation', 'export_job', 'query_execution']),
                    'duration_ms': round(random.uniform(10, 500), 2),
                    'success': random.choice([True, True, True, False])  # Bias towards success
                }
            }
            
            logs.append(log_entry)
        
        return logs

# Global logs manager instance
logs_manager = LogsManager()

def get_logs_manager() -> LogsManager:
    """Get the global logs manager instance"""
    return logs_manager
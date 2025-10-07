import asyncio
import aiohttp
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any, Callable
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
import logging
import json
import time
from jsonpath_ng import parse as jsonpath_parse
import base64

logger = logging.getLogger(__name__)


class HTTPJsonExporter:
    def __init__(self, connection_config: Dict):
        """Initialize HTTP JSON Exporter with connection configuration"""
        self.endpoint_url = connection_config['endpoint_url']
        self.auth_type = connection_config.get('auth_type', 'none')
        self.auth_config = connection_config.get('auth_config', {})
        self.headers = connection_config.get('headers', {})
        self.query_params = connection_config.get('query_params', {})
        self.method = connection_config.get('method', 'GET')

        # Data mapping configuration
        self.timestamp_field = connection_config['timestamp_field']
        self.timestamp_format = connection_config.get('timestamp_format')
        self.measurement_field = connection_config.get('measurement_field', 'measurement')
        self.data_root = connection_config.get('data_root', '$')
        self.tags_mapping = connection_config.get('tags_mapping', {})
        self.fields_mapping = connection_config.get('fields_mapping', {})

        # Pagination configuration
        self.pagination_type = connection_config.get('pagination_type', 'none')
        self.pagination_config = connection_config.get('pagination_config', {})

        # Request configuration
        self.timeout = connection_config.get('timeout', 30)
        self.max_retries = connection_config.get('max_retries', 3)
        self.rate_limit_ms = connection_config.get('rate_limit_ms', 0)

        # Compile JSONPath expressions
        self._compile_jsonpaths()

    def _compile_jsonpaths(self):
        """Pre-compile JSONPath expressions for better performance"""
        try:
            self.data_root_expr = jsonpath_parse(self.data_root)
            self.timestamp_expr = jsonpath_parse(self.timestamp_field)

            # Compile measurement field if it's a JSONPath
            if self.measurement_field and self.measurement_field.startswith('$'):
                self.measurement_expr = jsonpath_parse(self.measurement_field)
            else:
                self.measurement_expr = None

            # Compile tag mappings
            self.tags_expr = {}
            for tag_name, json_path in self.tags_mapping.items():
                self.tags_expr[tag_name] = jsonpath_parse(json_path)

            # Compile field mappings
            self.fields_expr = {}
            for field_name, json_path in self.fields_mapping.items():
                self.fields_expr[field_name] = jsonpath_parse(json_path)

        except Exception as e:
            logger.error(f"Failed to compile JSONPath expressions: {e}")
            raise

    def _get_auth_headers(self) -> Dict[str, str]:
        """Generate authentication headers based on auth type"""
        headers = self.headers.copy()

        if self.auth_type == 'bearer':
            token = self.auth_config.get('token')
            if token:
                headers['Authorization'] = f"Bearer {token}"

        elif self.auth_type == 'basic':
            username = self.auth_config.get('username')
            password = self.auth_config.get('password')
            if username and password:
                credentials = base64.b64encode(f"{username}:{password}".encode()).decode()
                headers['Authorization'] = f"Basic {credentials}"

        elif self.auth_type == 'api_key':
            key_header = self.auth_config.get('header_name', 'X-API-Key')
            api_key = self.auth_config.get('api_key')
            if api_key:
                headers[key_header] = api_key

        return headers

    def _build_url_with_params(self, start_time: datetime, end_time: datetime,
                              page_token: Optional[str] = None) -> tuple[str, dict]:
        """Build URL with query parameters, handling time-based variables"""
        params = self.query_params.copy()

        # Add API key as query parameter if configured
        if self.auth_type == 'api_key':
            param_name = self.auth_config.get('param_name')
            api_key = self.auth_config.get('api_key')
            if param_name and api_key:
                # Strip whitespace from API key
                api_key = str(api_key).strip()
                params[param_name] = api_key
                logger.debug(f"Adding API key as query param: {param_name}={api_key[:8]}...")

        # Replace time placeholders
        for key, value in params.items():
            if isinstance(value, str):
                # Replace time placeholders
                value = value.replace('{{start_time}}', start_time.isoformat())
                value = value.replace('{{end_time}}', end_time.isoformat())
                value = value.replace('{{start_unix}}', str(int(start_time.timestamp())))
                value = value.replace('{{end_unix}}', str(int(end_time.timestamp())))
                value = value.replace('{{start_unix_ms}}', str(int(start_time.timestamp() * 1000)))
                value = value.replace('{{end_unix_ms}}', str(int(end_time.timestamp() * 1000)))
                params[key] = value

        # Add pagination parameters
        if self.pagination_type == 'offset':
            if page_token:
                params[self.pagination_config.get('offset_param', 'offset')] = page_token
            params[self.pagination_config.get('limit_param', 'limit')] = \
                self.pagination_config.get('page_size', 100)

        elif self.pagination_type == 'cursor':
            if page_token:
                params[self.pagination_config.get('cursor_param', 'cursor')] = page_token
            params[self.pagination_config.get('limit_param', 'limit')] = \
                self.pagination_config.get('page_size', 100)

        elif self.pagination_type == 'page':
            page_num = int(page_token) if page_token else 1
            params[self.pagination_config.get('page_param', 'page')] = page_num
            params[self.pagination_config.get('limit_param', 'limit')] = \
                self.pagination_config.get('page_size', 100)

        return self.endpoint_url, params

    def _parse_timestamp(self, timestamp_value: Any) -> datetime:
        """Parse timestamp from various formats"""
        if timestamp_value is None:
            return None

        try:
            if self.timestamp_format == 'unix':
                return datetime.fromtimestamp(float(timestamp_value))
            elif self.timestamp_format == 'unix_ms':
                return datetime.fromtimestamp(float(timestamp_value) / 1000.0)
            elif self.timestamp_format:
                # Custom strftime format
                return datetime.strptime(str(timestamp_value), self.timestamp_format)
            else:
                # Try to parse ISO format or other common formats
                if isinstance(timestamp_value, (int, float)):
                    # Assume unix timestamp
                    if timestamp_value > 1e10:  # Likely milliseconds
                        return datetime.fromtimestamp(timestamp_value / 1000.0)
                    else:
                        return datetime.fromtimestamp(timestamp_value)
                else:
                    # Try ISO parse
                    return datetime.fromisoformat(str(timestamp_value).replace('Z', '+00:00'))
        except Exception as e:
            logger.warning(f"Failed to parse timestamp {timestamp_value}: {e}")
            return None

    def _extract_value(self, data: dict, expr: Any) -> Any:
        """Extract value from JSON using compiled JSONPath expression"""
        try:
            matches = expr.find(data)
            if matches:
                return matches[0].value
            return None
        except Exception as e:
            logger.debug(f"Failed to extract value: {e}")
            return None

    def _parse_response_to_records(self, response_data: dict) -> List[dict]:
        """Parse API response into records using JSONPath mappings"""
        records = []

        try:
            # Extract data array using data_root JSONPath
            data_matches = self.data_root_expr.find(response_data)
            if not data_matches:
                logger.warning(f"No data found at path {self.data_root}")
                return records

            data_items = data_matches[0].value
            if not isinstance(data_items, list):
                data_items = [data_items]  # Wrap single item in list

            for item in data_items:
                # Extract timestamp
                timestamp_value = self._extract_value(item, self.timestamp_expr)
                timestamp = self._parse_timestamp(timestamp_value)

                if not timestamp:
                    continue

                # Build record
                record = {'time': timestamp}

                # Extract measurement name
                if self.measurement_expr:
                    measurement = self._extract_value(item, self.measurement_expr)
                    record['measurement'] = measurement or 'http_json'
                else:
                    record['measurement'] = self.measurement_field or 'http_json'

                # Extract tags
                for tag_name, tag_expr in self.tags_expr.items():
                    tag_value = self._extract_value(item, tag_expr)
                    if tag_value is not None:
                        record[f"tag_{tag_name}"] = str(tag_value)

                # Extract fields
                for field_name, field_expr in self.fields_expr.items():
                    field_value = self._extract_value(item, field_expr)
                    if field_value is not None:
                        # Try to preserve numeric types
                        if isinstance(field_value, (int, float)):
                            record[f"field_{field_name}"] = field_value
                        else:
                            try:
                                record[f"field_{field_name}"] = float(field_value)
                            except (ValueError, TypeError):
                                record[f"field_{field_name}"] = str(field_value)

                records.append(record)

        except Exception as e:
            logger.error(f"Failed to parse response: {e}")

        return records

    def _get_next_page_token(self, response_data: dict, current_token: Optional[str]) -> Optional[str]:
        """Extract next page token based on pagination type"""
        if self.pagination_type == 'none':
            return None

        elif self.pagination_type == 'cursor':
            cursor_path = self.pagination_config.get('next_cursor_path')
            if cursor_path:
                expr = jsonpath_parse(cursor_path)
                matches = expr.find(response_data)
                if matches and matches[0].value:
                    return str(matches[0].value)
            return None

        elif self.pagination_type == 'offset':
            # Calculate next offset
            page_size = self.pagination_config.get('page_size', 100)
            current_offset = int(current_token) if current_token else 0

            # Check if there's more data
            total_path = self.pagination_config.get('total_count_path')
            if total_path:
                expr = jsonpath_parse(total_path)
                matches = expr.find(response_data)
                if matches:
                    total = int(matches[0].value)
                    next_offset = current_offset + page_size
                    if next_offset < total:
                        return str(next_offset)
            else:
                # If no total, check if we got a full page
                data_matches = self.data_root_expr.find(response_data)
                if data_matches and len(data_matches[0].value) == page_size:
                    return str(current_offset + page_size)
            return None

        elif self.pagination_type == 'page':
            current_page = int(current_token) if current_token else 1

            # Check if there's a next page
            has_more_path = self.pagination_config.get('has_more_path')
            if has_more_path:
                expr = jsonpath_parse(has_more_path)
                matches = expr.find(response_data)
                if matches and matches[0].value:
                    return str(current_page + 1)
            else:
                # Check if we got a full page
                page_size = self.pagination_config.get('page_size', 100)
                data_matches = self.data_root_expr.find(response_data)
                if data_matches and len(data_matches[0].value) == page_size:
                    return str(current_page + 1)
            return None

    async def fetch_data(self, session: aiohttp.ClientSession, start_time: datetime,
                        end_time: datetime, page_token: Optional[str] = None) -> tuple[List[dict], Optional[str]]:
        """Fetch data from HTTP endpoint for a time range"""
        records = []
        next_token = None

        url, params = self._build_url_with_params(start_time, end_time, page_token)
        headers = self._get_auth_headers()

        for retry in range(self.max_retries):
            try:
                # Apply rate limiting
                if self.rate_limit_ms > 0:
                    await asyncio.sleep(self.rate_limit_ms / 1000.0)

                # Make request
                async with session.request(
                    method=self.method,
                    url=url,
                    params=params if self.method == 'GET' else None,
                    json=params if self.method == 'POST' else None,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as response:
                    response.raise_for_status()
                    response_data = await response.json()

                    # Parse records
                    records = self._parse_response_to_records(response_data)

                    # Get next page token
                    next_token = self._get_next_page_token(response_data, page_token)

                    logger.info(f"Fetched {len(records)} records from {url}")
                    return records, next_token

            except aiohttp.ClientError as e:
                logger.warning(f"HTTP request failed (attempt {retry + 1}/{self.max_retries}): {e}")
                if retry < self.max_retries - 1:
                    await asyncio.sleep(2 ** retry)  # Exponential backoff
                else:
                    raise
            except Exception as e:
                logger.error(f"Unexpected error fetching data: {e}")
                raise

        return records, next_token

    async def export_to_parquet(
        self,
        measurement: str,
        start_date: datetime,
        end_date: datetime,
        output_path: str,
        cancellation_check: Optional[Callable] = None
    ) -> int:
        """Export data from HTTP JSON API to Parquet file"""

        all_records = []
        total_records = 0

        async with aiohttp.ClientSession() as session:
            # Handle pagination
            page_token = None
            page_count = 0
            max_pages = self.pagination_config.get('max_pages', 1000)  # Safety limit

            while page_count < max_pages:
                # Check for cancellation
                if cancellation_check and cancellation_check():
                    logger.info("Export cancelled by user")
                    return 0

                # Fetch page
                records, next_token = await self.fetch_data(
                    session, start_date, end_date, page_token
                )

                if records:
                    all_records.extend(records)
                    total_records += len(records)
                    logger.info(f"Page {page_count + 1}: Retrieved {len(records)} records")

                # Check if we should continue
                if not next_token or self.pagination_type == 'none':
                    break

                page_token = next_token
                page_count += 1

            if page_count >= max_pages:
                logger.warning(f"Reached maximum page limit ({max_pages})")

        # Convert to DataFrame and save as Parquet
        if all_records:
            try:
                df = pl.DataFrame(all_records)

                # Ensure time column is datetime
                if 'time' in df.columns:
                    df = df.with_columns(pl.col('time').cast(pl.Datetime))

                # Sort by time
                df = df.sort('time')

                # Write to Parquet
                output_file = Path(output_path) / f"{measurement}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.parquet"
                output_file.parent.mkdir(parents=True, exist_ok=True)

                df.write_parquet(str(output_file), compression='snappy')
                logger.info(f"Exported {total_records} records to {output_file}")

            except Exception as e:
                logger.error(f"Failed to write Parquet file: {e}")
                raise
        else:
            logger.warning(f"No data retrieved for {measurement} between {start_date} and {end_date}")

        return total_records

    async def test_connection(self) -> Dict[str, Any]:
        """Test the HTTP JSON connection and return sample data"""
        try:
            # Use a small time range for testing
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=1)

            async with aiohttp.ClientSession() as session:
                records, _ = await self.fetch_data(session, start_time, end_time)

                return {
                    'success': True,
                    'message': f"Successfully connected to {self.endpoint_url}",
                    'sample_records': records[:5] if records else [],
                    'total_records': len(records)
                }

        except Exception as e:
            return {
                'success': False,
                'message': f"Connection failed: {str(e)}",
                'error': str(e)
            }

    async def discover_schema(self) -> Dict[str, Any]:
        """Discover the schema of the API response"""
        try:
            # Fetch sample data
            end_time = datetime.utcnow()
            start_time = end_time - timedelta(hours=1)

            async with aiohttp.ClientSession() as session:
                url, params = self._build_url_with_params(start_time, end_time)
                headers = self._get_auth_headers()

                async with session.request(
                    method=self.method,
                    url=url,
                    params=params if self.method == 'GET' else None,
                    json=params if self.method == 'POST' else None,
                    headers=headers,
                    timeout=aiohttp.ClientTimeout(total=self.timeout)
                ) as response:
                    response.raise_for_status()
                    response_data = await response.json()

                    # Extract schema information
                    schema = {
                        'raw_response': response_data,
                        'discovered_paths': self._discover_paths(response_data),
                        'suggested_mappings': self._suggest_mappings(response_data)
                    }

                    return schema

        except Exception as e:
            logger.error(f"Failed to discover schema: {e}")
            raise

    def _discover_paths(self, data: Any, path: str = '$', paths: List[str] = None) -> List[str]:
        """Recursively discover all JSONPath expressions in data"""
        if paths is None:
            paths = []

        if isinstance(data, dict):
            for key, value in data.items():
                new_path = f"{path}.{key}"
                paths.append(new_path)
                self._discover_paths(value, new_path, paths)
        elif isinstance(data, list) and data:
            # Sample first item
            paths.append(f"{path}[*]")
            if isinstance(data[0], dict):
                for key in data[0].keys():
                    new_path = f"{path}[*].{key}"
                    paths.append(new_path)

        return paths

    def _suggest_mappings(self, data: Any) -> Dict[str, Any]:
        """Suggest field mappings based on common patterns"""
        suggestions = {
            'timestamp_candidates': [],
            'tag_candidates': [],
            'field_candidates': []
        }

        # Look for timestamp-like fields
        timestamp_patterns = ['time', 'timestamp', 'datetime', 'date', 'created', 'updated', 'ts']

        paths = self._discover_paths(data)
        for path in paths:
            path_lower = path.lower()
            # Check for timestamp patterns
            if any(pattern in path_lower for pattern in timestamp_patterns):
                suggestions['timestamp_candidates'].append(path)
            # String fields are good tag candidates
            elif not any(x in path_lower for x in ['value', 'count', 'amount', 'price', 'rate']):
                suggestions['tag_candidates'].append(path)
            # Numeric fields are good field candidates
            else:
                suggestions['field_candidates'].append(path)

        return suggestions
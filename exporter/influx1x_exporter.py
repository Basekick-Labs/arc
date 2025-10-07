import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from influxdb import InfluxDBClient
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class InfluxDB1xExporter:
    def __init__(self, host: str, port: int, username: str, password: str, database: str, ssl: bool = False):
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database
        self.ssl = ssl
        self.base_url = f"{'https' if ssl else 'http'}://{host}:{port}"
        
        # Keep client for backward compatibility
        try:
            self.client = InfluxDBClient(host=host, port=port, username=username, password=password, database=database)
        except Exception as e:
            logger.warning(f"Failed to initialize InfluxDB client: {e}")
            self.client = None
        
    async def export_measurement_batch(
        self, 
        measurement: str, 
        start_time: datetime, 
        end_time: datetime,
        batch_size: int = 10000
    ) -> pl.DataFrame:
        """Export measurement data in batches for memory efficiency"""
        
        # Ensure timestamps are in UTC for InfluxDB query
        start_utc = start_time.replace(tzinfo=None) if start_time.tzinfo else start_time
        end_utc = end_time.replace(tzinfo=None) if end_time.tzinfo else end_time
        
        query = f"""
        SELECT * FROM "{measurement}" 
        WHERE time >= '{start_utc.isoformat()}Z' 
        AND time < '{end_utc.isoformat()}Z'
        """
        
        logger.info(f"Executing query: {query}")
        
        # Use HTTP requests for reverse proxy compatibility
        import requests
        
        query_url = f"{self.base_url}/query"
        # Try full query first, fall back to chunking on JSON errors
        try:
            return await self._execute_single_query(measurement, start_utc, end_utc, query)
        except Exception as e:
            error_str = str(e)
            if any(x in error_str for x in ["Extra data", "Failed to parse JSON", "JSON decode", "Expecting"]):
                logger.warning(f"Large/malformed JSON response detected for {measurement}, switching to 1-hour chunks")
                return await self._export_in_time_chunks(measurement, start_utc, end_utc, timedelta(hours=1))
            else:
                logger.error(f"Non-JSON error for {measurement}: {error_str}")
                raise
        
        # Execute single query; if JSON too large/malformed, fallback to time-chunk loop
        return await self._execute_single_query(measurement, start_utc, end_utc, query)
    
    async def _execute_single_query(self, measurement: str, start_utc: datetime, end_utc: datetime, query: str) -> pl.DataFrame:
        """Execute a single query and return DataFrame"""
        import requests
        
        query_url = f"{self.base_url}/query"
        params = {
            'db': self.database,
            'q': query,
            'epoch': 'ns'
        }
        
        if self.username:
            params['u'] = self.username
        if self.password:
            params['p'] = self.password
        
        try:
            loop = asyncio.get_event_loop()
            response = await loop.run_in_executor(
                None, 
                lambda: requests.get(query_url, params=params, timeout=120, verify=True if self.ssl else False)
            )
            
            if response.status_code != 200:
                logger.error(f"Query failed: {response.status_code} {response.text[:500]}")
                return pl.DataFrame()
            
            # Handle large responses that might be malformed
            try:
                result = response.json()
            except Exception as json_error:
                logger.error(f"Failed to parse JSON response: {json_error}")
                logger.error(f"Response size: {len(response.text)} chars")
                logger.error(f"Response preview: {response.text[:1000]}")
                # Re-raise to trigger chunking fallback
                raise Exception(f"Failed to parse JSON response: {json_error}")
            
            if not result.get('results'):
                logger.warning(f"No results in response for {measurement}")
                return pl.DataFrame()
                
            if result['results'][0].get('error'):
                logger.error(f"InfluxDB query error: {result['results'][0]['error']}")
                return pl.DataFrame()
                
            if not result['results'][0].get('series'):
                logger.info(f"No data found for {measurement} in time range")
                return pl.DataFrame()
                
            # Convert to Polars DataFrame for high performance
            series = result['results'][0]['series'][0]
            columns = series['columns']
            values = series['values']
            
            if not values:
                return pl.DataFrame()
            
            # Create Polars DataFrame directly from raw data
            df = pl.DataFrame(values, schema=columns, orient="row")
            
            # Convert time column to proper datetime (UTC)
            if 'time' in columns:
                df = df.with_columns([
                    pl.col("time").cast(pl.Int64).cast(pl.Datetime("ns")).alias("timestamp")
                ]).drop("time")
            
            logger.info(f"Successfully exported {df.height} rows for {measurement}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to export {measurement}: {e}")
            raise  # Re-raise to allow chunking fallback
    

    
    async def export_to_parquet(
        self,
        measurement: str,
        start_date: datetime,
        end_date: datetime,
        output_path: Path,
        chunk_hours: int = 24,
        cancellation_check=None,
        database_name: str = None
    ) -> None:
        """Export measurement to partitioned Parquet files"""
        
        current_time = start_date
        total_rows = 0
        
        while current_time < end_date:
            chunk_end = min(current_time + timedelta(hours=chunk_hours), end_date)
            
            try:
                # Export batch with smaller time windows
                df = await self.export_measurement_batch(measurement, current_time, chunk_end)
                
                if df.height > 0:  # Polars uses .height instead of len()
                    # Create partitioned directory structure
                    partition_path = output_path / measurement / str(current_time.year) / f"{current_time.month:02d}" / f"{current_time.day:02d}"
                    partition_path.mkdir(parents=True, exist_ok=True)
                    
                    file_name = f"{current_time.hour:02d}.parquet"
                    file_path = partition_path / file_name
                    
                    # Write with maximum compression
                    df.write_parquet(
                        file_path,
                        compression="zstd",  # Better compression than snappy
                        compression_level=3,  # Good balance of speed/compression
                        use_pyarrow=True
                    )
                    
                    total_rows += df.height
                    logger.info(f"Exported {df.height} rows to {file_path}")
                else:
                    logger.debug(f"No data for {measurement} in {current_time} to {chunk_end}")
                    
            except Exception as e:
                logger.error(f"Failed to export chunk {current_time} to {chunk_end} for {measurement}: {e}")
                # Continue with next chunk instead of failing completely
            
            current_time = chunk_end
            
            # Check for cancellation
            if cancellation_check and cancellation_check():
                logger.info(f"Export cancelled for {measurement}")
                return
            
            # Add small delay to avoid overwhelming InfluxDB
            await asyncio.sleep(0.1)
        
        logger.info(f"Completed export of {measurement}: {total_rows} total rows")
    
    async def _export_in_time_chunks(self, measurement: str, start_utc: datetime, end_utc: datetime, chunk_size: timedelta = timedelta(hours=1)) -> pl.DataFrame:
        """Export data in smaller time chunks to avoid large JSON responses"""
        all_dataframes = []
        current_time = start_utc
        
        while current_time < end_utc:
            chunk_end = min(current_time + chunk_size, end_utc)
            
            query = f"""
            SELECT * FROM "{measurement}" 
            WHERE time >= '{current_time.isoformat()}Z' 
            AND time < '{chunk_end.isoformat()}Z'
            """
            
            logger.info(f"Chunked query: {current_time} to {chunk_end}")
            
            query_url = f"{self.base_url}/query"
            params = {
                'q': query,
                'u': self.username,
                'p': self.password,
                'db': self.database,
                'epoch': 'ns',
                'chunked': 'true',
                'chunk_size': '5000'
            }
            
            try:
                import requests
                loop = asyncio.get_event_loop()
                response = await loop.run_in_executor(
                    None, 
                    lambda: requests.get(query_url, params=params, timeout=30, verify=True if self.ssl else False)
                )
                
                if response.status_code == 200:
                    try:
                        result = response.json()
                        if (result.get('results') and 
                            result['results'][0].get('series') and 
                            not result['results'][0].get('error')):
                            
                            series = result['results'][0]['series'][0]
                            columns = series['columns']
                            values = series['values']
                            
                            if values:
                                df = pl.DataFrame(values, schema=columns, orient="row")
                                if 'time' in columns:
                                    df = df.with_columns([
                                        pl.col("time").cast(pl.Int64).cast(pl.Datetime("ns")).alias("timestamp")
                                    ]).drop("time")
                                all_dataframes.append(df)
                                logger.info(f"Chunk {current_time} to {chunk_end}: {df.height} rows")
                    except Exception as json_error:
                        logger.warning(f"JSON parse error for chunk {current_time}: {json_error}")
                        logger.warning(f"Response size: {len(response.text)} chars")
                        # Continue with next chunk instead of failing
                        
            except Exception as e:
                logger.error(f"Failed to export chunk {current_time} to {chunk_end}: {e}")
            
            current_time = chunk_end
            await asyncio.sleep(0.1)  # Small delay between chunks
        
        # Combine all dataframes with consistent schema
        if all_dataframes:
            # Ensure all dataframes have the same schema by casting to consistent types
            if len(all_dataframes) > 1:
                # Get the first dataframe's schema as reference
                reference_schema = all_dataframes[0].schema
                
                # Cast all other dataframes to match the reference schema
                aligned_dataframes = [all_dataframes[0]]
                for df in all_dataframes[1:]:
                    try:
                        # Cast columns to match reference schema
                        cast_exprs = []
                        for col_name, col_type in reference_schema.items():
                            if col_name in df.columns:
                                cast_exprs.append(pl.col(col_name).cast(col_type))
                        
                        if cast_exprs:
                            aligned_df = df.with_columns(cast_exprs)
                        else:
                            aligned_df = df
                        aligned_dataframes.append(aligned_df)
                    except Exception as cast_error:
                        logger.warning(f"Failed to align schema for chunk: {cast_error}")
                        aligned_dataframes.append(df)  # Use original if casting fails
                
                combined_df = pl.concat(aligned_dataframes)
            else:
                combined_df = all_dataframes[0]
            
            logger.info(f"Combined {len(all_dataframes)} chunks into {combined_df.height} total rows")
            return combined_df
        else:
            return pl.DataFrame()
    
    def close(self):
        """Close InfluxDB connection"""
        if self.client:
            self.client.close()

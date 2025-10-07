import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import polars as pl
from influxdb_client import InfluxDBClient
from influxdb_client.client.query_api import QueryApi
from pathlib import Path
import logging

logger = logging.getLogger(__name__)

class InfluxDB2xExporter:
    def __init__(self, url: str, token: str, org: str, bucket: str = None):
        self.url = url
        self.token = token
        self.org = org
        self.bucket = bucket
        self.client = InfluxDBClient(url=url, token=token, org=org)
        self.query_api = self.client.query_api()
        
    async def export_measurement_batch(
        self, 
        measurement: str, 
        start_time: datetime, 
        end_time: datetime,
        bucket: str = None
    ) -> pl.DataFrame:
        """Export measurement data using Flux query"""
        
        bucket_name = bucket or self.bucket
        if not bucket_name:
            raise ValueError("Bucket name is required")
        
        # Ensure timestamps are in UTC
        start_utc = start_time.replace(tzinfo=None) if start_time.tzinfo else start_time
        end_utc = end_time.replace(tzinfo=None) if end_time.tzinfo else end_time
        
        flux_query = f'''
        from(bucket: "{bucket_name}")
          |> range(start: {start_utc.isoformat()}Z, stop: {end_utc.isoformat()}Z)
          |> filter(fn: (r) => r._measurement == "{measurement}")
          |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
        '''
        
        logger.info(f"Executing Flux query: {flux_query}")
        
        try:
            # Execute Flux query - async
            loop = asyncio.get_event_loop()
            tables = await loop.run_in_executor(None, self.query_api.query, flux_query)
            
            if not tables:
                logger.info(f"No data found for {measurement} in time range")
                return pl.DataFrame()
            
            # Convert FluxTable to Polars DataFrame
            records = []
            for table in tables:
                for record in table.records:
                    row_data = {
                        'timestamp': record.get_time(),
                        **{k: v for k, v in record.values.items() 
                           if not k.startswith('_') or k in ['_measurement']}
                    }
                    records.append(row_data)
            
            if not records:
                return pl.DataFrame()
                
            # Create Polars DataFrame
            df = pl.DataFrame(records)
            
            # Ensure timestamp column exists and is properly typed
            if 'timestamp' in df.columns:
                df = df.with_columns([
                    pl.col('timestamp').cast(pl.Datetime("ns"))
                ])
            
            logger.info(f"Successfully exported {df.height} rows for {measurement}")
            return df
            
        except Exception as e:
            logger.error(f"Failed to export {measurement}: {e}")
            raise
    
    async def export_to_parquet(
        self,
        measurement: str,
        start_date: datetime,
        end_date: datetime,
        output_path: Path,
        chunk_hours: int = 24,
        cancellation_check=None,
        database_name: str = None,
        bucket: str = None
    ) -> None:
        """Export measurement to partitioned Parquet files"""
        
        current_time = start_date
        total_rows = 0
        
        while current_time < end_date:
            chunk_end = min(current_time + timedelta(hours=chunk_hours), end_date)
            
            try:
                # Export batch
                df = await self.export_measurement_batch(measurement, current_time, chunk_end, bucket)
                
                if df.height > 0:
                    # Create partitioned directory structure (same as 1.x)
                    partition_path = output_path / measurement / str(current_time.year) / f"{current_time.month:02d}" / f"{current_time.day:02d}"
                    partition_path.mkdir(parents=True, exist_ok=True)
                    
                    file_name = f"{current_time.hour:02d}.parquet"
                    file_path = partition_path / file_name
                    
                    # Write with ZSTD compression (same as 1.x)
                    df.write_parquet(
                        file_path,
                        compression="zstd",
                        compression_level=3,
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
    
    def close(self):
        """Close InfluxDB connection"""
        self.client.close()
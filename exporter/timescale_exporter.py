import asyncio
import asyncpg
import polars as pl
import logging
from typing import Dict, List, Optional, Any, AsyncGenerator
from datetime import datetime, timezone
import pyarrow as pa

logger = logging.getLogger(__name__)

class TimescaleExporter:
    """Export data from TimescaleDB to Parquet files"""
    
    def __init__(self, host: str, port: int = 5432, database: str = "postgres", 
                 username: str = "postgres", password: str = "password", ssl: bool = False):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.ssl = ssl
        self.pool: Optional[asyncpg.Pool] = None
        
    async def connect(self):
        """Initialize connection pool"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.username,
                password=self.password,
                ssl=self.ssl,
                min_size=2,
                max_size=10,
                command_timeout=60
            )
            logger.info(f"Connected to TimescaleDB: {self.host}:{self.port}/{self.database}")
        except Exception as e:
            logger.error(f"Failed to connect to TimescaleDB: {e}")
            raise
    
    async def test_connection(self) -> Dict[str, Any]:
        """Test TimescaleDB connection"""
        try:
            if not self.pool:
                await self.connect()
            
            async with self.pool.acquire() as conn:
                # Test basic query
                result = await conn.fetchval("SELECT version();")
                
                # Check TimescaleDB extension
                ts_version = await conn.fetchval(
                    "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb';"
                )
                
                # Get hypertable count
                hypertable_count = await conn.fetchval("""
                    SELECT COUNT(*) FROM timescaledb_information.hypertables;
                """)
                
                return {
                    "success": True,
                    "message": "Connected to TimescaleDB successfully",
                    "postgres_version": result,
                    "timescaledb_version": ts_version,
                    "hypertable_count": hypertable_count
                }
                
        except Exception as e:
            return {
                "success": False,
                "message": f"TimescaleDB connection failed: {str(e)}"
            }
    
    async def get_tables(self) -> List[str]:
        """Get list of hypertables"""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT hypertable_name 
                    FROM timescaledb_information.hypertables 
                    ORDER BY hypertable_name;
                """)
                return [row['hypertable_name'] for row in rows]
        except Exception as e:
            logger.error(f"Failed to get hypertables: {e}")
            return []
    
    async def export_table(self, table_name: str, start_time: Optional[datetime] = None, 
                          end_time: Optional[datetime] = None, 
                          batch_size: int = 50000) -> AsyncGenerator[pl.DataFrame, None]:
        """Export table data in batches as Polars DataFrames"""
        try:
            async with self.pool.acquire() as conn:
                # Build query with time filters
                where_clause = ""
                params = []
                
                if start_time or end_time:
                    conditions = []
                    if start_time:
                        conditions.append(f"time >= ${len(params) + 1}")
                        params.append(start_time)
                    if end_time:
                        conditions.append(f"time <= ${len(params) + 1}")
                        params.append(end_time)
                    where_clause = f"WHERE {' AND '.join(conditions)}"
                
                # Get total count for progress tracking
                count_query = f"SELECT COUNT(*) FROM {table_name} {where_clause}"
                total_rows = await conn.fetchval(count_query, *params)
                logger.info(f"Exporting {total_rows} rows from {table_name}")
                
                # Export in batches
                offset = 0
                while offset < total_rows:
                    query = f"""
                        SELECT * FROM {table_name} 
                        {where_clause}
                        ORDER BY time 
                        LIMIT {batch_size} OFFSET {offset}
                    """
                    
                    rows = await conn.fetch(query, *params)
                    if not rows:
                        break
                    
                    # Convert to Polars DataFrame
                    data = {}
                    for col in rows[0].keys():
                        values = []
                        for row in rows:
                            value = row[col]
                            # Convert datetime to timestamp
                            if isinstance(value, datetime):
                                if value.tzinfo is None:
                                    value = value.replace(tzinfo=timezone.utc)
                                value = value.isoformat()
                            values.append(value)
                        data[col] = values
                    
                    df = pl.DataFrame(data)
                    
                    # Rename 'time' column to 'timestamp' for consistency
                    if 'time' in df.columns:
                        df = df.rename({'time': 'timestamp'})
                    
                    yield df
                    
                    offset += batch_size
                    logger.debug(f"Exported batch: {offset}/{total_rows} rows")
                    
        except Exception as e:
            logger.error(f"Failed to export table {table_name}: {e}")
            raise
    
    async def get_table_schema(self, table_name: str) -> Dict[str, str]:
        """Get table schema information"""
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT column_name, data_type 
                    FROM information_schema.columns 
                    WHERE table_name = $1 
                    ORDER BY ordinal_position;
                """, table_name)
                
                schema = {}
                for row in rows:
                    col_name = row['column_name']
                    pg_type = row['data_type']
                    
                    # Map PostgreSQL types to standard types
                    if pg_type in ['timestamp', 'timestamptz', 'timestamp with time zone']:
                        schema[col_name] = 'timestamp'
                    elif pg_type in ['double precision', 'real', 'numeric']:
                        schema[col_name] = 'float'
                    elif pg_type in ['integer', 'bigint', 'smallint']:
                        schema[col_name] = 'int'
                    elif pg_type == 'boolean':
                        schema[col_name] = 'bool'
                    else:
                        schema[col_name] = 'string'
                
                return schema
                
        except Exception as e:
            logger.error(f"Failed to get schema for {table_name}: {e}")
            return {}
    
    async def close(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
            logger.info("TimescaleDB connection pool closed")
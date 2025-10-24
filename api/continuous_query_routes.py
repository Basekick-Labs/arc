"""
Continuous Query API Routes

Provides endpoints for managing and manually triggering continuous queries.
Continuous queries allow automatic downsampling and aggregation of time-series data.
Automatic execution is reserved for enterprise edition.
"""

import logging
import time
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, List, Dict, Any
from fastapi import APIRouter, HTTPException, Request, Header, Depends
from pydantic import BaseModel, Field

from config_loader import get_config
from api.config import get_db_path
from api.auth import AuthManager

logger = logging.getLogger(__name__)
router = APIRouter()

# Import query_engine from main (will be initialized at startup)
_query_engine = None
_auth_manager = None

def get_query_engine():
    """Get the global query engine instance"""
    # Import the module, then access the attribute to get current value
    import api.main as main
    return main.query_engine

def get_auth_manager():
    """Get the global auth manager instance"""
    global _auth_manager
    if _auth_manager is None:
        # Import here to avoid circular imports
        from api.main import auth_manager
        _auth_manager = auth_manager
    return _auth_manager

# Initialize config
config = get_config()


class ContinuousQueryRequest(BaseModel):
    """Request model for creating/updating continuous queries"""
    name: str = Field(..., description="Unique name for the continuous query")
    description: Optional[str] = Field(None, description="Description of what this query does")
    database: str = Field(..., description="Target database name")
    source_measurement: str = Field(..., description="Source measurement to aggregate from")
    destination_measurement: str = Field(..., description="Destination measurement for aggregated data")
    query: str = Field(..., description="DuckDB SQL aggregation query with {start_time} and {end_time} placeholders")
    interval: str = Field(..., description="Aggregation interval (e.g., '1h', '1d')")
    retention_days: Optional[int] = Field(None, gt=0, description="Retention period for aggregated data (days)")
    delete_source_after_days: Optional[int] = Field(None, gt=0, description="Delete source data after N days (optional)")
    is_active: bool = Field(default=True, description="Whether query is active")


class ContinuousQueryResponse(BaseModel):
    """Response model for continuous queries"""
    id: int
    name: str
    description: Optional[str]
    database: str
    source_measurement: str
    destination_measurement: str
    query: str
    interval: str
    retention_days: Optional[int]
    delete_source_after_days: Optional[int]
    is_active: bool
    last_execution_time: Optional[str]
    last_execution_status: Optional[str]
    last_processed_time: Optional[str]
    last_records_written: Optional[int]
    created_at: str
    updated_at: str


class ExecuteContinuousQueryRequest(BaseModel):
    """Request model for executing a continuous query"""
    start_time: Optional[str] = Field(None, description="Start time (ISO format). Defaults to last processed time.")
    end_time: Optional[str] = Field(None, description="End time (ISO format). Defaults to current time.")
    dry_run: bool = Field(default=False, description="If true, show query without executing")


class ExecuteContinuousQueryResponse(BaseModel):
    """Response model for continuous query execution"""
    query_id: int
    query_name: str
    execution_id: str
    status: str
    start_time: str
    end_time: str
    records_read: Optional[int]
    records_written: int
    execution_time_seconds: float
    destination_measurement: str
    dry_run: bool
    executed_at: str


class ContinuousQueryManager:
    """Manager for continuous query database operations"""

    def __init__(self, db_path: str = None):
        self.db_path = db_path or get_db_path()
        self._init_tables()

    def _init_tables(self):
        """Initialize continuous query tables"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Continuous queries table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS continuous_queries (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT UNIQUE NOT NULL,
                    description TEXT,
                    database TEXT NOT NULL,
                    source_measurement TEXT NOT NULL,
                    destination_measurement TEXT NOT NULL,
                    query TEXT NOT NULL,
                    interval TEXT NOT NULL,
                    retention_days INTEGER,
                    delete_source_after_days INTEGER,
                    is_active BOOLEAN DEFAULT TRUE,
                    last_processed_time TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''')

            # Continuous query execution history
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS continuous_query_executions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    query_id INTEGER NOT NULL,
                    execution_id TEXT NOT NULL,
                    execution_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    status TEXT NOT NULL,
                    start_time TIMESTAMP NOT NULL,
                    end_time TIMESTAMP NOT NULL,
                    records_read INTEGER,
                    records_written INTEGER DEFAULT 0,
                    execution_duration_seconds FLOAT,
                    error_message TEXT,
                    FOREIGN KEY (query_id) REFERENCES continuous_queries (id)
                )
            ''')

            conn.commit()
            conn.close()
            logger.info("Continuous query tables initialized")

        except Exception as e:
            logger.error(f"Failed to initialize continuous query tables: {e}")
            raise

    def create_query(self, cq: ContinuousQueryRequest) -> int:
        """Create new continuous query"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO continuous_queries
                (name, description, database, source_measurement, destination_measurement,
                 query, interval, retention_days, delete_source_after_days, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                cq.name,
                cq.description,
                cq.database,
                cq.source_measurement,
                cq.destination_measurement,
                cq.query,
                cq.interval,
                cq.retention_days,
                cq.delete_source_after_days,
                cq.is_active
            ))

            query_id = cursor.lastrowid
            conn.commit()
            conn.close()

            logger.info(f"Created continuous query: {cq.name} (ID: {query_id})")
            return query_id

        except sqlite3.IntegrityError as e:
            if "UNIQUE constraint" in str(e):
                raise ValueError(f"Continuous query with name '{cq.name}' already exists")
            raise
        except Exception as e:
            logger.error(f"Failed to create continuous query: {e}")
            raise

    def get_queries(self, database: Optional[str] = None, is_active: Optional[bool] = None) -> List[Dict]:
        """Get continuous queries with optional filters"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            # Build query with filters
            where_clauses = []
            params = []

            if database:
                where_clauses.append("cq.database = ?")
                params.append(database)

            if is_active is not None:
                where_clauses.append("cq.is_active = ?")
                params.append(is_active)

            where_sql = ""
            if where_clauses:
                where_sql = "WHERE " + " AND ".join(where_clauses)

            query_sql = f'''
                SELECT
                    cq.*,
                    cqe.execution_time as last_execution_time,
                    cqe.status as last_execution_status,
                    cqe.records_written as last_records_written
                FROM continuous_queries cq
                LEFT JOIN (
                    SELECT query_id, execution_time, status, records_written
                    FROM continuous_query_executions
                    WHERE id IN (
                        SELECT MAX(id)
                        FROM continuous_query_executions
                        GROUP BY query_id
                    )
                ) cqe ON cq.id = cqe.query_id
                {where_sql}
                ORDER BY cq.created_at DESC
            '''

            cursor.execute(query_sql, params)

            queries = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return queries

        except Exception as e:
            logger.error(f"Failed to get continuous queries: {e}")
            return []

    def get_query(self, query_id: int) -> Optional[Dict]:
        """Get single continuous query by ID"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
                SELECT
                    cq.*,
                    cqe.execution_time as last_execution_time,
                    cqe.status as last_execution_status,
                    cqe.records_written as last_records_written
                FROM continuous_queries cq
                LEFT JOIN (
                    SELECT query_id, execution_time, status, records_written
                    FROM continuous_query_executions
                    WHERE id IN (
                        SELECT MAX(id)
                        FROM continuous_query_executions
                        GROUP BY query_id
                    )
                ) cqe ON cq.id = cqe.query_id
                WHERE cq.id = ?
            ''', (query_id,))

            row = cursor.fetchone()
            conn.close()

            if row:
                return dict(row)
            return None

        except Exception as e:
            logger.error(f"Failed to get continuous query {query_id}: {e}")
            return None

    def update_query(self, query_id: int, cq: ContinuousQueryRequest) -> bool:
        """Update existing continuous query"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
                UPDATE continuous_queries SET
                name = ?, description = ?, database = ?, source_measurement = ?,
                destination_measurement = ?, query = ?, interval = ?, retention_days = ?,
                delete_source_after_days = ?, is_active = ?, updated_at = CURRENT_TIMESTAMP
                WHERE id = ?
            ''', (
                cq.name,
                cq.description,
                cq.database,
                cq.source_measurement,
                cq.destination_measurement,
                cq.query,
                cq.interval,
                cq.retention_days,
                cq.delete_source_after_days,
                cq.is_active,
                query_id
            ))

            rows_affected = cursor.rowcount
            conn.commit()
            conn.close()

            if rows_affected == 0:
                return False

            logger.info(f"Updated continuous query {query_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to update continuous query: {e}")
            return False

    def delete_query(self, query_id: int) -> bool:
        """Delete continuous query"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            # Delete execution history first
            cursor.execute('DELETE FROM continuous_query_executions WHERE query_id = ?', (query_id,))

            # Delete query
            cursor.execute('DELETE FROM continuous_queries WHERE id = ?', (query_id,))

            rows_affected = cursor.rowcount
            conn.commit()
            conn.close()

            if rows_affected == 0:
                return False

            logger.info(f"Deleted continuous query {query_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to delete continuous query: {e}")
            return False

    def get_executions(self, query_id: int, limit: int = 50) -> List[Dict]:
        """Get execution history for a query"""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()

            cursor.execute('''
                SELECT * FROM continuous_query_executions
                WHERE query_id = ?
                ORDER BY execution_time DESC
                LIMIT ?
            ''', (query_id, limit))

            executions = [dict(row) for row in cursor.fetchall()]
            conn.close()
            return executions

        except Exception as e:
            logger.error(f"Failed to get executions for query {query_id}: {e}")
            return []

    def record_execution(
        self,
        query_id: int,
        execution_id: str,
        status: str,
        start_time: datetime,
        end_time: datetime,
        records_read: Optional[int],
        records_written: int,
        execution_duration: float,
        error_message: Optional[str] = None
    ):
        """Record continuous query execution"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
                INSERT INTO continuous_query_executions
                (query_id, execution_id, status, start_time, end_time, records_read,
                 records_written, execution_duration_seconds, error_message)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                query_id,
                execution_id,
                status,
                start_time.isoformat(),
                end_time.isoformat(),
                records_read,
                records_written,
                execution_duration,
                error_message
            ))

            conn.commit()
            conn.close()

        except Exception as e:
            logger.error(f"Failed to record execution: {e}")

    def update_last_processed_time(self, query_id: int, processed_time: datetime):
        """Update last processed time for a continuous query"""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
                UPDATE continuous_queries
                SET last_processed_time = ?
                WHERE id = ?
            ''', (processed_time.isoformat(), query_id))

            conn.commit()
            conn.close()

        except Exception as e:
            logger.error(f"Failed to update last processed time: {e}")


# Global manager instance
cq_manager = ContinuousQueryManager()


@router.post("/api/v1/continuous_queries", response_model=ContinuousQueryResponse)
async def create_continuous_query(request: ContinuousQueryRequest):
    """
    Create a new continuous query.

    Continuous queries automatically aggregate time-series data into materialized views.
    Manual execution only - automatic scheduling is reserved for enterprise edition.

    Example:
        POST /api/v1/continuous_queries
        {
            "name": "cpu_hourly_avg",
            "description": "Hourly average CPU metrics",
            "database": "default",
            "source_measurement": "cpu",
            "destination_measurement": "cpu_1h",
            "query": "SELECT time_bucket(INTERVAL '1 hour', time) as time, host, AVG(usage_user) as usage_user_avg FROM cpu WHERE time >= {start_time} AND time < {end_time} GROUP BY 1, 2",
            "interval": "1h",
            "retention_days": 365,
            "is_active": true
        }
    """
    try:
        query_id = cq_manager.create_query(request)
        query = cq_manager.get_query(query_id)

        if not query:
            raise HTTPException(status_code=500, detail="Failed to retrieve created query")

        return ContinuousQueryResponse(**query)

    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to create continuous query: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/api/v1/continuous_queries", response_model=List[ContinuousQueryResponse])
async def list_continuous_queries(
    database: Optional[str] = None,
    is_active: Optional[bool] = None
):
    """
    List all continuous queries with optional filters.

    Query Parameters:
        database: Filter by database name
        is_active: Filter by active status (true/false)
    """
    queries = cq_manager.get_queries(database=database, is_active=is_active)
    return [ContinuousQueryResponse(**query) for query in queries]


@router.get("/api/v1/continuous_queries/{query_id}", response_model=ContinuousQueryResponse)
async def get_continuous_query(query_id: int):
    """
    Get a specific continuous query by ID.
    """
    query = cq_manager.get_query(query_id)

    if not query:
        raise HTTPException(status_code=404, detail="Continuous query not found")

    return ContinuousQueryResponse(**query)


@router.put("/api/v1/continuous_queries/{query_id}", response_model=ContinuousQueryResponse)
async def update_continuous_query(
    query_id: int,
    request: ContinuousQueryRequest
):
    """
    Update an existing continuous query.
    """
    success = cq_manager.update_query(query_id, request)

    if not success:
        raise HTTPException(status_code=404, detail="Continuous query not found")

    query = cq_manager.get_query(query_id)
    return ContinuousQueryResponse(**query)


@router.delete("/api/v1/continuous_queries/{query_id}")
async def delete_continuous_query(
    query_id: int,
    delete_destination: bool = False
):
    """
    Delete a continuous query.

    Query Parameters:
        delete_destination: If true, also delete destination measurement data
    """
    if delete_destination:
        # TODO: Implement destination data deletion
        logger.warning("Destination data deletion not yet implemented")

    success = cq_manager.delete_query(query_id)

    if not success:
        raise HTTPException(status_code=404, detail="Continuous query not found")

    return {"message": "Continuous query deleted successfully"}


@router.post("/api/v1/continuous_queries/{query_id}/execute", response_model=ExecuteContinuousQueryResponse)
async def execute_continuous_query(
    query_id: int,
    request: ExecuteContinuousQueryRequest
):
    """
    Manually execute a continuous query.

    Runs the aggregation query and writes results to the destination measurement.

    Example:
        POST /api/v1/continuous_queries/1/execute
        {
            "start_time": "2025-10-20T00:00:00Z",
            "end_time": "2025-10-24T00:00:00Z",
            "dry_run": false
        }
    """
    import uuid

    start_exec_time = time.perf_counter()

    # Get query definition
    query = cq_manager.get_query(query_id)

    if not query:
        raise HTTPException(status_code=404, detail="Continuous query not found")

    if not query['is_active']:
        raise HTTPException(status_code=400, detail="Continuous query is not active")

    try:
        # Determine time range
        if request.start_time:
            start_time = datetime.fromisoformat(request.start_time.replace('Z', '+00:00'))
        elif query['last_processed_time']:
            start_time = datetime.fromisoformat(query['last_processed_time'])
        else:
            # Default: start from 1 hour ago
            start_time = datetime.utcnow() - timedelta(hours=1)

        if request.end_time:
            end_time = datetime.fromisoformat(request.end_time.replace('Z', '+00:00'))
        else:
            end_time = datetime.utcnow()

        # Validate time range
        if start_time >= end_time:
            raise HTTPException(
                status_code=400,
                detail="start_time must be before end_time"
            )

        # Replace placeholders in query
        sql_query = query['query'].replace(
            '{start_time}', f"'{start_time.isoformat()}'"
        ).replace(
            '{end_time}', f"'{end_time.isoformat()}'"
        )

        execution_id = f"cq-exec-{uuid.uuid4().hex[:8]}"

        logger.info(
            f"Executing continuous query '{query['name']}' "
            f"(ID: {query_id}, execution: {execution_id}) "
            f"from {start_time} to {end_time}"
        )

        if request.dry_run:
            # Dry run - just return the query
            execution_duration = (time.perf_counter() - start_exec_time)

            return ExecuteContinuousQueryResponse(
                query_id=query_id,
                query_name=query['name'],
                execution_id=execution_id,
                status="dry_run",
                start_time=start_time.isoformat(),
                end_time=end_time.isoformat(),
                records_read=None,
                records_written=0,
                execution_time_seconds=execution_duration,
                destination_measurement=query['destination_measurement'],
                dry_run=True,
                executed_at=datetime.utcnow().isoformat()
            )

        # Execute query
        engine = get_query_engine()
        if not engine:
            raise HTTPException(status_code=503, detail="Query engine not initialized")

        # Execute aggregation query
        result = await engine.execute_query(sql_query)

        if not result.get('success'):
            error_msg = result.get('error', 'Query execution failed')
            raise Exception(error_msg)

        # Get results
        data = result.get('data', [])
        columns = result.get('columns', [])
        records_written = len(data)

        logger.info(f"Aggregation query returned {records_written} records")

        if records_written > 0:
            # Write aggregated results to destination measurement
            # Get arrow buffer from msgpack_routes
            import api.msgpack_routes as msgpack_routes
            arrow_buffer = msgpack_routes.arrow_buffer

            if not arrow_buffer:
                logger.warning("Arrow buffer not initialized, skipping write")
            else:
                # Convert DuckDB results to records for writing
                records = []
                for row in data:
                    record = dict(zip(columns, row))

                    # Ensure time is in milliseconds (arrow_writer expects ms, not microseconds)
                    # OPTIMIZATION: When continuous queries use epoch_us() in SQL, time arrives as integer microseconds
                    time_milliseconds = None
                    if 'time' in record:
                        time_val = record['time']
                        if isinstance(time_val, (int, float)):
                            # Integer time - determine format by magnitude
                            if time_val < 1_000_000_000_000:  # Less than 1T = seconds or milliseconds
                                if time_val < 10_000_000_000:  # Less than 10B = seconds (e.g., 1729780800)
                                    time_milliseconds = int(time_val * 1_000)
                                else:  # Between 10B and 1T = milliseconds
                                    time_milliseconds = int(time_val)
                            else:
                                # Greater than 1T = microseconds (e.g., 1729780800000000)
                                time_milliseconds = int(time_val / 1_000)
                        elif isinstance(time_val, datetime):
                            time_milliseconds = int(time_val.timestamp() * 1_000)
                        elif isinstance(time_val, str):
                            # LEGACY: Parse timestamp string from DuckDB (less efficient)
                            # Recommend using epoch_us() in queries instead
                            from dateutil import parser
                            dt = parser.parse(time_val)
                            time_milliseconds = int(dt.timestamp() * 1_000)

                        record['time'] = time_milliseconds

                    # Create flat record (arrow_writer expects flat dictionaries, not nested tags/fields)
                    final_time = record.get('time', int(datetime.utcnow().timestamp() * 1_000))

                    flat_record = {
                        'measurement': query['destination_measurement'],
                        '_database': query['database'],  # Use _database for arrow_writer override
                        'time': final_time
                    }

                    # Add all fields from the aggregation result
                    for key, value in record.items():
                        if key != 'time':  # time is already added
                            flat_record[key] = value

                    records.append(flat_record)

                # Write to destination measurement using arrow buffer
                logger.info(f"Writing {len(records)} records to arrow buffer for {query['destination_measurement']}")
                if records:
                    sample = records[0]
                    sample_keys = [k for k in sample.keys() if k not in ['measurement', 'database', 'time']]
                    logger.info(f"Sample record time: {sample['time']} (fields: {sample_keys})")

                try:
                    await arrow_buffer.write(records)
                    logger.info(f"Successfully wrote {records_written} aggregated records to {query['destination_measurement']}")
                except Exception as e:
                    logger.error(f"Failed to write records to arrow buffer: {e}")
                    raise

        execution_duration = (time.perf_counter() - start_exec_time)

        # Record execution
        cq_manager.record_execution(
            query_id=query_id,
            execution_id=execution_id,
            status='completed',
            start_time=start_time,
            end_time=end_time,
            records_read=None,  # DuckDB doesn't provide this easily
            records_written=records_written,
            execution_duration=execution_duration
        )

        # Update last processed time
        cq_manager.update_last_processed_time(query_id, end_time)

        logger.info(
            f"Continuous query '{query['name']}' completed: "
            f"{records_written} records written in {execution_duration:.2f}s"
        )

        return ExecuteContinuousQueryResponse(
            query_id=query_id,
            query_name=query['name'],
            execution_id=execution_id,
            status='completed',
            start_time=start_time.isoformat(),
            end_time=end_time.isoformat(),
            records_read=None,
            records_written=records_written,
            execution_time_seconds=execution_duration,
            destination_measurement=query['destination_measurement'],
            dry_run=False,
            executed_at=datetime.utcnow().isoformat()
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Continuous query execution failed: {e}")

        execution_duration = (time.perf_counter() - start_exec_time)
        execution_id = f"cq-exec-{uuid.uuid4().hex[:8]}"

        cq_manager.record_execution(
            query_id=query_id,
            execution_id=execution_id,
            status='failed',
            start_time=start_time if 'start_time' in locals() else datetime.utcnow(),
            end_time=end_time if 'end_time' in locals() else datetime.utcnow(),
            records_read=None,
            records_written=0,
            execution_duration=execution_duration,
            error_message=str(e)
        )

        raise HTTPException(status_code=500, detail=f"Execution failed: {str(e)}")


@router.get("/api/v1/continuous_queries/{query_id}/executions")
async def get_continuous_query_executions(query_id: int, limit: int = 50):
    """
    Get execution history for a continuous query.

    Query Parameters:
        limit: Maximum number of executions to return (default: 50)
    """
    executions = cq_manager.get_executions(query_id, limit)

    return {
        "query_id": query_id,
        "executions": executions
    }

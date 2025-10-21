from fastapi import APIRouter, HTTPException, Request, Body
from typing import Dict, List, Optional
import logging
from api.database import ConnectionManager
from exporter.http_json_exporter import HTTPJsonExporter
from datetime import datetime, timedelta
import asyncio

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/http-json", tags=["http-json"])


@router.post("/connections")
async def create_http_json_connection(
    connection_data: Dict = Body(...)
):
    """Create a new HTTP JSON connection"""
    try:
        conn_mgr = ConnectionManager()

        # Validate required fields
        required = ['name', 'endpoint_url', 'timestamp_field']
        for field in required:
            if field not in connection_data:
                raise HTTPException(status_code=400, detail=f"Missing required field: {field}")

        # Test connection before saving
        if connection_data.get('test_connection', False):
            try:
                exporter = HTTPJsonExporter(connection_data)
                test_result = await exporter.test_connection()
                if not test_result['success']:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Connection test failed: {test_result['message']}"
                    )
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Connection test failed: {str(e)}")

        # Save connection
        connection_id = conn_mgr.add_http_json_connection(connection_data)

        return {
            "status": "success",
            "connection_id": connection_id,
            "message": f"HTTP JSON connection '{connection_data['name']}' created successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create HTTP JSON connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/connections")
async def list_http_json_connections(
):
    """List all HTTP JSON connections"""
    try:
        conn_mgr = ConnectionManager()
        connections = conn_mgr.get_http_json_connections()

        # Remove sensitive data
        for conn in connections:
            if 'auth_config' in conn and isinstance(conn['auth_config'], dict):
                # Mask sensitive fields
                if 'token' in conn['auth_config']:
                    conn['auth_config']['token'] = '***'
                if 'password' in conn['auth_config']:
                    conn['auth_config']['password'] = '***'
                if 'api_key' in conn['auth_config']:
                    conn['auth_config']['api_key'] = '***'

        return {"status": "success", "connections": connections}

    except Exception as e:
        logger.error(f"Failed to list HTTP JSON connections: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/connections/{connection_id}")
async def get_http_json_connection(
    connection_id: int
):
    """Get HTTP JSON connection details"""
    try:
        conn_mgr = ConnectionManager()
        connections = conn_mgr.get_http_json_connections()

        for conn in connections:
            if conn['id'] == connection_id:
                # Mask sensitive data
                if 'auth_config' in conn and isinstance(conn['auth_config'], dict):
                    if 'token' in conn['auth_config']:
                        conn['auth_config']['token'] = '***'
                    if 'password' in conn['auth_config']:
                        conn['auth_config']['password'] = '***'
                    if 'api_key' in conn['auth_config']:
                        conn['auth_config']['api_key'] = '***'
                return {"status": "success", "connection": conn}

        raise HTTPException(status_code=404, detail="Connection not found")

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get HTTP JSON connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.put("/connections/{connection_id}")
async def update_http_json_connection(
    connection_id: int,
    connection_data: Dict = Body(...)
):
    """Update HTTP JSON connection"""
    try:
        conn_mgr = ConnectionManager()

        # Test connection if requested
        if connection_data.get('test_connection', False):
            try:
                # Get existing connection to merge with updates
                connections = conn_mgr.get_http_json_connections()
                existing = None
                for conn in connections:
                    if conn['id'] == connection_id:
                        existing = conn
                        break

                if not existing:
                    raise HTTPException(status_code=404, detail="Connection not found")

                # Merge updates
                test_config = {**existing, **connection_data}
                exporter = HTTPJsonExporter(test_config)
                test_result = await exporter.test_connection()
                if not test_result['success']:
                    raise HTTPException(
                        status_code=400,
                        detail=f"Connection test failed: {test_result['message']}"
                    )
            except HTTPException:
                raise
            except Exception as e:
                raise HTTPException(status_code=400, detail=f"Connection test failed: {str(e)}")

        # Update connection
        success = conn_mgr.update_http_json_connection(connection_id, connection_data)
        if not success:
            raise HTTPException(status_code=404, detail="Connection not found")

        return {
            "status": "success",
            "message": f"HTTP JSON connection updated successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update HTTP JSON connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/connections/{connection_id}/activate")
async def activate_http_json_connection(connection_id: int):
    """Activate HTTP JSON connection"""
    try:
        conn_mgr = ConnectionManager()
        conn_mgr.activate_http_json_connection(connection_id)
        return {"status": "success", "message": "Connection activated successfully"}
    except Exception as e:
        logger.error(f"Failed to activate HTTP JSON connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/connections/{connection_id}")
async def delete_http_json_connection(
    connection_id: int
):
    """Delete HTTP JSON connection"""
    try:
        conn_mgr = ConnectionManager()
        success = conn_mgr.delete_http_json_connection(connection_id)

        if not success:
            raise HTTPException(status_code=404, detail="Connection not found")

        return {
            "status": "success",
            "message": "HTTP JSON connection deleted successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete HTTP JSON connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/connections/{connection_id}/activate")
async def activate_http_json_connection(
    connection_id: int
):
    """Set HTTP JSON connection as active"""
    try:
        conn_mgr = ConnectionManager()
        success = conn_mgr.set_http_json_connection_active(connection_id)

        if not success:
            raise HTTPException(status_code=404, detail="Connection not found")

        return {
            "status": "success",
            "message": "HTTP JSON connection activated successfully"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to activate HTTP JSON connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/connections/{connection_id}/test")
async def test_http_json_connection(
    connection_id: int
):
    """Test HTTP JSON connection"""
    try:
        conn_mgr = ConnectionManager()
        connections = conn_mgr.get_http_json_connections()

        connection = None
        for conn in connections:
            if conn['id'] == connection_id:
                connection = conn
                break

        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")

        # Test connection
        exporter = HTTPJsonExporter(connection)
        test_result = await exporter.test_connection()

        return {
            "status": "success" if test_result['success'] else "failed",
            "message": test_result['message'],
            "sample_records": test_result.get('sample_records', []),
            "total_records": test_result.get('total_records', 0)
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to test HTTP JSON connection: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/connections/{connection_id}/discover-schema")
async def discover_schema(
    connection_id: int
):
    """Discover the schema of HTTP JSON API response"""
    try:
        conn_mgr = ConnectionManager()
        connections = conn_mgr.get_http_json_connections()

        connection = None
        for conn in connections:
            if conn['id'] == connection_id:
                connection = conn
                break

        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")

        # Discover schema
        exporter = HTTPJsonExporter(connection)
        schema = await exporter.discover_schema()

        return {
            "status": "success",
            "schema": schema
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to discover schema: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/export")
async def export_http_json_data(
    export_request: Dict = Body(...)
):
    """Export data from HTTP JSON API to storage"""
    try:
        conn_mgr = ConnectionManager()

        # Get HTTP JSON connection
        connection_id = export_request.get('connection_id')
        if not connection_id:
            raise HTTPException(status_code=400, detail="Missing connection_id")

        connections = conn_mgr.get_http_json_connections()
        connection = None
        for conn in connections:
            if conn['id'] == connection_id:
                connection = conn
                break

        if not connection:
            raise HTTPException(status_code=404, detail="Connection not found")

        # Get storage connection
        storage_conn = conn_mgr.get_active_storage_connection()
        if not storage_conn:
            raise HTTPException(status_code=400, detail="No active storage connection")

        # Parse time range
        start_date = datetime.fromisoformat(export_request.get('start_date'))
        end_date = datetime.fromisoformat(export_request.get('end_date'))
        measurement = export_request.get('measurement', connection.get('measurement_field', 'http_json'))

        # Initialize exporter
        exporter = HTTPJsonExporter(connection)

        # Create storage backend
        from storage.minio_backend import MinIOBackend
        storage = MinIOBackend(
            endpoint_url=storage_conn['endpoint'],
            access_key=storage_conn['access_key'],
            secret_key=storage_conn['secret_key'],
            bucket=storage_conn['bucket'],
            database=storage_conn.get('database', 'default')
        )

        # Export to temporary local file first
        import tempfile
        import os
        with tempfile.TemporaryDirectory() as temp_dir:
            # Export data
            records_exported = await exporter.export_to_parquet(
                measurement=measurement,
                start_date=start_date,
                end_date=end_date,
                output_path=temp_dir
            )

            # Upload to storage
            if records_exported > 0:
                parquet_file = f"{measurement}_{start_date.strftime('%Y%m%d')}_{end_date.strftime('%Y%m%d')}.parquet"
                local_path = os.path.join(temp_dir, parquet_file)

                if os.path.exists(local_path):
                    remote_path = f"{storage_conn.get('prefix', '')}/{measurement}/{parquet_file}".strip('/')
                    storage.upload_file(local_path, remote_path)

        return {
            "status": "success",
            "records_exported": records_exported,
            "message": f"Exported {records_exported} records from {connection['name']}"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to export HTTP JSON data: {e}")
        raise HTTPException(status_code=500, detail=str(e))
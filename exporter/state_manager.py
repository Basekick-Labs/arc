import json
import aiofiles
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

class StateManager:
    def __init__(self, state_file: Path = Path("historian_state.json")):
        self.state_file = state_file
        
    async def get_last_export(self, measurement: str) -> Optional[datetime]:
        """Get last successful export timestamp for measurement"""
        if not self.state_file.exists():
            return None
            
        async with aiofiles.open(self.state_file, 'r') as f:
            content = await f.read()
            state = json.loads(content)
            
        timestamp_str = state.get('last_exports', {}).get(measurement)
        if timestamp_str:
            return datetime.fromisoformat(timestamp_str)
        return None

    async def update_last_export(self, measurement: str, timestamp: datetime) -> None:
        """Update last successful export timestamp"""
        state = {}
        if self.state_file.exists():
            async with aiofiles.open(self.state_file, 'r') as f:
                content = await f.read()
                state = json.loads(content)
        
        if 'last_exports' not in state:
            state['last_exports'] = {}
            
        state['last_exports'][measurement] = timestamp.isoformat()
        
        async with aiofiles.open(self.state_file, 'w') as f:
            await f.write(json.dumps(state, indent=2))


class SQLiteStateManager:
    """State manager backed by SQLite for centralized tracking.
    Stores last export timestamps per measurement in /tmp/historian.db (cli_export_state table).
    """
    def __init__(self, db_path: str = "/tmp/historian.db"):
        import sqlite3
        self.db_path = db_path
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS cli_export_state (
                measurement TEXT PRIMARY KEY,
                last_export_time TEXT
            )
            """
        )
        conn.commit()
        conn.close()

    async def get_last_export(self, measurement: str) -> Optional[datetime]:
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("SELECT last_export_time FROM cli_export_state WHERE measurement = ?", (measurement,))
        row = cur.fetchone()
        conn.close()
        if row and row[0]:
            return datetime.fromisoformat(row[0])
        return None

    async def update_last_export(self, measurement: str, timestamp: datetime) -> None:
        import sqlite3
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute(
            "INSERT INTO cli_export_state (measurement, last_export_time) VALUES (?, ?)\n             ON CONFLICT(measurement) DO UPDATE SET last_export_time = excluded.last_export_time",
            (measurement, timestamp.isoformat()),
        )
        conn.commit()
        conn.close()

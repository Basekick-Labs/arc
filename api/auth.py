"""
Simple API Token Authentication for Arc Core
No RBAC - simple token-based authentication only
"""
import hashlib
import os
import secrets
import sqlite3
import time
from datetime import datetime
from typing import Optional, Dict
import logging
import threading

from fastapi import HTTPException, Request
from api.config import get_db_path

logger = logging.getLogger(__name__)


class AuthManager:
    """Simple token-based authentication manager with in-memory cache"""

    def __init__(self, db_path: str = None, cache_ttl: int = 30, max_cache_size: int = 1000):
        """
        Initialize AuthManager

        Args:
            db_path: Path to SQLite database
            cache_ttl: Cache TTL in seconds (default: 30s)
            max_cache_size: Maximum number of cached tokens (default: 1000)
        """
        from collections import OrderedDict
        self.db_path = db_path or get_db_path()
        self.cache_ttl = cache_ttl
        self.max_cache_size = max_cache_size
        self._cache = OrderedDict()  # token_hash -> (token_info, expiry_time), LRU order
        self._cache_lock = threading.Lock()
        self._cache_hits = 0
        self._cache_misses = 0
        self._cache_evictions = 0
        self._init_db()

    def _init_db(self):
        """Initialize authentication database"""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS api_tokens (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL UNIQUE,
                    token_hash TEXT NOT NULL,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_used_at TIMESTAMP,
                    enabled INTEGER DEFAULT 1
                )
            """)

            # Add permissions column if it doesn't exist (migration)
            cursor = conn.execute("PRAGMA table_info(api_tokens)")
            columns = [row[1] for row in cursor.fetchall()]
            if 'permissions' not in columns:
                try:
                    conn.execute("ALTER TABLE api_tokens ADD COLUMN permissions TEXT DEFAULT 'read,write'")
                    logger.info("Added permissions column to api_tokens table")
                except sqlite3.OperationalError as e:
                    # Column might already exist from a race condition
                    if "duplicate column" not in str(e).lower():
                        raise

            conn.commit()

    def _hash_token(self, token: str) -> str:
        """Hash a token for storage"""
        return hashlib.sha256(token.encode()).hexdigest()

    def has_permission(self, token_info: Dict, permission: str) -> bool:
        """
        Check if a token has a specific permission

        Args:
            token_info: Token info dict from verify_token
            permission: Permission to check (read, write, delete, admin)

        Returns:
            True if token has the permission
        """
        if not token_info:
            return False

        permissions = token_info.get('permissions', [])

        # Admin permission grants all permissions
        if 'admin' in permissions:
            return True

        # Check specific permission
        return permission in permissions

    def create_token(self, name: str, description: str = None, expires_at: datetime = None, permissions: str = "read,write") -> str:
        """
        Create a new API token

        Args:
            name: Token name (unique identifier)
            description: Optional description
            expires_at: Optional expiration datetime
            permissions: Comma-separated permissions (default: "read,write")
                        Available: read, write, delete, admin

        Returns:
            The generated token string (save this - it's not stored!)
        """
        # Generate secure random token
        token = secrets.token_urlsafe(32)
        token_hash = self._hash_token(token)

        with sqlite3.connect(self.db_path) as conn:
            try:
                # Check what columns exist
                cursor = conn.execute("PRAGMA table_info(api_tokens)")
                columns = [row[1] for row in cursor.fetchall()]
                has_expires_at = 'expires_at' in columns
                has_permissions = 'permissions' in columns

                # Build INSERT query based on available columns
                if has_expires_at and has_permissions and expires_at is not None:
                    conn.execute(
                        "INSERT INTO api_tokens (name, token_hash, description, expires_at, permissions) VALUES (?, ?, ?, ?, ?)",
                        (name, token_hash, description, expires_at, permissions)
                    )
                elif has_permissions:
                    conn.execute(
                        "INSERT INTO api_tokens (name, token_hash, description, permissions) VALUES (?, ?, ?, ?)",
                        (name, token_hash, description, permissions)
                    )
                elif has_expires_at and expires_at is not None:
                    conn.execute(
                        "INSERT INTO api_tokens (name, token_hash, description, expires_at) VALUES (?, ?, ?, ?)",
                        (name, token_hash, description, expires_at)
                    )
                else:
                    conn.execute(
                        "INSERT INTO api_tokens (name, token_hash, description) VALUES (?, ?, ?)",
                        (name, token_hash, description)
                    )
                conn.commit()
                logger.info(f"Created API token: {name} with permissions: {permissions}")
                return token
            except sqlite3.IntegrityError:
                raise ValueError(f"Token with name '{name}' already exists")

    def verify_token(self, token: str) -> Optional[Dict]:
        """Verify a token and return token info if valid (with caching)"""
        if not token:
            logger.debug("Authentication failed: No token provided")
            return None

        token_hash = self._hash_token(token)
        current_time = time.time()

        # Check cache first
        with self._cache_lock:
            if token_hash in self._cache:
                token_info, expiry_time = self._cache[token_hash]
                if current_time < expiry_time:
                    self._cache_hits += 1
                    # Move to end (most recently used)
                    self._cache.move_to_end(token_hash)
                    return token_info
                else:
                    # Cache expired, remove it
                    del self._cache[token_hash]

            self._cache_misses += 1

        # Cache miss - query database
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                cursor = conn.execute(
                    "SELECT * FROM api_tokens WHERE token_hash = ? AND enabled = 1",
                    (token_hash,)
                )
                row = cursor.fetchone()

                if row:
                    # Check token expiration if expires_at is set
                    try:
                        expires_at_value = row['expires_at'] if 'expires_at' in row.keys() else None
                    except (KeyError, IndexError):
                        expires_at_value = None

                    if expires_at_value:
                        try:
                            # Handle both datetime objects and ISO strings
                            if isinstance(expires_at_value, str):
                                expires_at = datetime.fromisoformat(expires_at_value.replace('Z', '+00:00'))
                            else:
                                expires_at = expires_at_value

                            if datetime.now() > expires_at:
                                logger.warning(f"Authentication failed: Token '{row['name']}' has expired")
                                return None
                        except Exception as e:
                            logger.error(f"Error checking token expiration: {e}")
                            return None

                    # Update last used timestamp
                    conn.execute(
                        "UPDATE api_tokens SET last_used_at = ? WHERE id = ?",
                        (datetime.now(), row['id'])
                    )
                    conn.commit()

                    # Get permissions if column exists
                    try:
                        permissions = row['permissions'] if 'permissions' in row.keys() else 'read,write'
                    except (KeyError, IndexError):
                        permissions = 'read,write'

                    token_info = {
                        'id': row['id'],
                        'name': row['name'],
                        'description': row['description'],
                        'created_at': row['created_at'],
                        'last_used_at': row['last_used_at'],
                        'permissions': permissions.split(',') if permissions else []
                    }

                    # Store in cache with LRU eviction
                    with self._cache_lock:
                        # Evict oldest entry if cache is full
                        if len(self._cache) >= self.max_cache_size and token_hash not in self._cache:
                            # Remove oldest (first item in OrderedDict)
                            self._cache.popitem(last=False)
                            self._cache_evictions += 1

                        self._cache[token_hash] = (token_info, current_time + self.cache_ttl)

                    return token_info
                else:
                    logger.warning(f"Authentication failed: Invalid token (hash: {token_hash[:8]}...)")
                    return None

        except sqlite3.OperationalError as e:
            logger.error(f"Database error during token verification: {e} (db_path: {self.db_path})")
            return None
        except Exception as e:
            logger.error(f"Unexpected error during token verification: {e}", exc_info=True)
            return None

    def list_tokens(self) -> list:
        """List all API tokens (without revealing actual tokens)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            # Check if permissions column exists
            cursor = conn.execute("PRAGMA table_info(api_tokens)")
            columns = [row[1] for row in cursor.fetchall()]
            has_permissions = 'permissions' in columns

            if has_permissions:
                cursor = conn.execute(
                    "SELECT id, name, description, created_at, last_used_at, enabled, permissions FROM api_tokens"
                )
            else:
                cursor = conn.execute(
                    "SELECT id, name, description, created_at, last_used_at, enabled FROM api_tokens"
                )

            tokens = []
            for row in cursor.fetchall():
                token_dict = dict(row)
                # Convert permissions string to list if it exists
                if 'permissions' in token_dict and token_dict['permissions']:
                    token_dict['permissions'] = token_dict['permissions'].split(',')
                elif 'permissions' not in token_dict:
                    token_dict['permissions'] = ['read', 'write']  # Default
                tokens.append(token_dict)

            return tokens

    def revoke_token(self, name: str) -> bool:
        """Revoke (disable) a token and invalidate cache"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "UPDATE api_tokens SET enabled = 0 WHERE name = ?",
                (name,)
            )
            conn.commit()
            success = cursor.rowcount > 0

        # Invalidate entire cache when revoking to ensure immediate effect
        if success:
            self.invalidate_cache()

        return success

    def delete_token(self, name: str) -> bool:
        """Delete a token permanently by name and invalidate cache"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM api_tokens WHERE name = ?",
                (name,)
            )
            conn.commit()
            success = cursor.rowcount > 0

        # Invalidate cache when deleting
        if success:
            self.invalidate_cache()

        return success

    def update_token(self, token_id: int, name: str = None, description: str = None, expires_at: datetime = None, permissions: str = None) -> bool:
        """
        Update token metadata

        Args:
            token_id: Token ID to update
            name: New name (optional)
            description: New description (optional)
            expires_at: New expiration date (optional)
            permissions: New permissions (optional)

        Returns:
            True if updated successfully
        """
        with sqlite3.connect(self.db_path) as conn:
            # Check what columns exist
            cursor = conn.execute("PRAGMA table_info(api_tokens)")
            columns = [row[1] for row in cursor.fetchall()]
            has_expires_at = 'expires_at' in columns
            has_permissions = 'permissions' in columns

            # Build UPDATE query dynamically based on provided fields
            updates = []
            values = []

            if name is not None:
                updates.append("name = ?")
                values.append(name)

            if description is not None:
                updates.append("description = ?")
                values.append(description)

            if expires_at is not None and has_expires_at:
                updates.append("expires_at = ?")
                values.append(expires_at)

            if permissions is not None and has_permissions:
                updates.append("permissions = ?")
                values.append(permissions)

            if not updates:
                # Nothing to update
                return False

            # Add token_id for WHERE clause
            values.append(token_id)

            # Execute update
            sql = f"UPDATE api_tokens SET {', '.join(updates)} WHERE id = ?"
            cursor = conn.execute(sql, values)
            conn.commit()
            success = cursor.rowcount > 0

        # Invalidate cache when updating
        if success:
            self.invalidate_cache()
            logger.info(f"Updated token ID: {token_id}")

        return success

    def delete_token_by_id(self, token_id: int) -> bool:
        """Delete a token permanently by ID and invalidate cache"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM api_tokens WHERE id = ?",
                (token_id,)
            )
            conn.commit()
            success = cursor.rowcount > 0

        # Invalidate cache when deleting
        if success:
            self.invalidate_cache()

        return success

    def get_token_info(self, token_id: int) -> Optional[Dict]:
        """Get token info by ID (without revealing the token value)"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row

            # Check if permissions column exists
            cursor = conn.execute("PRAGMA table_info(api_tokens)")
            columns = [row[1] for row in cursor.fetchall()]
            has_permissions = 'permissions' in columns

            if has_permissions:
                cursor = conn.execute(
                    "SELECT id, name, description, created_at, last_used_at, enabled, permissions FROM api_tokens WHERE id = ?",
                    (token_id,)
                )
            else:
                cursor = conn.execute(
                    "SELECT id, name, description, created_at, last_used_at, enabled FROM api_tokens WHERE id = ?",
                    (token_id,)
                )

            row = cursor.fetchone()
            if row:
                token_dict = dict(row)
                # Convert permissions string to list if it exists
                if 'permissions' in token_dict and token_dict['permissions']:
                    token_dict['permissions'] = token_dict['permissions'].split(',')
                elif 'permissions' not in token_dict:
                    token_dict['permissions'] = ['read', 'write']  # Default
                return token_dict
            return None

    def rotate_token(self, token_id: int) -> Optional[str]:
        """Rotate a token - generate new token value while keeping metadata"""
        # Generate new secure random token
        new_token = secrets.token_urlsafe(32)
        new_token_hash = self._hash_token(new_token)

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "UPDATE api_tokens SET token_hash = ? WHERE id = ?",
                (new_token_hash, token_id)
            )
            conn.commit()
            success = cursor.rowcount > 0

        # Invalidate cache when rotating to ensure old token stops working immediately
        if success:
            self.invalidate_cache()
            logger.info(f"Rotated token ID: {token_id}")
            return new_token

        return None

    def invalidate_cache(self):
        """Clear the entire token cache"""
        with self._cache_lock:
            cleared_count = len(self._cache)
            self._cache.clear()
            logger.info(f"Token cache invalidated: cleared {cleared_count} entries")

    def get_cache_stats(self) -> Dict:
        """Get cache statistics"""
        with self._cache_lock:
            total_requests = self._cache_hits + self._cache_misses
            hit_rate = (self._cache_hits / total_requests * 100) if total_requests > 0 else 0

            return {
                "cache_size": len(self._cache),
                "max_cache_size": self.max_cache_size,
                "cache_ttl_seconds": self.cache_ttl,
                "utilization_percent": round(len(self._cache) / self.max_cache_size * 100, 1),
                "total_requests": total_requests,
                "cache_hits": self._cache_hits,
                "cache_misses": self._cache_misses,
                "cache_evictions": self._cache_evictions,
                "hit_rate_percent": round(hit_rate, 2)
            }

    def ensure_seed_token(self, token: str, name: str = "default") -> bool:
        """Ensure a seed token exists (for initial setup)"""
        token_hash = self._hash_token(token)

        with sqlite3.connect(self.db_path) as conn:
            # Check if token already exists
            cursor = conn.execute(
                "SELECT id FROM api_tokens WHERE name = ?",
                (name,)
            )
            if cursor.fetchone():
                return False  # Token already exists

            # Create seed token
            conn.execute(
                "INSERT INTO api_tokens (name, token_hash, description) VALUES (?, ?, ?)",
                (name, token_hash, "Default seed token")
            )
            conn.commit()
            logger.info(f"Created seed token: {name}")
            return True

    def ensure_initial_token(self) -> Optional[str]:
        """
        Ensure an initial admin token exists on first run.
        Returns the token if created, None if tokens already exist.
        Thread-safe: handles race conditions when multiple workers start simultaneously.
        """
        with sqlite3.connect(self.db_path) as conn:
            # Check if any tokens exist
            cursor = conn.execute("SELECT COUNT(*) FROM api_tokens")
            count = cursor.fetchone()[0]

            if count > 0:
                # Tokens already exist, no need to create initial token
                return None

            # First run - create initial admin token with all permissions
            logger.info("First run detected - creating initial admin token")
            try:
                token = self.create_token(
                    name="admin",
                    description="Initial admin token (auto-generated on first run)",
                    permissions="read,write,delete,admin"
                )
                return token
            except ValueError as e:
                # Race condition: another worker already created the token
                # This is expected in multi-worker setups, silently ignore
                if "already exists" in str(e):
                    logger.debug("Admin token already created by another worker")
                    return None
                raise

    def verify_request_header(self, headers) -> bool:
        """Verify authentication from request headers"""
        auth_header = headers.get("Authorization", "") or headers.get("authorization", "")

        if not auth_header:
            return False

        # Extract token from Bearer or Token prefix
        token = None
        if auth_header.startswith("Bearer "):
            token = auth_header[7:]
        elif auth_header.startswith("Token "):
            token = auth_header[6:]
        else:
            token = auth_header

        # Verify the token
        return self.verify_token(token) is not None


class AuthMiddleware:
    """Simple authentication middleware"""

    def __init__(self, auth_manager: AuthManager, enabled: bool = True, allowlist: list = None):
        self.auth_manager = auth_manager
        self.enabled = enabled
        self.allowlist = allowlist or []

    async def __call__(self, request: Request, call_next):
        # Skip auth for allowlisted paths
        if not self.enabled or any(request.url.path.startswith(path) for path in self.allowlist):
            return await call_next(request)

        # Extract token from Authorization header or x-api-key header
        auth_header = request.headers.get("Authorization", "")
        api_key = request.headers.get("x-api-key", "")
        token = None

        if auth_header:
            if auth_header.startswith("Bearer "):
                token = auth_header[7:]
            elif auth_header.startswith("Token "):
                token = auth_header[6:]
            else:
                token = auth_header
        elif api_key:
            # Support x-api-key header for compatibility
            token = api_key

        # Verify token
        token_info = self.auth_manager.verify_token(token)

        if not token_info:
            return JSONResponse(
                status_code=401,
                content={"error": "Unauthorized", "detail": "Invalid or missing API token"}
            )

        # Attach token info to request state (both names for compatibility)
        request.state.token_info = token_info
        request.state.token_data = token_info  # For delete_routes.py compatibility

        return await call_next(request)


# For backwards compatibility
from fastapi.responses import JSONResponse

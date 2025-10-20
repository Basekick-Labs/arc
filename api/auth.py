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

    def __init__(self, db_path: str = None, cache_ttl: int = 30):
        """
        Initialize AuthManager

        Args:
            db_path: Path to SQLite database
            cache_ttl: Cache TTL in seconds (default: 30s)
        """
        self.db_path = db_path or get_db_path()
        self.cache_ttl = cache_ttl
        self._cache = {}  # token_hash -> (token_info, expiry_time)
        self._cache_lock = threading.Lock()
        self._cache_hits = 0
        self._cache_misses = 0
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
            conn.commit()

    def _hash_token(self, token: str) -> str:
        """Hash a token for storage"""
        return hashlib.sha256(token.encode()).hexdigest()

    def create_token(self, name: str, description: str = None, expires_at: datetime = None) -> str:
        """Create a new API token"""
        # Generate secure random token
        token = secrets.token_urlsafe(32)
        token_hash = self._hash_token(token)

        with sqlite3.connect(self.db_path) as conn:
            try:
                # Check if expires_at column exists, if not, ignore it
                cursor = conn.execute("PRAGMA table_info(api_tokens)")
                columns = [row[1] for row in cursor.fetchall()]
                has_expires_at = 'expires_at' in columns

                if has_expires_at and expires_at is not None:
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
                logger.info(f"Created API token: {name}")
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

                    token_info = {
                        'id': row['id'],
                        'name': row['name'],
                        'description': row['description'],
                        'created_at': row['created_at'],
                        'last_used_at': row['last_used_at']
                    }

                    # Store in cache
                    with self._cache_lock:
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
            cursor = conn.execute(
                "SELECT id, name, description, created_at, last_used_at, enabled FROM api_tokens"
            )
            return [dict(row) for row in cursor.fetchall()]

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
            cursor = conn.execute(
                "SELECT id, name, description, created_at, last_used_at, enabled FROM api_tokens WHERE id = ?",
                (token_id,)
            )
            row = cursor.fetchone()
            if row:
                return dict(row)
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
                "cache_ttl_seconds": self.cache_ttl,
                "total_requests": total_requests,
                "cache_hits": self._cache_hits,
                "cache_misses": self._cache_misses,
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

            # First run - create initial admin token
            logger.info("First run detected - creating initial admin token")
            try:
                token = self.create_token(
                    name="admin",
                    description="Initial admin token (auto-generated on first run)"
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

        # Attach token info to request state
        request.state.token_info = token_info

        return await call_next(request)


# For backwards compatibility
from fastapi.responses import JSONResponse

"""
api/connection_store.py
-----------------------
Thread-safe in-memory store for named database connections.
Allows the UI to save, list, switch, and delete DB connections.
In production: back with Redis or a DB.
"""

from __future__ import annotations
import threading
from typing import Any, Dict, List, Optional


class ConnectionStore:
    """Stores named database connection configs."""

    def __init__(self):
        self._connections: Dict[str, Dict[str, Any]] = {}
        self._lock = threading.Lock()
        # Pre-seed with the default from config.yaml if desired
        # (auto_pipeline.py can call add() on startup)

    def add(self, name: str, db_config: dict) -> dict:
        """Save a named connection. Overwrites if name exists."""
        with self._lock:
            safe = {k: v for k, v in db_config.items() if k != "password"}
            safe["_has_password"] = bool(db_config.get("password"))
            self._connections[name] = {
                "name": name,
                "config": db_config,          # full config including password
                "display": safe,              # safe version for API responses
                "engine": db_config.get("engine", "postgres"),
            }
            return {"name": name, **safe}

    def get(self, name: str) -> Optional[Dict]:
        with self._lock:
            conn = self._connections.get(name)
            return dict(conn) if conn else None

    def get_config(self, name: str) -> Optional[Dict]:
        """Return full config including password for internal use."""
        with self._lock:
            conn = self._connections.get(name)
            return conn["config"] if conn else None

    def list_all(self) -> List[Dict]:
        with self._lock:
            return [
                {"name": name, **c["display"], "engine": c["engine"]}
                for name, c in self._connections.items()
            ]

    def delete(self, name: str) -> bool:
        with self._lock:
            if name in self._connections:
                del self._connections[name]
                return True
            return False

    def exists(self, name: str) -> bool:
        with self._lock:
            return name in self._connections


# Singleton
connection_store = ConnectionStore()
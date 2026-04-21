"""
api/job_store.py
----------------
In-memory job store for tracking pipeline runs.

For production, replace with Redis or a database.
"""

from __future__ import annotations

import threading
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional


class JobStatus(str, Enum):
    QUEUED = "queued"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class JobStore:
    """Thread-safe in-memory job store."""

    def __init__(self, max_jobs: int = 200):
        self._jobs: Dict[str, Dict[str, Any]] = {}
        self._order: List[str] = []
        self._lock = threading.Lock()
        self._max = max_jobs

    def create(self, job_id: str, request_data: dict) -> None:
        with self._lock:
            # Sanitise sensitive fields from stored request
            safe_request = {k: v for k, v in request_data.items() if k != "groq_api_key"}
            if "database" in safe_request:
                db = dict(safe_request["database"])
                db.pop("password", None)
                safe_request["database"] = db

            self._jobs[job_id] = {
                "job_id": job_id,
                "status": JobStatus.QUEUED,
                "created_at": datetime.utcnow().isoformat(),
                "started_at": None,
                "completed_at": None,
                "result": None,
                "error": None,
                "request_summary": safe_request,
            }
            self._order.append(job_id)

            # Evict oldest if over limit
            while len(self._order) > self._max:
                oldest = self._order.pop(0)
                self._jobs.pop(oldest, None)

    def start(self, job_id: str) -> None:
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id]["status"] = JobStatus.RUNNING
                self._jobs[job_id]["started_at"] = datetime.utcnow().isoformat()

    def complete(self, job_id: str, result: dict) -> None:
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id]["status"] = JobStatus.COMPLETED
                self._jobs[job_id]["completed_at"] = datetime.utcnow().isoformat()
                self._jobs[job_id]["result"] = result

    def fail(self, job_id: str, error: str) -> None:
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id]["status"] = JobStatus.FAILED
                self._jobs[job_id]["completed_at"] = datetime.utcnow().isoformat()
                self._jobs[job_id]["error"] = error

    def cancel(self, job_id: str) -> None:
        with self._lock:
            if job_id in self._jobs:
                self._jobs[job_id]["status"] = JobStatus.CANCELLED
                self._jobs[job_id]["completed_at"] = datetime.utcnow().isoformat()

    def get(self, job_id: str) -> Optional[Dict[str, Any]]:
        with self._lock:
            job = self._jobs.get(job_id)
            if job:
                return dict(job)  # Return a copy
            return None

    def list_all(self) -> List[Dict[str, Any]]:
        with self._lock:
            # Return last 50, newest first
            recent = self._order[-50:]
            return [dict(self._jobs[jid]) for jid in reversed(recent) if jid in self._jobs]
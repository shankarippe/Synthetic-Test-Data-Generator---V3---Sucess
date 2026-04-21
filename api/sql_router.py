"""
api/sql_router.py
-----------------
FastAPI router for the SQL editor panel.

Endpoints:
  POST /sql/execute     → run any SQL statement, return results
  POST /sql/explain     → run EXPLAIN / EXPLAIN PLAN
  POST /sql/validate    → check syntax without executing (EXPLAIN only)
  GET  /sql/dialect     → return editor dialect info for a connection
  GET  /sql/history     → last N queries for a connection (in-memory)
  POST /sql/ddl         → run DDL statements (CREATE/ALTER/DROP)
"""

from __future__ import annotations
import logging
from collections import defaultdict, deque
from datetime import datetime
from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.connection_store import connection_store
from core.sql_executor import SQLExecutor, DIALECT_INFO

logger = logging.getLogger("app")
router = APIRouter(prefix="/api/v1/sql", tags=["SQL Editor"])

# In-memory query history: connection_name → deque of last 50 queries
_history: dict[str, deque] = defaultdict(lambda: deque(maxlen=50))


class SQLExecuteRequest(BaseModel):
    connection_name: str
    sql: str
    params: Optional[List[Any]] = None
    max_rows: int = 1000          # cap at 5000


class SQLExplainRequest(BaseModel):
    connection_name: str
    sql: str


@router.post("/execute")
async def execute_sql(request: SQLExecuteRequest):
    """
    Execute any SQL statement against a saved connection.
    Returns:
      - columns, rows (for SELECT)
      - row_count (for DML)
      - statement_type: SELECT | DML | DDL
      - dialect: pgsql | tsql | plsql | mysql
      - execution_ms: elapsed time
      - error: null or error string
    Errors do NOT raise HTTP 500 — they return error in the response body
    so the SQL editor can display them inline.
    """
    cfg = _get_config(request.connection_name)
    executor = SQLExecutor(cfg)
    max_rows = min(request.max_rows, 5000)

    t0 = datetime.utcnow()
    result = executor.execute(request.sql, request.params, max_rows)
    elapsed = (datetime.utcnow() - t0).total_seconds() * 1000

    # Record in history
    _history[request.connection_name].appendleft({
        "sql": request.sql[:500],
        "executed_at": t0.isoformat(),
        "statement_type": result.get("statement_type"),
        "row_count": result.get("row_count", 0),
        "error": result.get("error"),
    })

    return {
        **result,
        "execution_ms": round(elapsed, 2),
        "dialect": executor.get_dialect(),
        "connection": request.connection_name,
    }


@router.post("/explain")
async def explain_sql(request: SQLExplainRequest):
    """
    Return EXPLAIN output for a SELECT query.
    Engine-aware: uses EXPLAIN ANALYZE (PG), EXPLAIN PLAN (Oracle), SET SHOWPLAN_TEXT (MSSQL), EXPLAIN (MySQL).
    """
    cfg = _get_config(request.connection_name)
    executor = SQLExecutor(cfg)
    engine = cfg.get("engine", "postgres")

    if engine in ("postgres", "postgresql"):
        explain_sql = f"EXPLAIN ANALYZE {request.sql}"
    elif engine in ("oracle", "oracledb"):
        explain_sql = f"EXPLAIN PLAN FOR {request.sql}"
    elif engine in ("sqlserver", "mssql"):
        # MSSQL: needs SET SHOWPLAN_TEXT ON; not easily wrapped in one call
        # Return a helpful message instead
        return {"explain": "Use SET SHOWPLAN_TEXT ON / OFF in the SQL editor for MSSQL execution plans.", "engine": engine}
    else:  # mysql
        explain_sql = f"EXPLAIN {request.sql}"

    result = executor.execute(explain_sql)
    return {"explain_rows": result.get("rows", []), "columns": result.get("columns", []), "error": result.get("error")}


@router.post("/validate")
async def validate_sql(request: SQLExplainRequest):
    """
    Validate SQL syntax without executing.
    Runs EXPLAIN only — no data is read or modified.
    Returns: { "valid": bool, "error": str | null }
    """
    cfg = _get_config(request.connection_name)
    executor = SQLExecutor(cfg)
    engine = cfg.get("engine", "postgres")

    if engine in ("postgres", "postgresql"):
        test_sql = f"EXPLAIN {request.sql}"
    elif engine in ("oracle", "oracledb"):
        test_sql = f"EXPLAIN PLAN FOR {request.sql}"
    else:
        # MySQL / MSSQL: just try executing with LIMIT 0
        test_sql = f"SELECT * FROM ({request.sql}) _v WHERE 1=0"

    result = executor.execute(test_sql)
    error = result.get("error")
    return {"valid": error is None, "error": error}


@router.get("/dialect")
async def get_dialect(connection_name: str):
    """
    Return SQL dialect info for a saved connection.
    The frontend uses this to configure the Monaco/CodeMirror editor language mode.

    Response:
      {
        "dialect":     "pgsql" | "tsql" | "plsql" | "mysql",
        "label":       "PostgreSQL" | "T-SQL (SQL Server)" | ...,
        "placeholder": "%s" | "?" | ":1",
        "engine":      "postgres" | "sqlserver" | "oracle" | "mysql",
        "schema":      "public" | "dbo" | "BANKING" | ...
      }
    """
    cfg = _get_config(connection_name)
    engine = cfg.get("engine", "postgres").lower()
    info = DIALECT_INFO.get(engine, {"dialect": "pgsql", "placeholder": "%s", "label": "SQL"})
    return {**info, "engine": engine, "schema": cfg.get("schema", "public")}


@router.get("/history")
async def get_history(connection_name: str, limit: int = 20):
    """Return the last N SQL statements executed for a connection."""
    _get_config(connection_name)   # validates the connection exists
    history = list(_history.get(connection_name, []))[:limit]
    return {"connection": connection_name, "history": history}


@router.post("/ddl")
async def execute_ddl(request: SQLExecuteRequest):
    """
    Convenience endpoint for DDL statements (CREATE/ALTER/DROP/TRUNCATE).
    Identical to /execute but tagged separately so the frontend can
    show a confirmation dialog before calling this endpoint.
    """
    return await execute_sql(request)


def _get_config(connection_name: str) -> dict:
    cfg = connection_store.get_config(connection_name)
    if not cfg:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_name}' not found.")
    return cfg
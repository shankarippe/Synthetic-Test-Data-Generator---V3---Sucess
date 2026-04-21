"""
api/data_router.py
------------------
FastAPI router — registers under prefix /api/v1

Endpoints:
  Connections:
    GET    /data/connections          → list saved connections
    POST   /data/connections          → add / update a connection
    DELETE /data/connections/{name}   → remove a connection

  Data viewer (paginated table browser):
    GET    /data/tables               → list tables in schema
    POST   /data/query                → paginated SELECT with filters/sort
    GET    /data/preview/{table}      → first 100 rows (quick view)
    POST   /data/count                → row count for a table

  Import:
    POST   /data/import/preview       → parse uploaded CSV, return first 10 rows + column map
    POST   /data/import/commit        → append or replace rows into target table

  Export:
    POST   /data/export/dml           → returns INSERT statements as text
    POST   /data/export/csv           → returns CSV as text
    POST   /data/export/ddl           → returns CREATE TABLE statement
"""

from __future__ import annotations
import csv
import io
import logging
from typing import Any, List, Optional

from fastapi import APIRouter, HTTPException, UploadFile, File
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel

from api.connection_store import connection_store
from api.models import DatabaseConfig
from core.sql_executor import SQLExecutor
from core.export_engine import ExportEngine
from adapters import get_adapter

logger = logging.getLogger("app")
router = APIRouter(prefix="/api/v1", tags=["Data"])


# ── Pydantic models specific to this router ───────────────────────────

class ConnectionSaveRequest(BaseModel):
    name: str
    database: DatabaseConfig

class QueryRequest(BaseModel):
    connection_name: str
    table: str
    page: int = 1
    page_size: int = 100        # max 1000
    order_by: Optional[str] = None
    order_dir: str = "ASC"      # ASC | DESC
    filters: Optional[dict] = None   # {col: value}  simple equality filter

class CountRequest(BaseModel):
    connection_name: str
    table: str
    filters: Optional[dict] = None

class ExportRequest(BaseModel):
    connection_name: str
    table: str
    format: str = "dml"         # dml | csv | ddl
    limit: int = 10000          # max rows to export
    filters: Optional[dict] = None

class ImportPreviewRequest(BaseModel):
    connection_name: str
    table: str

class ImportCommitRequest(BaseModel):
    connection_name: str
    table: str
    mode: str = "append"        # append | replace
    rows: List[dict]            # parsed rows from the preview step
    columns: List[str]          # column names in order


# ── Connection management ─────────────────────────────────────────────

@router.get("/data/connections")
async def list_connections():
    """List all saved database connections (passwords hidden)."""
    return {"connections": connection_store.list_all()}


@router.post("/data/connections")
async def save_connection(request: ConnectionSaveRequest):
    """Save or update a named connection. Tests connectivity first."""
    db_dict = request.database.model_dump()
    dummy_loggers = {"app": logger, "error": logger, "audit": logger}
    try:
        adapter = get_adapter({"database": db_dict}, dummy_loggers)
        ok = adapter.test_connection()
        if not ok:
            raise HTTPException(status_code=400, detail="Connection test failed.")
        display = connection_store.add(request.name, db_dict)
        return {"success": True, "connection": display}
    except Exception as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.delete("/data/connections/{name}")
async def delete_connection(name: str):
    """Remove a saved connection by name."""
    if not connection_store.delete(name):
        raise HTTPException(status_code=404, detail=f"Connection '{name}' not found.")
    return {"success": True, "deleted": name}


# ── Table listing ─────────────────────────────────────────────────────

@router.get("/data/tables")
async def list_tables(connection_name: str):
    """Return all tables and their column counts for a saved connection."""
    cfg = _get_config(connection_name)
    dummy_loggers = {"app": logger, "error": logger, "audit": logger}
    try:
        adapter = get_adapter({"database": cfg}, dummy_loggers)
        table_meta = adapter.read_all()
        return {
            "connection": connection_name,
            "schema": cfg.get("schema", "public"),
            "engine": cfg.get("engine", "postgres"),
            "tables": [
                {
                    "name": name,
                    "column_count": len(tm.columns),
                    "primary_keys": tm.primary_keys,
                }
                for name, tm in table_meta.items()
            ]
        }
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ── Paginated data query ──────────────────────────────────────────────

@router.post("/data/query")
async def query_table(request: QueryRequest):
    """
    Paginated, filtered, sorted table data viewer.
    Returns columns + rows + total count + metadata.
    Column type information is included so the UI can render cells correctly.
    """
    cfg = _get_config(request.connection_name)
    executor = SQLExecutor(cfg)
    schema = cfg.get("schema", "public")
    engine = cfg.get("engine", "postgres")
    page_size = min(request.page_size, 1000)
    offset = (request.page - 1) * page_size

    # Build engine-aware SELECT
    qt = _quote_table(engine, schema, request.table)
    where, where_params = _build_where(engine, request.filters)
    order = _build_order(engine, request.order_by, request.order_dir)

    count_sql = f"SELECT COUNT(*) FROM {qt}{where}"
    count_result = executor.execute(count_sql, where_params)
    total = count_result["rows"][0][0] if count_result["rows"] else 0

    if engine in ("postgres", "postgresql", "mysql", "mariadb"):
        data_sql = f"SELECT * FROM {qt}{where}{order} LIMIT {page_size} OFFSET {offset}"
    elif engine in ("sqlserver", "mssql"):
        data_sql = (
            f"SELECT * FROM {qt}{where}{order} "
            f"OFFSET {offset} ROWS FETCH NEXT {page_size} ROWS ONLY"
        )
    elif engine in ("oracle", "oracledb"):
        data_sql = (
            f"SELECT * FROM (SELECT a.*, ROWNUM rn FROM "
            f"(SELECT * FROM {qt}{where}{order}) a WHERE ROWNUM <= {offset + page_size}) "
            f"WHERE rn > {offset}"
        )
    else:
        data_sql = f"SELECT * FROM {qt}{where}{order}"

    result = executor.execute(data_sql, where_params)
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])

    # Fetch column type metadata
    dummy_loggers = {"app": logger, "error": logger, "audit": logger}
    adapter = get_adapter({"database": cfg}, dummy_loggers)
    table_meta = adapter.read_all()
    col_meta = {}
    if request.table in table_meta:
        tm = table_meta[request.table]
        col_meta = {
            col.name: {
                "data_type": col.data_type,
                "udt_name": col.udt_name,
                "is_nullable": col.is_nullable,
                "is_pk": col.name in tm.primary_keys,
            }
            for col in tm.columns
        }

    return {
        "connection": request.connection_name,
        "table": request.table,
        "columns": result["columns"],
        "column_meta": col_meta,
        "rows": result["rows"],
        "page": request.page,
        "page_size": page_size,
        "total": total,
        "total_pages": max(1, (total + page_size - 1) // page_size),
        "engine": engine,
    }


@router.get("/data/preview/{table}")
async def preview_table(table: str, connection_name: str, limit: int = 100):
    """Quick preview — first N rows of a table, no pagination needed."""
    cfg = _get_config(connection_name)
    executor = SQLExecutor(cfg)
    engine = cfg.get("engine", "postgres")
    schema = cfg.get("schema", "public")
    qt = _quote_table(engine, schema, table)

    if engine in ("sqlserver", "mssql"):
        sql = f"SELECT TOP {limit} * FROM {qt}"
    elif engine in ("oracle", "oracledb"):
        sql = f"SELECT * FROM {qt} WHERE ROWNUM <= {limit}"
    else:
        sql = f"SELECT * FROM {qt} LIMIT {limit}"

    result = executor.execute(sql)
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])
    return {"table": table, "columns": result["columns"], "rows": result["rows"]}


@router.post("/data/count")
async def count_rows(request: CountRequest):
    """Return row count for a table, with optional filter."""
    cfg = _get_config(request.connection_name)
    executor = SQLExecutor(cfg)
    engine = cfg.get("engine", "postgres")
    schema = cfg.get("schema", "public")
    qt = _quote_table(engine, schema, request.table)
    where, params = _build_where(engine, request.filters)
    result = executor.execute(f"SELECT COUNT(*) FROM {qt}{where}", params)
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])
    return {"table": request.table, "count": result["rows"][0][0] if result["rows"] else 0}


# ── Import ────────────────────────────────────────────────────────────

@router.post("/data/import/preview")
async def import_preview(
    connection_name: str,
    table: str,
    file: UploadFile = File(...),
):
    """
    Upload a CSV file and return:
      - first 10 rows as preview
      - column names detected in CSV
      - column names in the target DB table
      - suggested column mapping (CSV col → DB col)
    """
    cfg = _get_config(connection_name)
    dummy_loggers = {"app": logger, "error": logger, "audit": logger}

    content = await file.read()
    text = content.decode("utf-8-sig")   # handle BOM
    reader = csv.DictReader(io.StringIO(text))
    csv_rows = []
    for i, row in enumerate(reader):
        if i >= 10:
            break
        csv_rows.append(dict(row))
    csv_cols = list(csv_rows[0].keys()) if csv_rows else []

    # Get DB table columns
    adapter = get_adapter({"database": cfg}, dummy_loggers)
    table_meta = adapter.read_all()
    if table not in table_meta:
        raise HTTPException(status_code=404, detail=f"Table '{table}' not found.")
    db_cols = [c.name for c in table_meta[table].columns]

    # Auto-suggest mapping (case-insensitive match)
    db_cols_lower = {c.lower(): c for c in db_cols}
    mapping = {}
    for csv_col in csv_cols:
        match = db_cols_lower.get(csv_col.lower())
        if match:
            mapping[csv_col] = match

    return {
        "file_name": file.filename,
        "csv_columns": csv_cols,
        "db_columns": db_cols,
        "suggested_mapping": mapping,
        "preview_rows": csv_rows,
        "connection": connection_name,
        "table": table,
    }


@router.post("/data/import/commit")
async def import_commit(request: ImportCommitRequest):
    """
    Insert rows into the target table.
    mode='append'  → INSERT rows as-is (errors on PK conflict)
    mode='replace' → TRUNCATE table first, then INSERT
    Rows should come from the parsed CSV (from /import/preview) with column mapping applied.
    """
    cfg = _get_config(request.connection_name)
    dummy_loggers = {"app": logger, "error": logger, "audit": logger}
    adapter = get_adapter({"database": cfg}, dummy_loggers)

    if request.mode == "replace":
        executor = SQLExecutor(cfg)
        engine = cfg.get("engine", "postgres")
        schema = cfg.get("schema", "public")
        qt = _quote_table(engine, schema, request.table)
        truncate_sql = f"TRUNCATE TABLE {qt}" if engine in ("postgres", "postgresql") else f"DELETE FROM {qt}"
        result = executor.execute(truncate_sql)
        if result.get("error"):
            raise HTTPException(status_code=400, detail=f"Truncate failed: {result['error']}")

    # Write rows to a temp CSV in memory, then use adapter.bulk_load
    import tempfile, os
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, newline="") as f:
        writer = csv.DictWriter(f, fieldnames=request.columns)
        writer.writeheader()
        writer.writerows(request.rows)
        tmp_path = f.name

    try:
        rows_loaded = adapter.bulk_load(request.table, tmp_path, request.columns)
    finally:
        os.unlink(tmp_path)

    return {
        "success": True,
        "table": request.table,
        "mode": request.mode,
        "rows_imported": rows_loaded,
    }


# ── Export ────────────────────────────────────────────────────────────

@router.post("/data/export/dml", response_class=PlainTextResponse)
async def export_dml(request: ExportRequest):
    """Export table data as INSERT INTO DML statements (plain text)."""
    cfg = _get_config(request.connection_name)
    rows, columns = _fetch_rows(cfg, request)
    engine = ExportEngine(cfg)
    rows_as_dicts = [dict(zip(columns, r)) for r in rows]
    return engine.export_dml(request.table, rows_as_dicts, columns)


@router.post("/data/export/csv", response_class=PlainTextResponse)
async def export_csv(request: ExportRequest):
    """Export table data as CSV text."""
    cfg = _get_config(request.connection_name)
    rows, columns = _fetch_rows(cfg, request)
    engine = ExportEngine(cfg)
    rows_as_dicts = [dict(zip(columns, r)) for r in rows]
    return engine.export_csv(rows_as_dicts, columns)


@router.post("/data/export/ddl", response_class=PlainTextResponse)
async def export_ddl(request: ExportRequest):
    """Export CREATE TABLE DDL reconstructed from live schema."""
    cfg = _get_config(request.connection_name)
    dummy_loggers = {"app": logger, "error": logger, "audit": logger}
    adapter = get_adapter({"database": cfg}, dummy_loggers)
    table_meta = adapter.read_all()
    if request.table not in table_meta:
        raise HTTPException(status_code=404, detail=f"Table '{request.table}' not found.")
    engine = ExportEngine(cfg)
    return engine.export_ddl(request.table, table_meta[request.table])


# ── Helpers ───────────────────────────────────────────────────────────

def _get_config(connection_name: str) -> dict:
    cfg = connection_store.get_config(connection_name)
    if not cfg:
        raise HTTPException(status_code=404, detail=f"Connection '{connection_name}' not found. Save it first via POST /data/connections.")
    return cfg


def _fetch_rows(cfg: dict, request: ExportRequest):
    executor = SQLExecutor(cfg)
    engine = cfg.get("engine", "postgres")
    schema = cfg.get("schema", "public")
    qt = _quote_table(engine, schema, request.table)
    where, params = _build_where(engine, request.filters)
    limit = min(request.limit, 50000)

    if engine in ("sqlserver", "mssql"):
        sql = f"SELECT TOP {limit} * FROM {qt}{where}"
    elif engine in ("oracle", "oracledb"):
        sql = f"SELECT * FROM {qt}{where} AND ROWNUM <= {limit}" if where else f"SELECT * FROM {qt} WHERE ROWNUM <= {limit}"
    else:
        sql = f"SELECT * FROM {qt}{where} LIMIT {limit}"

    result = executor.execute(sql, params)
    if result.get("error"):
        raise HTTPException(status_code=400, detail=result["error"])
    return result["rows"], result["columns"]


def _quote_table(engine: str, schema: str, table: str) -> str:
    if engine in ("sqlserver", "mssql"):
        return f"[{schema}].[{table}]"
    elif engine in ("mysql", "mariadb"):
        return f"`{schema}`.`{table}`"
    elif engine in ("oracle", "oracledb"):
        return f'"{schema.upper()}"."{table.upper()}"'
    return f'"{schema}"."{table}"'


def _build_where(engine: str, filters: dict | None):
    if not filters:
        return "", []
    ph = "?" if engine in ("sqlserver", "mssql") else "%s"
    parts, params = [], []
    for col, val in filters.items():
        qc = f"[{col}]" if engine in ("sqlserver", "mssql") else f'"{col}"'
        parts.append(f"{qc} = {ph}")
        params.append(val)
    return " WHERE " + " AND ".join(parts), params


def _build_order(engine: str, order_by: str | None, order_dir: str) -> str:
    if not order_by:
        return ""
    direction = "DESC" if order_dir.upper() == "DESC" else "ASC"
    qc = f"[{order_by}]" if engine in ("sqlserver", "mssql") else f'"{order_by}"'
    return f" ORDER BY {qc} {direction}"
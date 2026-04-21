"""
api/main.py
-----------
FastAPI application for the Synthetic Data Generation Framework.

Endpoints:
  POST /api/v1/pipeline/run          → Run the full automated pipeline
  POST /api/v1/pipeline/run-async    → Run pipeline async (returns job_id)
  GET  /api/v1/pipeline/status/{id} → Check async job status
  POST /api/v1/schema/read           → Read schema from DB (no generation)
  POST /api/v1/schema/test-connection → Test DB connectivity
  GET  /api/v1/scenarios             → List available scenarios
  POST /api/v1/scenarios/run         → Run a specific scenario
  GET  /api/v1/jobs                  → List all jobs
  GET  /api/v1/jobs/{job_id}         → Get job details
  DELETE /api/v1/jobs/{job_id}       → Cancel a running job
  GET  /api/v1/health                → Health check

Run with:
  uvicorn api.main:app --reload --host 0.0.0.0 --port 8000

Or:
  python -m api.main
"""

from __future__ import annotations

import asyncio
import os
import sys
import time

# Load .env immediately — must happen before any os.environ.get() calls
from pathlib import Path as _Path
try:
    from dotenv import load_dotenv as _load_dotenv
    _load_dotenv(dotenv_path=_Path(__file__).parent.parent / ".env", override=False)
except ImportError:
    pass
from api.data_router import router as data_router
from api.sql_router import router as sql_router

import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import yaml
from fastapi import FastAPI, BackgroundTasks, HTTPException, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse


# Add parent to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from api.models import (
    PipelineRunRequest,
    PipelineRunResponse,
    JobStatusResponse,
    SchemaReadRequest,
    SchemaReadResponse,
    ConnectionTestRequest,
    ConnectionTestResponse,
    ScenarioRunRequest,
    JobListResponse,
    HealthResponse,
)
from api.job_store import JobStore, JobStatus

# ---------------------------------------------------------------------------
# App setup
# ---------------------------------------------------------------------------

app = FastAPI(
    title="Synthetic Data Generation API",
    description=(
        "Enterprise-grade synthetic data generation framework. "
        "Supports PostgreSQL, Oracle, SQL Server, and MySQL. "
        "Powered by LangGraph + Groq LLM."
    ),
    version="2.1.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# CORS — allow UI team to connect from any origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(data_router)
app.include_router(sql_router)

# Job store (in-memory; replace with Redis for production)
job_store = JobStore()

# Thread pool for running blocking pipeline operations
executor = ThreadPoolExecutor(max_workers=4)


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

@app.get("/api/v1/health", response_model=HealthResponse, tags=["System"])
async def health_check():
    """Returns API health status and version info."""
    return HealthResponse(
    status="healthy",
    version="2.1.0",
    timestamp=datetime.utcnow().isoformat(),
    supported_engines=["postgres", "oracle", "sqlserver", "mysql"],
    groq_key_configured=bool(os.environ.get("GROQ_API_KEY")),
    )


# ---------------------------------------------------------------------------
# Connection test
# ---------------------------------------------------------------------------

@app.post("/api/v1/schema/test-connection", response_model=ConnectionTestResponse, tags=["Schema"])
async def test_connection(request: ConnectionTestRequest):
    """
    Test database connectivity.
    
    Provide database config and engine type — returns success/failure.
    """
    import logging
    dummy_loggers = {
        "app": logging.getLogger("app"),
        "error": logging.getLogger("error"),
        "audit": logging.getLogger("audit"),
    }

    config = {"database": request.database.model_dump()}

    try:
        from adapters import get_adapter
        adapter = get_adapter(config, dummy_loggers)
        ok = adapter.test_connection()
        return ConnectionTestResponse(
            success=ok,
            engine=request.database.engine,
            message="Connection successful" if ok else "Connection failed",
        )
    except Exception as exc:
        return ConnectionTestResponse(
            success=False,
            engine=request.database.get("engine", "unknown"), #type: ignore
            message=str(exc),
        )


# ---------------------------------------------------------------------------
# Schema reader
# ---------------------------------------------------------------------------

@app.post("/api/v1/schema/read", response_model=SchemaReadResponse, tags=["Schema"])
async def read_schema(request: SchemaReadRequest):
    """
    Read the database schema (tables, columns, PKs, FKs) without generating data.
    Useful for previewing what will be processed.
    """
    import logging
    dummy_loggers = {
        "app": logging.getLogger("app"),
        "error": logging.getLogger("error"),
        "audit": logging.getLogger("audit"),
    }

    config = {"database": request.database.model_dump()}

    try:
        from adapters import get_adapter
        adapter = get_adapter(config, dummy_loggers)
        table_meta = adapter.read_all()

        tables_summary = []
        for tbl_name, tm in table_meta.items():
            tables_summary.append({
                "table": tbl_name,
                "columns": len(tm.columns),
                "primary_keys": tm.primary_keys,
                "foreign_keys": [
                    {"column": fk.column, "ref_table": fk.ref_table, "ref_column": fk.ref_column}
                    for fk in tm.foreign_keys
                ],
            })

        return SchemaReadResponse(
            engine=request.database.engine,
            schema=request.database.schema,
            table_count=len(table_meta),
            tables=tables_summary,
        )
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Synchronous pipeline run
# ---------------------------------------------------------------------------

@app.post("/api/v1/pipeline/run", response_model=PipelineRunResponse, tags=["Pipeline"])
async def run_pipeline(request: PipelineRunRequest):
    """
    Run the full synthetic data generation pipeline synchronously.
    
    Waits for completion and returns full results.
    For large datasets use /run-async instead.
    """
    job_id = str(uuid.uuid4())
    job_store.create(job_id, request.model_dump())

    loop = asyncio.get_event_loop()
    try:
        result = await loop.run_in_executor(
            executor,
            _run_pipeline_sync,
            job_id, request,
        )
        return PipelineRunResponse(
            job_id=job_id,
            status="completed",
            **result,
        )
    except Exception as exc:
        job_store.fail(job_id, str(exc))
        raise HTTPException(status_code=500, detail=str(exc))


# ---------------------------------------------------------------------------
# Async pipeline run
# ---------------------------------------------------------------------------

@app.post("/api/v1/pipeline/run-async", tags=["Pipeline"])
async def run_pipeline_async(request: PipelineRunRequest, background_tasks: BackgroundTasks):
    """
    Start the pipeline in the background. Returns a job_id immediately.
    Poll /api/v1/pipeline/status/{job_id} to check progress.
    """
    job_id = str(uuid.uuid4())
    job_store.create(job_id, request.model_dump())

    background_tasks.add_task(_run_pipeline_background, job_id, request)

    return {
        "job_id": job_id,
        "status": "queued",
        "message": f"Pipeline queued. Poll /api/v1/jobs/{job_id} for status.",
        "status_url": f"/api/v1/jobs/{job_id}",
    }


# ---------------------------------------------------------------------------
# Job status
# ---------------------------------------------------------------------------

@app.get("/api/v1/jobs/{job_id}", response_model=JobStatusResponse, tags=["Jobs"])
async def get_job_status(job_id: str):
    """Get the current status and results of a pipeline job."""
    job = job_store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return JobStatusResponse(**job)


@app.get("/api/v1/jobs", response_model=JobListResponse, tags=["Jobs"])
async def list_jobs():
    """List all pipeline jobs (recent 50)."""
    jobs = job_store.list_all()
    return JobListResponse(jobs=jobs, total=len(jobs))


@app.delete("/api/v1/jobs/{job_id}", tags=["Jobs"])
async def cancel_job(job_id: str):
    """Cancel a running job (best-effort)."""
    job = job_store.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    if job["status"] in ("completed", "failed"):
        raise HTTPException(status_code=400, detail="Job already finished.")
    job_store.cancel(job_id)
    return {"job_id": job_id, "status": "cancelled"}


# ---------------------------------------------------------------------------
# Scenario endpoints
# ---------------------------------------------------------------------------

@app.get("/api/v1/scenarios", tags=["Scenarios"])
async def list_scenarios(scenarios_path: str = "scenarios.yaml"):
    """List all available business scenarios from scenarios.yaml."""
    path = Path(scenarios_path)
    if not path.exists():
        return {"scenarios": [], "message": "No scenarios.yaml found. Run pipeline first."}
    with open(path) as fh:
        data = yaml.safe_load(fh)
    scenarios = []
    for name, s in (data.get("scenarios") or {}).items():
        scenarios.append({
            "name": name,
            "description": s.get("description", ""),
            "domain": s.get("domain", ""),
        })
    return {"scenarios": scenarios, "total": len(scenarios)}


@app.post("/api/v1/scenarios/run", tags=["Scenarios"])
async def run_scenario(request: ScenarioRunRequest, background_tasks: BackgroundTasks):
    """
    Run a specific business scenario (always async).
    """
    job_id = str(uuid.uuid4())
    job_store.create(job_id, request.model_dump())

    background_tasks.add_task(_run_scenario_background, job_id, request)

    return {
        "job_id": job_id,
        "status": "queued",
        "scenario": request.scenario_name,
        "message": f"Scenario '{request.scenario_name}' queued.",
        "status_url": f"/api/v1/jobs/{job_id}",
    }


# ---------------------------------------------------------------------------
# Pipeline execution helpers (run in thread pool)
# ---------------------------------------------------------------------------

def _run_pipeline_sync(job_id: str, request: PipelineRunRequest) -> dict:
    """Blocking pipeline execution — runs in executor thread."""
    job_store.start(job_id)
    t0 = time.perf_counter()

    try:
        result = _execute_pipeline(request)
        elapsed = time.perf_counter() - t0
        result["elapsed_seconds"] = round(elapsed, 2)
        job_store.complete(job_id, result)
        return result
    except Exception as exc:
        job_store.fail(job_id, str(exc))
        raise


async def _run_pipeline_background(job_id: str, request: PipelineRunRequest):
    """Async wrapper for background task."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, _run_pipeline_sync, job_id, request)


async def _run_scenario_background(job_id: str, request: ScenarioRunRequest):
    """Async wrapper for scenario run."""
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, _run_scenario_sync, job_id, request)


def _run_scenario_sync(job_id: str, request: ScenarioRunRequest) -> dict:
    """Blocking scenario execution."""
    job_store.start(job_id)
    t0 = time.perf_counter()
    try:
        pipeline_req = PipelineRunRequest(
            database=request.database,
            # groq_api_key intentionally omitted — read from GROQ_API_KEY env var server-side
            model=request.model,
            dry_run=request.dry_run,
            scenario=request.scenario_name,
        )
        result = _execute_pipeline(pipeline_req)
        result["scenario"] = request.scenario_name
        result["elapsed_seconds"] = round(time.perf_counter() - t0, 2)
        job_store.complete(job_id, result)
        return result
    except Exception as exc:
        job_store.fail(job_id, str(exc))
        raise


def _execute_pipeline(request: PipelineRunRequest) -> dict:
    """
    Core pipeline execution — wires up LangGraph and runs all 7 nodes.

    Bug fixes:
      1. dry_run is now stored at the TOP LEVEL of config (not inside database dict)
         so pipeline_executor_node reads config.get("_dry_run") correctly.
      2. graph.invoke() in LangGraph returns a plain DICT, not a PipelineState object.
         We use dict .get() access instead of dot-attribute access on final_state.
    """
    import logging

    # Build config from request
    db_dict = request.database.model_dump()

    # FIX 1: _dry_run must sit at TOP LEVEL of the config dict.
    # pipeline_executor_node reads:  config.get("_dry_run")
    # where config = state.db_config (the whole config, not just the database section).
    # Previously _dry_run was placed inside db_dict so node 7 never saw it —
    # it always tried to load even when dry_run=true was sent.
    config = {
        "database": db_dict,
        "_dry_run": request.dry_run,           # <── top-level; node 7 reads this
        "_config_path": "config.yaml",
        "generation": {
            "batch_size":       request.batch_size or 10000,
            "null_probability": request.null_probability or 0.05,
            "output_dir":       request.output_dir or "./output",
            "seed":             request.seed or 42,
        },
        "loader": {
            "disable_fk_checks":    False,
            "disable_indexes":      True,
            "truncate_before_load": True,      # FIX 4: wipe rows before re-run → no duplicate PK
        },
        "logging": {
            "app_log":   "logs/app.log",
            "error_log": "logs/error.log",
            "audit_log": "logs/audit.log",
            "level":     "INFO",
        },
    }

    # Setup LLM client
    from Intelligence.llm_client import LLMClient
    from Intelligence.graph import build_graph
    from Intelligence.state import PipelineState

    # FIX 3: Groq API key is NEVER accepted from the client request.
    # It must be set server-side as the GROQ_API_KEY environment variable.
    # This keeps the key completely invisible to API consumers.
    api_key = os.environ.get("GROQ_API_KEY", "")
    if not api_key:
        raise RuntimeError(
            "API_KEY is not set on the server. "
            "Ask your administrator: export API_KEY=gsk_xxxx"
        )

    llm_client = LLMClient(api_key=api_key, model=request.model or "llama-3.3-70b-versatile")

    config["_config_path"] = "config.yaml"

    initial_state = PipelineState(
        db_url=_build_dsn(db_dict),
        db_config=config,
    )

    graph = build_graph(llm_client)

    # FIX 2: LangGraph graph.invoke() returns a plain DICT, not a PipelineState object.
    # Using dot-access (final_state.detected_domain) caused:
    #   AttributeError: 'dict' object has no attribute 'detected_domain'
    # All fields must be accessed with .get() on the raw dict.
    raw: dict = graph.invoke(initial_state)

    return {
        "detected_domain":      raw.get("detected_domain", "unknown"),
        "domain_confidence":    round(float(raw.get("domain_confidence") or 0), 4),
        "tables_processed":     len(raw.get("table_meta") or {}),
        "total_rows_generated": raw.get("total_rows_generated", 0),
        "llm_calls":            raw.get("llm_calls", 0),
        "scenarios_created":    list((raw.get("scenarios") or {}).keys()),
        "files_written": {
            "domains_yaml":   raw.get("domains_yaml_path", ""),
            "config_yaml":    raw.get("config_yaml_path", ""),
            "scenarios_yaml": raw.get("scenarios_yaml_path", ""),
        },
        "dry_run":       request.dry_run,
        "warnings":      raw.get("errors", []),
        "log_messages":  (raw.get("log_messages") or [])[-20:],
    }


def _build_dsn(cfg: dict) -> str:
    engine = cfg.get("engine", "postgres")
    if engine in ("postgres", "postgresql"):
        return (
            f"postgresql://{cfg['user']}:{cfg['password']}"
            f"@{cfg['host']}:{cfg.get('port', 5432)}/{cfg['dbname']}"
        )
    return f"{engine}://{cfg['user']}:{cfg['password']}@{cfg['host']}/{cfg['dbname']}"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info",
    )
# ---------------------------------------------------------------------------
# Startup: seed default connection
# ---------------------------------------------------------------------------

@app.on_event("startup")
async def seed_default_connection():
    """
    On startup, if config.yaml exists, register its database as
    the 'default' connection so the UI works immediately.
    """
    import yaml
    from pathlib import Path
    from api.connection_store import connection_store

    cfg_path = Path("config.yaml")

    if cfg_path.exists():
        with open(cfg_path) as f:
            cfg = yaml.safe_load(f)

        db = cfg.get("database", {})

        if db.get("host") and db.get("dbname"):
            name = db.get("dbname", "default")
            connection_store.add(name, db)
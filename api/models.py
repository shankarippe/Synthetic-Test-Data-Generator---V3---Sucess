"""
api/models.py
-------------
Pydantic request / response models for the FastAPI endpoints.
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator


# ---------------------------------------------------------------------------
# Database config model (engine-agnostic)
# ---------------------------------------------------------------------------

class DatabaseConfig(BaseModel):
    engine: str = Field(
        default="postgres",
        description="Database engine: postgres | oracle | sqlserver | mysql",
        examples=["postgres", "oracle", "sqlserver", "mysql"],
    )
    host: str = Field(..., description="Database host", examples=["localhost"])
    port: Optional[int] = Field(None, description="Database port (default varies by engine)")
    dbname: str = Field(..., description="Database / schema name", examples=["DatagenDB"])
    user: str = Field(..., description="Username", examples=["postgres"])
    password: str = Field(..., description="Password")
    schema: str = Field(default="public", description="Schema name (e.g. public, dbo, banking)")   #type: ignore

    # Oracle-specific
    service_name: Optional[str] = Field(None, description="Oracle service name (instead of dbname)")
    sid: Optional[str] = Field(None, description="Oracle SID (alternative to service_name)")

    # SQL Server-specific
    driver: Optional[str] = Field(
        None,
        description="ODBC driver name for SQL Server",
        examples=["ODBC Driver 18 for SQL Server"],
    )

    @field_validator("engine")
    @classmethod
    def validate_engine(cls, v: str) -> str:
        supported = {"postgres", "postgresql", "oracle", "oracledb", "sqlserver", "mssql", "mysql", "mariadb"}
        if v.lower() not in supported:
            raise ValueError(f"engine must be one of: {sorted(supported)}")
        return v.lower()

    def model_dump(self, **kwargs) -> dict:
        d = super().model_dump(**kwargs)
        d = {k: v for k, v in d.items() if v is not None}
        # Set default port by engine
        if "port" not in d:
            defaults = {"postgres": 5432, "oracle": 1521, "sqlserver": 1433, "mysql": 3306}
            engine_key = next((k for k in defaults if k in d.get("engine", "postgres")), "postgres")
            d["port"] = defaults[engine_key]
        return d


# ---------------------------------------------------------------------------
# Pipeline run request
# ---------------------------------------------------------------------------

class PipelineRunRequest(BaseModel):
    database: DatabaseConfig = Field(..., description="Database connection configuration")
    # groq_api_key is intentionally NOT in the request body.
    # The server reads it from the .env file. It never travels over the network.
    model: str = Field(
        default="llama-3.3-70b-versatile",
        description="Groq model to use for domain detection and schema analysis",
    )

    # Generation settings
    batch_size: int = Field(default=10000, ge=1000, le=100000, description="Rows per generation batch")
    null_probability: float = Field(default=0.05, ge=0.0, le=1.0, description="Probability of NULL for nullable columns")
    seed: int = Field(default=42, description="Random seed for reproducibility")
    output_dir: str = Field(default="./output", description="Directory for generated CSV files")

    # Run options
    dry_run: bool = Field(default=False, description="Generate CSVs but skip DB load")
    scenario: Optional[str] = Field(None, description="Run a specific business scenario")

    class Config:
        json_schema_extra = {
            "example": {
                "database": {
                    "engine": "postgres",
                    "host": "localhost",
                    "port": 5432,
                    "dbname": "DatagenDB",
                    "user": "postgres",
                    "password": "secret",
                    "schema": "banking",
                },
                "model": "llama-3.3-70b-versatile",
                "batch_size": 10000,
                "null_probability": 0.05,
                "seed": 42,
                "dry_run": False,
            }
        }


# ---------------------------------------------------------------------------
# Pipeline run response
# ---------------------------------------------------------------------------

class PipelineRunResponse(BaseModel):
    job_id: str
    status: str
    detected_domain: Optional[str] = None
    domain_confidence: Optional[float] = None
    tables_processed: Optional[int] = None
    total_rows_generated: Optional[int] = None
    llm_calls: Optional[int] = None
    scenarios_created: Optional[List[str]] = None
    files_written: Optional[Dict[str, str]] = None
    elapsed_seconds: Optional[float] = None
    warnings: Optional[List[str]] = None
    log_messages: Optional[List[str]] = None


# ---------------------------------------------------------------------------
# Job models
# ---------------------------------------------------------------------------

class JobStatusResponse(BaseModel):
    job_id: str
    status: str  # queued | running | completed | failed | cancelled
    created_at: str
    started_at: Optional[str] = None
    completed_at: Optional[str] = None
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    request_summary: Optional[Dict[str, Any]] = None


class JobListResponse(BaseModel):
    jobs: List[Dict[str, Any]]
    total: int


# ---------------------------------------------------------------------------
# Schema read
# ---------------------------------------------------------------------------

class SchemaReadRequest(BaseModel):
    database: DatabaseConfig

    class Config:
        json_schema_extra = {
            "example": {
                "database": {
                    "engine": "postgres",
                    "host": "localhost",
                    "port": 5432,
                    "dbname": "DatagenDB",
                    "user": "postgres",
                    "password": "secret",
                    "schema": "banking",
                }
            }
        }


class SchemaReadResponse(BaseModel):
    engine: str
    schema: str  #type: ignore
    table_count: int
    tables: List[Dict[str, Any]]


# ---------------------------------------------------------------------------
# Connection test
# ---------------------------------------------------------------------------

class ConnectionTestRequest(BaseModel):
    database: DatabaseConfig

    class Config:
        json_schema_extra = {
            "example": {
                "database": {
                    "engine": "postgres",
                    "host": "localhost",
                    "port": 5432,
                    "dbname": "DatagenDB",
                    "user": "postgres",
                    "password": "secret",
                    "schema": "banking",
                }
            }
        }


class ConnectionTestResponse(BaseModel):
    success: bool
    engine: str
    message: str


# ---------------------------------------------------------------------------
# Scenario run
# ---------------------------------------------------------------------------

class ScenarioRunRequest(BaseModel):
    database: DatabaseConfig
    scenario_name: str = Field(..., description="Name of scenario from scenarios.yaml")
    # groq_api_key not accepted in requests — server reads from .env
    model: str = "llama-3.3-70b-versatile"
    dry_run: bool = False
    seed: Optional[int] = None

    class Config:
        json_schema_extra = {
            "example": {
                "database": {
                    "engine": "postgres",
                    "host": "localhost",
                    "port": 5432,
                    "dbname": "DatagenDB",
                    "user": "postgres",
                    "password": "secret",
                    "schema": "banking",
                },
                "scenario_name": "stress_test",
                "dry_run": False,
            }
        }


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

class HealthResponse(BaseModel):
    status: str
    version: str
    timestamp: str
    supported_engines: List[str]
    groq_key_configured: bool = Field(
        default=False,
        description="True if GROQ_API_KEY is set on the server. Pipeline endpoints will fail if False.",
    )

class DialectResponse(BaseModel):
    dialect: str        # pgsql | tsql | plsql | mysql
    label: str          # "PostgreSQL", "T-SQL (SQL Server)", etc.
    placeholder: str    # %s | ? | :1
    engine: str
    schema: str       #type: ignore
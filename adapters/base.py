"""
adapters/base.py
----------------
Abstract base class for all database adapters.
Each adapter implements schema reading + bulk loading for one DB engine.

Supported engines (add more by subclassing BaseDBAdapter):
  - PostgreSQL  → adapters/postgres.py
  - Oracle DB   → adapters/oracle.py
  - SQL Server  → adapters/sqlserver.py
  - MySQL       → adapters/mysql.py
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any


# ---------------------------------------------------------------------------
# Shared data structures (engine-agnostic)
# ---------------------------------------------------------------------------

@dataclass
class ColumnMeta:
    name: str
    data_type: str
    udt_name: str
    is_nullable: bool
    character_maximum_length: int | None
    numeric_precision: int | None
    numeric_scale: int | None
    ordinal_position: int


@dataclass
class ForeignKeyMeta:
    constraint_name: str
    column: str
    ref_table: str
    ref_column: str


@dataclass
class TableMeta:
    schema: str
    name: str
    columns: list[ColumnMeta] = field(default_factory=list)
    primary_keys: list[str] = field(default_factory=list)
    foreign_keys: list[ForeignKeyMeta] = field(default_factory=list)

    @property
    def column_map(self) -> dict[str, ColumnMeta]:
        return {c.name: c for c in self.columns}


# ---------------------------------------------------------------------------
# Abstract adapter
# ---------------------------------------------------------------------------

class BaseDBAdapter(ABC):
    """
    All database adapters must implement these three methods.
    """

    def __init__(self, config: dict, loggers: dict):
        self.db_cfg = config.get("database", config)
        self.schema = self.db_cfg.get("schema", "public")
        self.log = loggers["app"]
        self.err_log = loggers["error"]

    @abstractmethod
    def read_all(self) -> dict[str, TableMeta]:
        """Read full schema metadata. Returns table_name → TableMeta."""
        ...

    @abstractmethod
    def bulk_load(
        self,
        table_name: str,
        csv_path: str,
        column_names: list[str],
    ) -> int:
        """
        Bulk-load a CSV file into the target table.
        Returns the number of rows loaded.
        """
        ...

    @abstractmethod
    def test_connection(self) -> bool:
        """Return True if connection is successful."""
        ...

    def get_engine_name(self) -> str:
        return self.__class__.__name__.replace("Adapter", "").lower()
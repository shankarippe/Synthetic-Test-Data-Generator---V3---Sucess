"""
core/export_engine.py
---------------------
Exports table data as:
  - DML  : INSERT INTO statements (engine-aware quoting)
  - CSV  : standard comma-separated
  - DDL  : CREATE TABLE statement reconstructed from metadata

Engine-aware quoting:
  postgres   → "schema"."table", $1/$2 (or VALUES literals)
  oracle     → "SCHEMA"."TABLE"
  sqlserver  → [schema].[table]
  mysql      → `schema`.`table`
"""

from __future__ import annotations
from datetime import date, datetime
from decimal import Decimal
from typing import Any


class ExportEngine:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.engine = db_config.get("engine", "postgres").lower()
        self.schema = db_config.get("schema", "public")

    # ── DML export ────────────────────────────────────────────────────

    def export_dml(self, table_name: str, rows: list[dict], columns: list[str]) -> str:
        """Generate INSERT INTO statements for given rows."""
        qt = self._quote_table(table_name)
        qcols = ", ".join(self._quote_col(c) for c in columns)
        lines = [f"-- DML export: {table_name}  ({len(rows)} rows)", ""]
        for row in rows:
            vals = ", ".join(self._literal(row.get(c)) for c in columns)
            lines.append(f"INSERT INTO {qt} ({qcols}) VALUES ({vals});")
        return "\n".join(lines)

    def export_ddl(self, table_name: str, table_meta) -> str:
        """Reconstruct a CREATE TABLE statement from TableMeta."""
        qt = self._quote_table(table_name)
        col_defs = []
        for col in table_meta.columns:
            nullable = "" if col.is_nullable else " NOT NULL"
            pk = " PRIMARY KEY" if len(table_meta.primary_keys) == 1 and col.name in table_meta.primary_keys else ""
            col_defs.append(f"  {self._quote_col(col.name)} {col.data_type.upper()}{nullable}{pk}")

        if len(table_meta.primary_keys) > 1:
            pk_cols = ", ".join(self._quote_col(c) for c in table_meta.primary_keys)
            col_defs.append(f"  PRIMARY KEY ({pk_cols})")

        lines = [f"CREATE TABLE {qt} ("] + [",\n".join(col_defs)] + [");"]
        return "\n".join(lines)

    def export_csv(self, rows: list[dict], columns: list[str]) -> str:
        """Export rows as CSV string."""
        import csv, io
        buf = io.StringIO()
        w = csv.writer(buf)
        w.writerow(columns)
        for row in rows:
            w.writerow([row.get(c) for c in columns])
        return buf.getvalue()

    # ── Quoting helpers ───────────────────────────────────────────────

    def _quote_table(self, table: str) -> str:
        s, t = self.schema, table
        if self.engine in ("postgres", "postgresql"):
            return f'"{s}"."{t}"'
        elif self.engine in ("oracle", "oracledb"):
            return f'"{s.upper()}"."{t.upper()}"'
        elif self.engine in ("sqlserver", "mssql"):
            return f"[{s}].[{t}]"
        elif self.engine in ("mysql", "mariadb"):
            return f"`{s}`.`{t}`"
        return f'"{s}"."{t}"'

    def _quote_col(self, col: str) -> str:
        if self.engine in ("sqlserver", "mssql"):
            return f"[{col}]"
        elif self.engine in ("mysql", "mariadb"):
            return f"`{col}`"
        return f'"{col}"'

    def _literal(self, val: Any) -> str:
        if val is None:
            return "NULL"
        if isinstance(val, bool):
            return "TRUE" if val else "FALSE"
        if isinstance(val, (int, float, Decimal)):
            return str(val)
        if isinstance(val, (date, datetime)):
            return f"'{val.isoformat()}'"
        s = str(val).replace("'", "''")
        return f"'{s}'"
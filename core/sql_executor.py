"""
core/sql_executor.py
--------------------
Engine-aware SQL execution layer.
- Runs SELECT, DML (INSERT/UPDATE/DELETE), DDL (CREATE/ALTER/DROP)
- Returns results as list-of-dicts
- Adapts parameter placeholder style per engine
- Returns dialect metadata so the UI can configure its SQL editor

Dialect map:
  postgres   → psycopg3,    placeholder: %s,  dialect: "pgsql"
  oracle     → oracledb,    placeholder: :1,  dialect: "plsql"
  sqlserver  → pyodbc,      placeholder: ?,   dialect: "tsql"
  mysql      → connector,   placeholder: %s,  dialect: "mysql"
"""

from __future__ import annotations
import logging
from typing import Any

import psycopg
import oracledb
from adapters.sqlserver import SQLServerAdapter
from adapters.mysql import MySQLAdapter

logger = logging.getLogger("app")

DIALECT_INFO = {
    "postgres":   {"dialect": "pgsql",  "placeholder": "%s", "label": "PostgreSQL"},
    "postgresql": {"dialect": "pgsql",  "placeholder": "%s", "label": "PostgreSQL"},
    "oracle":     {"dialect": "plsql",  "placeholder": ":1", "label": "Oracle PL/SQL"},
    "oracledb":   {"dialect": "plsql",  "placeholder": ":1", "label": "Oracle PL/SQL"},
    "sqlserver":  {"dialect": "tsql",   "placeholder": "?",  "label": "T-SQL (SQL Server)"},
    "mssql":      {"dialect": "tsql",   "placeholder": "?",  "label": "T-SQL (SQL Server)"},
    "mysql":      {"dialect": "mysql",  "placeholder": "%s", "label": "MySQL / MariaDB"},
    "mariadb":    {"dialect": "mysql",  "placeholder": "%s", "label": "MySQL / MariaDB"},
}


class SQLExecutor:
    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.engine = db_config.get("engine", "postgres").lower()

    def get_dialect(self) -> dict:
        return DIALECT_INFO.get(self.engine, {"dialect": "pgsql", "placeholder": "%s", "label": "SQL"})

    def execute(self, sql: str, params: list | None = None, max_rows: int = 1000) -> dict:
        """
        Execute any SQL statement.
        Returns:
          {
            "columns": [...],
            "rows": [[...], ...],
            "row_count": int,       # rows affected for DML
            "statement_type": "SELECT" | "DML" | "DDL",
            "truncated": bool,
            "error": str | None
          }
        """
        sql_upper = sql.strip().upper().lstrip("(")
        if sql_upper.startswith("SELECT") or sql_upper.startswith("WITH"):
            return self._execute_select(sql, params, max_rows)
        elif any(sql_upper.startswith(k) for k in ("INSERT", "UPDATE", "DELETE", "MERGE")):
            return self._execute_dml(sql, params)
        else:
            return self._execute_ddl(sql)

    # ── PostgreSQL ────────────────────────────────────────────────────

    def _pg_conn(self):
        c = self.db_config
        return psycopg.connect(
            f"host={c['host']} port={c.get('port',5432)} "
            f"dbname={c['dbname']} user={c['user']} password={c['password']}"
        )

    def _execute_select(self, sql: str, params, max_rows: int) -> dict:
        if self.engine in ("postgres", "postgresql"):
            return self._pg_select(sql, params, max_rows)
        elif self.engine in ("oracle", "oracledb"):
            return self._ora_select(sql, params, max_rows)
        elif self.engine in ("sqlserver", "mssql"):
            return self._mssql_select(sql, params, max_rows)
        elif self.engine in ("mysql", "mariadb"):
            return self._mysql_select(sql, params, max_rows)
        return {"error": f"Unknown engine: {self.engine}", "columns": [], "rows": []}

    def _pg_select(self, sql, params, max_rows):
        try:
            with self._pg_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params or [])
                    cols = [d.name for d in cur.description] if cur.description else []
                    rows = cur.fetchmany(max_rows)
                    total = cur.rowcount
                    serialized = [list(self._serialize_row(r)) for r in rows]
                    return {
                        "columns": cols, "rows": serialized,
                        "row_count": len(rows), "statement_type": "SELECT",
                        "truncated": len(rows) == max_rows, "error": None
                    }
        except Exception as e:
            return {"error": str(e), "columns": [], "rows": [], "statement_type": "SELECT"}

    def _ora_select(self, sql, params, max_rows):
        try:
            c = self.db_config
            dsn = oracledb.makedsn(c["host"], c.get("port", 1521), service_name=c.get("service_name", c.get("dbname")))  #type: ignore
            with oracledb.connect(user=c["user"], password=c["password"], dsn=dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, params or [])
                    cols = [d[0] for d in cur.description] if cur.description else []
                    rows = cur.fetchmany(max_rows)
                    return {
                        "columns": cols,
                        "rows": [list(self._serialize_row(r)) for r in rows],
                        "row_count": len(rows), "statement_type": "SELECT",
                        "truncated": len(rows) == max_rows, "error": None
                    }
        except Exception as e:
            return {"error": str(e), "columns": [], "rows": [], "statement_type": "SELECT"}

    def _mssql_select(self, sql, params, max_rows):
        try:
            from adapters.sqlserver import SQLServerAdapter
            conn = SQLServerAdapter({"database": self.db_config}, {"app": logger, "error": logger})._get_conn()
            cur = conn.cursor()
            cur.execute(sql, params or [])   #type: ignore
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchmany(max_rows)
            conn.close()
            return {
                "columns": cols,
                "rows": [list(self._serialize_row(r)) for r in rows],
                "row_count": len(rows), "statement_type": "SELECT",
                "truncated": len(rows) == max_rows, "error": None
            }
        except Exception as e:
            return {"error": str(e), "columns": [], "rows": [], "statement_type": "SELECT"}

    def _mysql_select(self, sql, params, max_rows):
        try:
            from adapters.mysql import MySQLAdapter
            conn = MySQLAdapter({"database": self.db_config}, {"app": logger, "error": logger})._get_conn()
            cur = conn.cursor()
            cur.execute(sql, params or [])
            cols = [d[0] for d in cur.description] if cur.description else []
            rows = cur.fetchmany(max_rows)
            conn.close()
            return {
                "columns": cols,
                "rows": [list(self._serialize_row(r)) for r in rows],
                "row_count": len(rows), "statement_type": "SELECT",
                "truncated": len(rows) == max_rows, "error": None
            }
        except Exception as e:
            return {"error": str(e), "columns": [], "rows": [], "statement_type": "SELECT"}

    def _execute_dml(self, sql, params):
        try:
            if self.engine in ("postgres", "postgresql"):
                with self._pg_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql, params or [])
                    conn.commit()
                    return {"columns": [], "rows": [], "row_count": cur.rowcount, "statement_type": "DML", "error": None}
            # Oracle / MSSQL / MySQL: re-use adapter connections
            adapter_map = {
                "oracle": ("adapters.oracle", "OracleAdapter"),
                "oracledb": ("adapters.oracle", "OracleAdapter"),
                "sqlserver": ("adapters.sqlserver", "SQLServerAdapter"),
                "mssql": ("adapters.sqlserver", "SQLServerAdapter"),
                "mysql": ("adapters.mysql", "MySQLAdapter"),
                "mariadb": ("adapters.mysql", "MySQLAdapter"),
            }
            mod_name, cls_name = adapter_map[self.engine]
            import importlib
            mod = importlib.import_module(mod_name)
            adapter = getattr(mod, cls_name)({"database": self.db_config}, {"app": logger, "error": logger})
            conn = adapter._get_conn()
            cur = conn.cursor()
            cur.execute(sql, params or [])
            conn.commit()
            rc = cur.rowcount
            conn.close()
            return {"columns": [], "rows": [], "row_count": rc, "statement_type": "DML", "error": None}
        except Exception as e:
            return {"error": str(e), "columns": [], "rows": [], "statement_type": "DML"}

    def _execute_ddl(self, sql):
        try:
            if self.engine in ("postgres", "postgresql"):
                with self._pg_conn() as conn:
                    with conn.cursor() as cur:
                        cur.execute(sql)
                    conn.commit()
                return {"columns": [], "rows": [], "row_count": 0, "statement_type": "DDL", "error": None}
            # Same pattern as DML for other engines
            return self._execute_dml(sql, None)
        except Exception as e:
            return {"error": str(e), "columns": [], "rows": [], "statement_type": "DDL"}

    @staticmethod
    def _serialize_row(row):
        from datetime import date, datetime
        from decimal import Decimal
        result = []
        for v in row:
            if v is None:
                result.append(None)
            elif isinstance(v, (date, datetime)):
                result.append(v.isoformat())
            elif isinstance(v, Decimal):
                result.append(float(v))
            elif isinstance(v, bytes):
                result.append(v.hex())
            else:
                result.append(v)
        return result
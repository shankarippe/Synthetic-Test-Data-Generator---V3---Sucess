"""
adapters/sqlserver.py
---------------------
Microsoft SQL Server adapter using pyodbc or pymssql.

Install: pip install pyodbc   (preferred, needs ODBC driver 17/18)
     OR: pip install pymssql  (pure Python fallback)

Config fields:
  host, port (default 1433), dbname, user, password, schema (default dbo)
  driver: "ODBC Driver 18 for SQL Server"  (optional, auto-detected)

SQL Server type mapping:
  int, bigint, smallint, tinyint → int4 / int8
  decimal, numeric, money        → numeric
  float, real                    → float8
  varchar, nvarchar, char, nchar → varchar / bpchar
  text, ntext                    → text
  datetime, datetime2            → timestamp
  date                           → date
  bit                            → bool
  uniqueidentifier                → uuid
  varbinary, binary, image       → bytea
  xml                            → text
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from adapters.base import BaseDBAdapter, TableMeta, ColumnMeta, ForeignKeyMeta

logger = logging.getLogger("app")

_MSSQL_TYPE_MAP = {
    "int": "int4",
    "bigint": "int8",
    "smallint": "int2",
    "tinyint": "int2",
    "decimal": "numeric",
    "numeric": "numeric",
    "money": "numeric",
    "smallmoney": "numeric",
    "float": "float8",
    "real": "float4",
    "bit": "bool",
    "varchar": "varchar",
    "nvarchar": "varchar",
    "char": "bpchar",
    "nchar": "bpchar",
    "text": "text",
    "ntext": "text",
    "datetime": "timestamp",
    "datetime2": "timestamp",
    "smalldatetime": "timestamp",
    "date": "date",
    "time": "time",
    "uniqueidentifier": "uuid",
    "varbinary": "bytea",
    "binary": "bytea",
    "image": "bytea",
    "xml": "text",
    "sql_variant": "text",
}


class SQLServerAdapter(BaseDBAdapter):
    """SQL Server adapter — tries pyodbc first, falls back to pymssql."""

    def _get_conn(self):
        c = self.db_cfg
        # Try pyodbc first
        try:
            import pyodbc
            driver = c.get("driver", "ODBC Driver 18 for SQL Server")
            conn_str = (
                f"DRIVER={{{driver}}};"
                f"SERVER={c['host']},{c.get('port', 1433)};"
                f"DATABASE={c['dbname']};"
                f"UID={c['user']};PWD={c['password']};"
                "TrustServerCertificate=yes;"
            )
            return pyodbc.connect(conn_str)
        except ImportError:
            pass
        # Fallback: pymssql
        try:
            import pymssql
            return pymssql.connect(
                server=c["host"],
                port=c.get("port", 1433),
                database=c["dbname"],
                user=c["user"],
                password=c["password"],
            )
        except ImportError:
            raise ImportError(
                "No SQL Server driver found. Run: pip install pyodbc  or  pip install pymssql"
            )

    def test_connection(self) -> bool:
        try:
            with self._get_conn() as conn:
                return True
        except Exception as exc:
            self.log.error("SQL Server connection failed: %s", exc)
            return False

    def read_all(self) -> dict[str, TableMeta]:
        schema = self.db_cfg.get("schema", "dbo")
        with self._get_conn() as conn:
            tables = self._fetch_tables(conn, schema)
            columns = self._fetch_columns(conn, schema)
            pks = self._fetch_primary_keys(conn, schema)
            fks = self._fetch_foreign_keys(conn, schema)

        meta: dict[str, TableMeta] = {}
        for tbl in tables:
            tm = TableMeta(schema=schema, name=tbl)
            tm.columns = columns.get(tbl, [])
            tm.primary_keys = pks.get(tbl, [])
            tm.foreign_keys = fks.get(tbl, [])
            meta[tbl] = tm
        self.log.info("SQL Server: discovered %d tables in schema '%s'", len(meta), schema)
        return meta

    def bulk_load(self, table_name: str, csv_path: str, column_names: list[str]) -> int:
        """
        SQL Server bulk insert using BULK INSERT or executemany fallback.
        For best performance, use BCP utility or BULK INSERT with a shared path.
        Here we use executemany for portability.
        """
        schema = self.db_cfg.get("schema", "dbo")
        qualified = f"[{schema}].[{table_name}]"
        placeholders = ", ".join("?" for _ in column_names)
        cols_sql = ", ".join(f"[{c}]" for c in column_names)
        insert_sql = f"INSERT INTO {qualified} ({cols_sql}) VALUES ({placeholders})"

        rows_loaded = 0
        batch_size = 1000  # MSSQL executemany is less efficient; keep batches smaller

        with self._get_conn() as conn:
            cursor = conn.cursor()
            if hasattr(cursor, 'fast_executemany'):
                cursor.fast_executemany = True  # type: ignore  # pyodbc speedup
            with open(csv_path, newline="", encoding="utf-8") as fh:
                reader = csv.DictReader(fh)
                batch = []
                for row in reader:
                    values = tuple(row.get(c) or None for c in column_names)
                    batch.append(values)
                    if len(batch) >= batch_size:
                        cursor.executemany(insert_sql, batch)
                        conn.commit()
                        rows_loaded += len(batch)
                        batch = []
                if batch:
                    cursor.executemany(insert_sql, batch)
                    conn.commit()
                    rows_loaded += len(batch)

        self.log.info("SQL Server loaded %d rows into %s", rows_loaded, qualified)
        return rows_loaded

    # ── Private helpers ───────────────────────────────────────────────

    def _fetch_tables(self, conn, schema: str) -> list[str]:
        sql = """
            SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = ? AND TABLE_TYPE = 'BASE TABLE'
            ORDER BY TABLE_NAME
        """
        cur = conn.cursor()
        cur.execute(sql, (schema,))
        return [r[0] for r in cur.fetchall()]

    def _fetch_columns(self, conn, schema: str) -> dict[str, list[ColumnMeta]]:
        sql = """
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                   CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = ?
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """
        result: dict[str, list[ColumnMeta]] = {}
        cur = conn.cursor()
        cur.execute(sql, (schema,))
        for row in cur.fetchall():
            tbl, col, dtype, nullable, max_len, num_prec, num_scale, pos = row
            udt = _MSSQL_TYPE_MAP.get(dtype.lower(), "varchar")
            result.setdefault(tbl, []).append(ColumnMeta(
                name=col, data_type=dtype, udt_name=udt,
                is_nullable=(nullable == "YES"),
                character_maximum_length=max_len,
                numeric_precision=num_prec,
                numeric_scale=num_scale,
                ordinal_position=pos,
            ))
        return result

    def _fetch_primary_keys(self, conn, schema: str) -> dict[str, list[str]]:
        sql = """
            SELECT KCU.TABLE_NAME, KCU.COLUMN_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU
                ON TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME AND TC.TABLE_SCHEMA = KCU.TABLE_SCHEMA
            WHERE TC.CONSTRAINT_TYPE = 'PRIMARY KEY' AND TC.TABLE_SCHEMA = ?
            ORDER BY KCU.TABLE_NAME, KCU.ORDINAL_POSITION
        """
        result: dict[str, list[str]] = {}
        cur = conn.cursor()
        cur.execute(sql, (schema,))
        for tbl, col in cur.fetchall():
            result.setdefault(tbl, []).append(col)
        return result

    def _fetch_foreign_keys(self, conn, schema: str) -> dict[str, list[ForeignKeyMeta]]:
        sql = """
            SELECT
                TC.TABLE_NAME, TC.CONSTRAINT_NAME, KCU.COLUMN_NAME,
                CCU.TABLE_NAME AS REF_TABLE, CCU.COLUMN_NAME AS REF_COLUMN
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS TC
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE KCU
                ON TC.CONSTRAINT_NAME = KCU.CONSTRAINT_NAME AND TC.TABLE_SCHEMA = KCU.TABLE_SCHEMA
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE CCU
                ON TC.CONSTRAINT_NAME = CCU.CONSTRAINT_NAME AND TC.TABLE_SCHEMA = CCU.TABLE_SCHEMA
            WHERE TC.CONSTRAINT_TYPE = 'FOREIGN KEY' AND TC.TABLE_SCHEMA = ?
            ORDER BY TC.TABLE_NAME, TC.CONSTRAINT_NAME
        """
        result: dict[str, list[ForeignKeyMeta]] = {}
        cur = conn.cursor()
        cur.execute(sql, (schema,))
        for fk_tbl, cname, fk_col, ref_tbl, ref_col in cur.fetchall():
            result.setdefault(fk_tbl, []).append(ForeignKeyMeta(
                constraint_name=cname, column=fk_col,
                ref_table=ref_tbl, ref_column=ref_col,
            ))
        return result
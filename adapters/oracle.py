"""
adapters/oracle.py
------------------
Oracle Database adapter using cx_Oracle / oracledb.

Install: pip install oracledb

DSN config fields:
  host, port, service_name (or sid), user, password, schema

Oracle type mapping to internal udt_name equivalents:
  NUMBER         → numeric
  VARCHAR2       → varchar
  CHAR           → bpchar
  DATE           → date
  TIMESTAMP      → timestamp
  CLOB/NCLOB     → text
  BLOB           → bytea
  INTEGER        → int4
  FLOAT          → float8
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from adapters.base import BaseDBAdapter, TableMeta, ColumnMeta, ForeignKeyMeta

logger = logging.getLogger("app")

# Oracle → internal udt_name mapping
_ORACLE_TYPE_MAP = {
    "NUMBER": "numeric",
    "INTEGER": "int4",
    "FLOAT": "float8",
    "BINARY_FLOAT": "float4",
    "BINARY_DOUBLE": "float8",
    "VARCHAR2": "varchar",
    "NVARCHAR2": "varchar",
    "CHAR": "bpchar",
    "NCHAR": "bpchar",
    "DATE": "timestamp",          # Oracle DATE includes time component
    "TIMESTAMP": "timestamp",
    "CLOB": "text",
    "NCLOB": "text",
    "BLOB": "bytea",
    "RAW": "bytea",
    "LONG": "text",
    "XMLTYPE": "text",
}


class OracleAdapter(BaseDBAdapter):
    """Oracle DB adapter using python-oracledb (thin mode, no Oracle Client needed)."""

    def _get_conn(self):
        try:
            import oracledb
        except ImportError:
            raise ImportError(
                "oracledb not installed. Run: pip install oracledb"
            )
        c = self.db_cfg
        # Support both service_name and sid
        if "service_name" in c:
            dsn = oracledb.makedsn(c["host"], c.get("port", 1521), service_name=c["service_name"])
        elif "sid" in c:
            dsn = oracledb.makedsn(c["host"], c.get("port", 1521), sid=c["sid"])
        else:
            # Try tns-style if 'dsn' key provided
            dsn = c.get("dsn", f"{c['host']}:{c.get('port',1521)}/{c.get('dbname','')}")

        return oracledb.connect(user=c["user"], password=c["password"], dsn=dsn)

    def test_connection(self) -> bool:
        try:
            with self._get_conn() as conn:
                return True
        except Exception as exc:
            self.log.error("Oracle connection failed: %s", exc)
            return False

    def read_all(self) -> dict[str, TableMeta]:
        schema_owner = self.db_cfg.get("schema", self.db_cfg["user"]).upper()

        with self._get_conn() as conn:
            tables = self._fetch_tables(conn, schema_owner)
            columns = self._fetch_columns(conn, schema_owner)
            pks = self._fetch_primary_keys(conn, schema_owner)
            fks = self._fetch_foreign_keys(conn, schema_owner)

        meta: dict[str, TableMeta] = {}
        for tbl in tables:
            tm = TableMeta(schema=schema_owner, name=tbl)
            tm.columns = columns.get(tbl, [])
            tm.primary_keys = pks.get(tbl, [])
            tm.foreign_keys = fks.get(tbl, [])
            meta[tbl] = tm
        self.log.info("Oracle: discovered %d tables in schema '%s'", len(meta), schema_owner)
        return meta

    def bulk_load(self, table_name: str, csv_path: str, column_names: list[str]) -> int:
        """
        Oracle bulk insert using executemany with arraysize batching.
        Oracle doesn't support COPY protocol — we use prepared statements.
        """
        import oracledb

        schema_owner = self.db_cfg.get("schema", self.db_cfg["user"]).upper()
        qualified = f'"{schema_owner}"."{table_name}"'
        placeholders = ", ".join(f":{i+1}" for i in range(len(column_names)))
        cols_sql = ", ".join(f'"{c}"' for c in column_names)
        insert_sql = f"INSERT INTO {qualified} ({cols_sql}) VALUES ({placeholders})"

        rows_loaded = 0
        batch_size = 5000

        with self._get_conn() as conn:
            with conn.cursor() as cur:
                cur.arraysize = batch_size
                with open(csv_path, newline="", encoding="utf-8") as fh:
                    reader = csv.DictReader(fh)
                    batch = []
                    for row in reader:
                        values = tuple(row.get(c) or None for c in column_names)
                        batch.append(values)
                        if len(batch) >= batch_size:
                            cur.executemany(insert_sql, batch)
                            rows_loaded += len(batch)
                            batch = []
                    if batch:
                        cur.executemany(insert_sql, batch)
                        rows_loaded += len(batch)
            conn.commit()

        self.log.info("Oracle loaded %d rows into %s", rows_loaded, qualified)
        return rows_loaded

    # ── Private helpers ───────────────────────────────────────────────

    def _fetch_tables(self, conn, owner: str) -> list[str]:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT table_name FROM all_tables WHERE owner = :1 ORDER BY table_name",
                (owner,)
            )
            return [r[0] for r in cur.fetchall()]

    def _fetch_columns(self, conn, owner: str) -> dict[str, list[ColumnMeta]]:
        sql = """
            SELECT table_name, column_name, data_type, nullable,
                   char_length, data_precision, data_scale, column_id
            FROM all_tab_columns
            WHERE owner = :1
            ORDER BY table_name, column_id
        """
        result: dict[str, list[ColumnMeta]] = {}
        with conn.cursor() as cur:
            cur.execute(sql, (owner,))
            for row in cur.fetchall():
                tbl, col, dtype, nullable, max_len, num_prec, num_scale, pos = row
                udt = _ORACLE_TYPE_MAP.get(dtype.split("(")[0].split(" ")[0].upper(), "varchar")
                result.setdefault(tbl, []).append(ColumnMeta(
                    name=col, data_type=dtype, udt_name=udt,
                    is_nullable=(nullable == "Y"),
                    character_maximum_length=max_len,
                    numeric_precision=num_prec,
                    numeric_scale=num_scale,
                    ordinal_position=pos,
                ))
        return result

    def _fetch_primary_keys(self, conn, owner: str) -> dict[str, list[str]]:
        sql = """
            SELECT cc.table_name, cc.column_name
            FROM all_constraints c
            JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name AND c.owner = cc.owner
            WHERE c.constraint_type = 'P' AND c.owner = :1
            ORDER BY cc.table_name, cc.position
        """
        result: dict[str, list[str]] = {}
        with conn.cursor() as cur:
            cur.execute(sql, (owner,))
            for tbl, col in cur.fetchall():
                result.setdefault(tbl, []).append(col)
        return result

    def _fetch_foreign_keys(self, conn, owner: str) -> dict[str, list[ForeignKeyMeta]]:
        sql = """
            SELECT c.table_name, c.constraint_name, cc.column_name,
                   rc.table_name AS ref_table, rcc.column_name AS ref_column
            FROM all_constraints c
            JOIN all_cons_columns cc ON c.constraint_name = cc.constraint_name AND c.owner = cc.owner
            JOIN all_constraints rc ON c.r_constraint_name = rc.constraint_name AND rc.owner = c.owner
            JOIN all_cons_columns rcc ON rc.constraint_name = rcc.constraint_name AND rcc.owner = rc.owner
                AND cc.position = rcc.position
            WHERE c.constraint_type = 'R' AND c.owner = :1
            ORDER BY c.table_name, c.constraint_name
        """
        result: dict[str, list[ForeignKeyMeta]] = {}
        with conn.cursor() as cur:
            cur.execute(sql, (owner,))
            for fk_tbl, cname, fk_col, ref_tbl, ref_col in cur.fetchall():
                result.setdefault(fk_tbl, []).append(ForeignKeyMeta(
                    constraint_name=cname, column=fk_col,
                    ref_table=ref_tbl, ref_column=ref_col,
                ))
        return result
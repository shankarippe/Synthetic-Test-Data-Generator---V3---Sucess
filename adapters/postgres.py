"""
adapters/postgres.py
--------------------
PostgreSQL adapter — wraps existing DBMetadataReader + PostgresLoader logic
behind the BaseDBAdapter interface.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from adapters.base import BaseDBAdapter, TableMeta, ColumnMeta, ForeignKeyMeta

logger = logging.getLogger("app")


class PostgresAdapter(BaseDBAdapter):
    """PostgreSQL adapter using psycopg3."""

    def _dsn(self) -> str:
        c = self.db_cfg
        return (
            f"host={c['host']} port={c.get('port', 5432)} "
            f"dbname={c['dbname']} user={c['user']} password={c['password']}"
        )

    def test_connection(self) -> bool:
        try:
            import psycopg
            with psycopg.connect(self._dsn()):
                return True
        except Exception as exc:
            self.log.error("PostgreSQL connection failed: %s", exc)
            return False

    def read_all(self) -> dict[str, TableMeta]:
        import psycopg
        with psycopg.connect(self._dsn()) as conn:
            tables = self._fetch_tables(conn)
            columns = self._fetch_columns(conn)
            pks = self._fetch_primary_keys(conn)
            fks = self._fetch_foreign_keys(conn)

        meta: dict[str, TableMeta] = {}
        for tbl_name in tables:
            tm = TableMeta(schema=self.schema, name=tbl_name)
            tm.columns = columns.get(tbl_name, [])
            tm.primary_keys = pks.get(tbl_name, [])
            tm.foreign_keys = fks.get(tbl_name, [])
            meta[tbl_name] = tm
        return meta

    def bulk_load(self, table_name: str, csv_path: str, column_names: list[str]) -> int:
        """
        FIX — Duplicate PK on re-run
        ─────────────────────────────
        Every run generates PKs starting from 1.  If the table already
        has rows from a previous run, COPY fails with:
            duplicate key value violates unique constraint
        Solution: TRUNCATE the table before loading.
        TRUNCATE … RESTART IDENTITY CASCADE also resets sequences and
        removes child rows so FK constraints are not violated.
        Controlled by db_cfg['_truncate_before_load'] (default True).
        """
        import psycopg
        schema = self.schema
        qualified = f'"{schema}"."{table_name}"'
        cols_sql = ", ".join(f'"{c}"' for c in column_names)
        copy_sql = (
            f"COPY {qualified} ({cols_sql}) "
            f"FROM STDIN WITH (FORMAT CSV, HEADER TRUE, NULL '')"
        )
        should_truncate = self.db_cfg.get("_truncate_before_load", True)

        with psycopg.connect(self._dsn(), autocommit=False) as conn:
            # Step 1: Wipe existing rows — makes every run idempotent
            if should_truncate:
                with conn.cursor() as cur:
                    cur.execute(f"TRUNCATE TABLE {qualified} RESTART IDENTITY CASCADE")  #type: ignore
                conn.commit()
                self.log.info("Truncated '%s' before load.", table_name)

            # Step 2: Drop non-PK indexes for fast COPY
            dropped = self._drop_indexes(conn, table_name)
            try:
                with conn.transaction():
                    with conn.cursor() as cur:
                        with open(csv_path, "rb") as fh:
                            with cur.copy(copy_sql) as copy:   #type: ignore
                                while True:
                                    chunk = fh.read(65536)
                                    if not chunk:
                                        break
                                    copy.write(chunk)
                        rows = cur.rowcount
            except Exception:
                self._rebuild_indexes(conn, dropped)
                raise
            # Step 3: Rebuild indexes after load
            self._rebuild_indexes(conn, dropped)
        return rows if rows >= 0 else 0

    # ── Private helpers ───────────────────────────────────────────────

    def _fetch_tables(self, conn) -> list[str]:
        sql = """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        with conn.cursor() as cur:
            cur.execute(sql, (self.schema,))
            return [r[0] for r in cur.fetchall()]

    def _fetch_columns(self, conn) -> dict[str, list[ColumnMeta]]:
        sql = """
            SELECT table_name, column_name, data_type, udt_name, is_nullable,
                   character_maximum_length, numeric_precision, numeric_scale, ordinal_position
            FROM information_schema.columns
            WHERE table_schema = %s ORDER BY table_name, ordinal_position
        """
        result: dict[str, list[ColumnMeta]] = {}
        with conn.cursor() as cur:
            cur.execute(sql, (self.schema,))
            for row in cur.fetchall():
                tbl, col, dtype, udt, nullable, max_len, num_prec, num_scale, pos = row
                result.setdefault(tbl, []).append(ColumnMeta(
                    name=col, data_type=dtype, udt_name=udt,
                    is_nullable=(nullable == "YES"),
                    character_maximum_length=max_len,
                    numeric_precision=num_prec,
                    numeric_scale=num_scale,
                    ordinal_position=pos,
                ))
        return result

    def _fetch_primary_keys(self, conn) -> dict[str, list[str]]:
        sql = """
            SELECT kcu.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = %s
            ORDER BY kcu.table_name, kcu.ordinal_position
        """
        result: dict[str, list[str]] = {}
        with conn.cursor() as cur:
            cur.execute(sql, (self.schema,))
            for tbl, col in cur.fetchall():
                result.setdefault(tbl, []).append(col)
        return result

    def _fetch_foreign_keys(self, conn) -> dict[str, list[ForeignKeyMeta]]:
        sql = """
            SELECT tc.table_name, tc.constraint_name, kcu.column_name,
                   ccu.table_name AS ref_table, ccu.column_name AS ref_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_schema = %s
            ORDER BY tc.table_name, tc.constraint_name
        """
        result: dict[str, list[ForeignKeyMeta]] = {}
        with conn.cursor() as cur:
            cur.execute(sql, (self.schema,))
            for fk_tbl, cname, fk_col, ref_tbl, ref_col in cur.fetchall():
                result.setdefault(fk_tbl, []).append(ForeignKeyMeta(
                    constraint_name=cname, column=fk_col,
                    ref_table=ref_tbl, ref_column=ref_col,
                ))
        return result

    def _drop_indexes(self, conn, table_name: str) -> list[dict]:
        sql = """
            SELECT i.relname, pg_get_indexdef(ix.indexrelid)
            FROM pg_class t
            JOIN pg_index ix ON t.oid = ix.indrelid
            JOIN pg_class i ON i.oid = ix.indexrelid
            JOIN pg_namespace n ON n.oid = t.relnamespace
            WHERE t.relname = %s AND n.nspname = %s
              AND ix.indisprimary = FALSE AND t.relkind = 'r'
        """
        dropped = []
        with conn.cursor() as cur:
            cur.execute(sql, (table_name, self.schema))
            indexes = cur.fetchall()
        for index_name, create_sql in indexes:
            try:
                with conn.cursor() as cur:
                    cur.execute(f'DROP INDEX IF EXISTS "{self.schema}"."{index_name}"')
                conn.commit()
                dropped.append({"name": index_name, "sql": create_sql})
            except Exception as exc:
                self.log.warning("Could not drop index %s: %s", index_name, exc)
        return dropped

    def _rebuild_indexes(self, conn, indexes: list[dict]) -> None:
        for idx in indexes:
            try:
                with conn.cursor() as cur:
                    cur.execute(idx["sql"])
                conn.commit()
            except Exception as exc:
                self.err_log.error("Failed to rebuild index %s: %s", idx["name"], exc)
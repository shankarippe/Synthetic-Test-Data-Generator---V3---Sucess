"""
adapters/mysql.py
-----------------
MySQL / MariaDB adapter using mysql-connector-python or PyMySQL.

Install: pip install mysql-connector-python
     OR: pip install PyMySQL

Config fields:
  host, port (default 3306), dbname, user, password, schema (= dbname in MySQL)

MySQL type mapping:
  INT, TINYINT, SMALLINT, BIGINT → int types
  DECIMAL, NUMERIC, FLOAT, DOUBLE → numeric
  VARCHAR, CHAR, TEXT             → text types
  DATE, DATETIME, TIMESTAMP       → date/timestamp
  TINYINT(1)                      → bool (MySQL bool convention)
  BLOB, BINARY, VARBINARY         → bytea
"""

from __future__ import annotations

import csv
import logging
from pathlib import Path

from adapters.base import BaseDBAdapter, TableMeta, ColumnMeta, ForeignKeyMeta

logger = logging.getLogger("app")

_MYSQL_TYPE_MAP = {
    "int": "int4",
    "integer": "int4",
    "tinyint": "int2",
    "smallint": "int2",
    "mediumint": "int4",
    "bigint": "int8",
    "decimal": "numeric",
    "numeric": "numeric",
    "float": "float4",
    "double": "float8",
    "real": "float8",
    "bit": "bool",
    "bool": "bool",
    "boolean": "bool",
    "varchar": "varchar",
    "char": "bpchar",
    "tinytext": "text",
    "text": "text",
    "mediumtext": "text",
    "longtext": "text",
    "date": "date",
    "datetime": "timestamp",
    "timestamp": "timestamp",
    "time": "time",
    "year": "int2",
    "binary": "bytea",
    "varbinary": "bytea",
    "tinyblob": "bytea",
    "blob": "bytea",
    "mediumblob": "bytea",
    "longblob": "bytea",
    "json": "json",
    "enum": "varchar",
    "set": "varchar",
}


class MySQLAdapter(BaseDBAdapter):
    """MySQL/MariaDB adapter."""

    def _get_conn(self):
        c = self.db_cfg
        # Try mysql-connector-python first
        try:
            import mysql.connector
            return mysql.connector.connect(
                host=c["host"],
                port=c.get("port", 3306),
                database=c["dbname"],
                user=c["user"],
                password=c["password"],
                allow_local_infile=True,
                charset="utf8mb4",
            )
        except ImportError:
            pass
        # Fallback: PyMySQL
        try:
            import pymysql
            return pymysql.connect(
                host=c["host"],
                port=c.get("port", 3306),
                db=c["dbname"],
                user=c["user"],
                password=c["password"],
                charset="utf8mb4",
                autocommit=False,
            )
        except ImportError:
            raise ImportError(
                "No MySQL driver found. Run: pip install mysql-connector-python  or  pip install PyMySQL"
            )

    def test_connection(self) -> bool:
        try:
            conn = self._get_conn()
            conn.close()
            return True
        except Exception as exc:
            self.log.error("MySQL connection failed: %s", exc)
            return False

    def read_all(self) -> dict[str, TableMeta]:
        # In MySQL, schema == database name
        db_name = self.db_cfg["dbname"]
        conn = self._get_conn()
        try:
            tables = self._fetch_tables(conn, db_name)
            columns = self._fetch_columns(conn, db_name)
            pks = self._fetch_primary_keys(conn, db_name)
            fks = self._fetch_foreign_keys(conn, db_name)
        finally:
            conn.close()

        meta: dict[str, TableMeta] = {}
        for tbl in tables:
            tm = TableMeta(schema=db_name, name=tbl)
            tm.columns = columns.get(tbl, [])
            tm.primary_keys = pks.get(tbl, [])
            tm.foreign_keys = fks.get(tbl, [])
            meta[tbl] = tm
        self.log.info("MySQL: discovered %d tables in database '%s'", len(meta), db_name)
        return meta

    def bulk_load(self, table_name: str, csv_path: str, column_names: list[str]) -> int:
        """
        MySQL bulk load using LOAD DATA LOCAL INFILE (fastest)
        with executemany fallback.
        """
        db_name = self.db_cfg["dbname"]
        qualified = f"`{db_name}`.`{table_name}`"

        rows_loaded = 0
        batch_size = 5000

        conn = self._get_conn()
        try:
            cur = conn.cursor()
            # Try LOAD DATA LOCAL INFILE first (requires server permission)
            cols_sql = ", ".join(f"`{c}`" for c in column_names)
            try:
                load_sql = f"""
                    LOAD DATA LOCAL INFILE '{csv_path}'
                    INTO TABLE {qualified}
                    FIELDS TERMINATED BY ','
                    OPTIONALLY ENCLOSED BY '"'
                    LINES TERMINATED BY '\\n'
                    IGNORE 1 ROWS ({cols_sql})
                """
                cur.execute(load_sql)
                rows_loaded = cur.rowcount
                conn.commit()
                self.log.info("MySQL LOAD DATA: %d rows into %s", rows_loaded, qualified)
            except Exception:
                # Fallback: executemany INSERT
                self.log.warning("LOAD DATA failed, falling back to executemany INSERT")
                placeholders = ", ".join("%s" for _ in column_names)
                insert_sql = f"INSERT INTO {qualified} ({cols_sql}) VALUES ({placeholders})"
                with open(csv_path, newline="", encoding="utf-8") as fh:
                    reader = csv.DictReader(fh)
                    batch = []
                    for row in reader:
                        values = tuple(row.get(c) or None for c in column_names)
                        batch.append(values)
                        if len(batch) >= batch_size:
                            cur.executemany(insert_sql, batch)
                            conn.commit()
                            rows_loaded += len(batch)
                            batch = []
                    if batch:
                        cur.executemany(insert_sql, batch)
                        conn.commit()
                        rows_loaded += len(batch)
        finally:
            conn.close()

        self.log.info("MySQL loaded %d rows into %s", rows_loaded, qualified)
        return rows_loaded

    # ── Private helpers ───────────────────────────────────────────────

    def _fetch_tables(self, conn, db_name: str) -> list[str]:
        cur = conn.cursor()
        cur.execute(
            "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES "
            "WHERE TABLE_SCHEMA = %s AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME",
            (db_name,)
        )
        return [r[0] for r in cur.fetchall()]

    def _fetch_columns(self, conn, db_name: str) -> dict[str, list[ColumnMeta]]:
        sql = """
            SELECT TABLE_NAME, COLUMN_NAME, DATA_TYPE, IS_NULLABLE,
                   CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, ORDINAL_POSITION
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = %s
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """
        result: dict[str, list[ColumnMeta]] = {}
        cur = conn.cursor()
        cur.execute(sql, (db_name,))
        for row in cur.fetchall():
            tbl, col, dtype, nullable, max_len, num_prec, num_scale, pos = row
            udt = _MYSQL_TYPE_MAP.get(dtype.lower(), "varchar")
            result.setdefault(tbl, []).append(ColumnMeta(
                name=col, data_type=dtype, udt_name=udt,
                is_nullable=(nullable == "YES"),
                character_maximum_length=max_len,
                numeric_precision=num_prec,
                numeric_scale=num_scale,
                ordinal_position=pos,
            ))
        return result

    def _fetch_primary_keys(self, conn, db_name: str) -> dict[str, list[str]]:
        sql = """
            SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s AND CONSTRAINT_NAME = 'PRIMARY'
            ORDER BY TABLE_NAME, ORDINAL_POSITION
        """
        result: dict[str, list[str]] = {}
        cur = conn.cursor()
        cur.execute(sql, (db_name,))
        for tbl, col in cur.fetchall():
            result.setdefault(tbl, []).append(col)
        return result

    def _fetch_foreign_keys(self, conn, db_name: str) -> dict[str, list[ForeignKeyMeta]]:
        sql = """
            SELECT
                TABLE_NAME, CONSTRAINT_NAME, COLUMN_NAME,
                REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = %s
              AND REFERENCED_TABLE_NAME IS NOT NULL
            ORDER BY TABLE_NAME, CONSTRAINT_NAME
        """
        result: dict[str, list[ForeignKeyMeta]] = {}
        cur = conn.cursor()
        cur.execute(sql, (db_name,))
        for fk_tbl, cname, fk_col, ref_tbl, ref_col in cur.fetchall():
            if fk_tbl and ref_tbl:
                result.setdefault(fk_tbl, []).append(ForeignKeyMeta(
                    constraint_name=cname, column=fk_col,
                    ref_table=ref_tbl, ref_column=ref_col,
                ))
        return result
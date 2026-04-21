"""
adapters/__init__.py
--------------------
Adapter factory — returns the correct DB adapter based on config.

Config field: database.engine  (one of: postgres, oracle, sqlserver, mysql)
Default: postgres
"""

from __future__ import annotations

from adapters.base import BaseDBAdapter


def get_adapter(config: dict, loggers: dict) -> BaseDBAdapter:
    """
    Factory function. Returns the correct adapter for the configured engine.

    Usage in config.yaml:
        database:
          engine: postgres   # or oracle, sqlserver, mysql
          host: ...
    """
    engine = config.get("database", {}).get("engine", "postgres").lower().strip()

    if engine in ("postgres", "postgresql"):
        from adapters.postgres import PostgresAdapter
        return PostgresAdapter(config, loggers)

    elif engine in ("oracle", "oracledb"):
        from adapters.oracle import OracleAdapter
        return OracleAdapter(config, loggers)

    elif engine in ("sqlserver", "mssql", "sql_server"):
        from adapters.sqlserver import SQLServerAdapter
        return SQLServerAdapter(config, loggers)

    elif engine in ("mysql", "mariadb"):
        from adapters.mysql import MySQLAdapter
        return MySQLAdapter(config, loggers)

    else:
        supported = ["postgres", "oracle", "sqlserver", "mysql"]
        raise ValueError(
            f"Unknown database engine: '{engine}'. "
            f"Supported engines: {supported}"
        )


__all__ = ["get_adapter", "BaseDBAdapter"]
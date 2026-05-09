import os
from contextlib import closing
from typing import Any

import trino

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8080"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "gold")


def get_connection():
    return trino.dbapi.connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=TRINO_CATALOG,
        schema=TRINO_SCHEMA,
    )


def run_query(sql: str, params: list[Any] | tuple[Any, ...] | None = None) -> list[dict[str, Any]]:
    with closing(get_connection()) as conn:
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params or [])
            columns = [desc[0] for desc in cursor.description or []]
            rows = cursor.fetchall()
        finally:
            cursor.close()

    return [dict(zip(columns, row)) for row in rows]

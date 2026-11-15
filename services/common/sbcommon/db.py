"""Postgres (Cloud SQL) helpers. Each service owns its own database on the shared Cloud SQL instance."""
import logging
import time

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

from .config import env, env_int

log = logging.getLogger("db")


def conninfo(dbname: str | None = None) -> str:
    return (
        f"host={env('DB_HOST', '127.0.0.1')} port={env('DB_PORT', '5432')} "
        f"dbname={dbname or env('DB_NAME')} user={env('DB_USER', 'sbapp')} "
        f"password={env('DB_PASSWORD', 'localpass')} connect_timeout=5 "
        f"application_name={env('SERVICE_NAME', 'svc')}"
    )


def create_pool(dbname: str | None = None, min_size: int = 1, max_size: int | None = None) -> ConnectionPool:
    pool = ConnectionPool(
        conninfo(dbname),
        min_size=min_size,
        max_size=max_size or env_int("DB_POOL_MAX", 10),
        kwargs={"row_factory": dict_row, "autocommit": False},
        open=False,
        name=dbname,
    )
    # Cloud SQL / pod networking may not be ready on the first attempt -> retry.
    for attempt in range(1, 31):
        try:
            pool.open(wait=True, timeout=10)
            return pool
        except Exception as exc:  # noqa: BLE001
            log.warning("database not ready, retrying", extra={"attempt": attempt, "error": str(exc)})
            time.sleep(min(2 * attempt, 10))
    raise RuntimeError("could not connect to database")


def migrate(pool: ConnectionPool, ddl: str) -> None:
    """Idempotent schema creation (CREATE ... IF NOT EXISTS). Serialised with an advisory lock
    so that several replicas starting at once do not race."""
    with pool.connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_xact_lock(777001)")
            cur.execute(ddl)
        conn.commit()

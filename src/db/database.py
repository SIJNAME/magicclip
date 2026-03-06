from contextlib import contextmanager
from typing import Generator

from psycopg2.extras import RealDictCursor
from psycopg2.pool import ThreadedConnectionPool

from src.config import settings

_POOL: ThreadedConnectionPool | None = None


def _pool() -> ThreadedConnectionPool:
    global _POOL
    if _POOL is None:
        _POOL = ThreadedConnectionPool(
            minconn=settings.db_pool_min,
            maxconn=settings.db_pool_max,
            dsn=settings.database_url,
        )
    return _POOL


@contextmanager
def get_connection() -> Generator:
    pool = _pool()
    conn = pool.getconn()
    try:
        conn.autocommit = False
        yield conn
    finally:
        pool.putconn(conn)


def close_pool() -> None:
    global _POOL
    if _POOL is not None:
        _POOL.closeall()
        _POOL = None


def init_db() -> None:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    password_hash TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL
                );

                CREATE TABLE IF NOT EXISTS subscriptions (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    plan_code TEXT NOT NULL,
                    stripe_customer_id TEXT,
                    stripe_subscription_id TEXT,
                    status TEXT NOT NULL,
                    current_period_end TIMESTAMPTZ,
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS jobs (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    source_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    error_message TEXT,
                    requested_minutes DOUBLE PRECISION NOT NULL,
                    reserved_minutes DOUBLE PRECISION NOT NULL,
                    reserved_storage_bytes BIGINT NOT NULL DEFAULT 0,
                    output_size_bytes BIGINT,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    started_at TIMESTAMPTZ,
                    heartbeat_at TIMESTAMPTZ,
                    transcript_json TEXT,
                    clips_json TEXT,
                    selected_clip_json TEXT,
                    output_key TEXT,
                    created_at TIMESTAMPTZ NOT NULL,
                    updated_at TIMESTAMPTZ NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS usage_ledger (
                    id TEXT PRIMARY KEY,
                    user_id TEXT NOT NULL,
                    job_id TEXT NOT NULL,
                    minutes DOUBLE PRECISION NOT NULL,
                    entry_type TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE CASCADE,
                    FOREIGN KEY (job_id) REFERENCES jobs(id) ON DELETE CASCADE
                );

                CREATE TABLE IF NOT EXISTS dead_letter_jobs (
                    id TEXT PRIMARY KEY,
                    job_id TEXT NOT NULL UNIQUE,
                    user_id TEXT NOT NULL,
                    reason TEXT NOT NULL,
                    payload_json TEXT,
                    created_at TIMESTAMPTZ NOT NULL
                );

                CREATE TABLE IF NOT EXISTS stripe_events (
                    id TEXT PRIMARY KEY,
                    event_id TEXT NOT NULL UNIQUE,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    created_at TIMESTAMPTZ NOT NULL
                );

                CREATE UNIQUE INDEX IF NOT EXISTS uq_usage_job_entry
                ON usage_ledger(job_id, entry_type);

                CREATE INDEX IF NOT EXISTS idx_jobs_user_status
                ON jobs(user_id, status);

                CREATE INDEX IF NOT EXISTS idx_jobs_heartbeat
                ON jobs(status, heartbeat_at);
                """
            )
        conn.commit()


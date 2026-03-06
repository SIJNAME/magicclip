import json
from calendar import monthrange
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import uuid4

from psycopg2.extras import RealDictCursor

from src.db.database import get_connection


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _row_to_dict(row: dict[str, Any] | None) -> dict[str, Any] | None:
    if not row:
        return None
    out = dict(row)
    for key, value in list(out.items()):
        if isinstance(value, datetime):
            out[key] = value.isoformat()
    return out


def create_user(email: str, password_hash: str) -> dict[str, Any]:
    user_id = str(uuid4())
    now = _now()
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "INSERT INTO users (id, email, password_hash, created_at) VALUES (%s, %s, %s, %s)",
                (user_id, email.lower().strip(), password_hash, now),
            )
        conn.commit()
    return get_user_by_id(user_id) or {}


def get_user_by_email(email: str) -> dict[str, Any] | None:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE email = %s", (email.lower().strip(),))
            row = cur.fetchone()
    return _row_to_dict(row)


def get_user_by_id(user_id: str) -> dict[str, Any] | None:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE id = %s", (user_id,))
            row = cur.fetchone()
    return _row_to_dict(row)


def upsert_subscription(
    *,
    user_id: str,
    plan_code: str,
    status: str,
    stripe_customer_id: str | None,
    stripe_subscription_id: str | None,
    current_period_end: str | None,
) -> dict[str, Any]:
    now = _now()
    period_end_dt = None
    if current_period_end:
        period_end_dt = datetime.fromisoformat(current_period_end.replace("Z", "+00:00"))
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO subscriptions (
                    id, user_id, plan_code, stripe_customer_id, stripe_subscription_id,
                    status, current_period_end, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (user_id)
                DO UPDATE SET
                    plan_code = EXCLUDED.plan_code,
                    status = EXCLUDED.status,
                    stripe_customer_id = EXCLUDED.stripe_customer_id,
                    stripe_subscription_id = EXCLUDED.stripe_subscription_id,
                    current_period_end = EXCLUDED.current_period_end,
                    updated_at = EXCLUDED.updated_at
                """,
                (
                    str(uuid4()),
                    user_id,
                    plan_code,
                    stripe_customer_id,
                    stripe_subscription_id,
                    status,
                    period_end_dt,
                    now,
                    now,
                ),
            )
        conn.commit()
    return get_subscription_by_user_id(user_id) or {}


def get_subscription_by_user_id(user_id: str) -> dict[str, Any] | None:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM subscriptions WHERE user_id = %s", (user_id,))
            row = cur.fetchone()
    return _row_to_dict(row)


def reserve_and_create_job_atomic(
    *,
    user_id: str,
    source_key: str,
    requested_minutes: float,
    reserved_storage_bytes: int,
    monthly_minutes_limit: int,
    max_concurrent_jobs: int,
    storage_limit_bytes: int,
) -> dict[str, Any]:
    now = _now()
    period_start = datetime(now.year, now.month, 1, tzinfo=timezone.utc)
    last_day = monthrange(now.year, now.month)[1]
    period_end = datetime(now.year, now.month, last_day, 23, 59, 59, tzinfo=timezone.utc)
    job_id = str(uuid4())
    usage_entry_id = str(uuid4())

    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, status FROM subscriptions WHERE user_id = %s FOR UPDATE", (user_id,))
            subscription = cur.fetchone()
            if not subscription or subscription["status"] not in {"active", "trialing"}:
                raise ValueError("Subscription is not active")

            cur.execute(
                """
                SELECT COALESCE(SUM(minutes), 0) AS used
                FROM usage_ledger
                WHERE user_id = %s
                  AND entry_type = 'reserve'
                  AND created_at >= %s
                  AND created_at <= %s
                """,
                (user_id, period_start, period_end),
            )
            monthly_used = float((cur.fetchone() or {}).get("used") or 0.0)
            if monthly_used + float(requested_minutes) > float(monthly_minutes_limit):
                raise ValueError("Monthly minutes exceeded")

            cur.execute(
                """
                SELECT id, status, reserved_storage_bytes, output_size_bytes
                FROM jobs
                WHERE user_id = %s
                FOR UPDATE
                """,
                (user_id,),
            )
            user_jobs = cur.fetchall() or []
            concurrent_jobs = sum(1 for item in user_jobs if item["status"] in {"queued", "processing"})
            if concurrent_jobs >= int(max_concurrent_jobs):
                raise ValueError("Max concurrent jobs exceeded")

            used_bytes = 0
            for item in user_jobs:
                if item["status"] in {"queued", "processing"}:
                    used_bytes += int(item.get("reserved_storage_bytes") or 0)
                elif item["status"] == "completed":
                    used_bytes += int(item.get("output_size_bytes") or 0)
            if used_bytes + int(reserved_storage_bytes) > int(storage_limit_bytes):
                raise ValueError("Storage limit exceeded")

            cur.execute(
                """
                INSERT INTO jobs (
                    id, user_id, source_key, status, error_message, requested_minutes, reserved_minutes,
                    reserved_storage_bytes, output_size_bytes, attempts, started_at, heartbeat_at,
                    transcript_json, clips_json, selected_clip_json, output_key, created_at, updated_at
                ) VALUES (%s, %s, %s, 'queued', NULL, %s, %s, %s, NULL, 0, NULL, NULL, NULL, NULL, NULL, NULL, %s, %s)
                """,
                (
                    job_id,
                    user_id,
                    source_key,
                    float(requested_minutes),
                    float(requested_minutes),
                    int(reserved_storage_bytes),
                    now,
                    now,
                ),
            )
            cur.execute(
                """
                INSERT INTO usage_ledger (id, user_id, job_id, minutes, entry_type, created_at)
                VALUES (%s, %s, %s, %s, 'reserve', %s)
                ON CONFLICT (job_id, entry_type) DO NOTHING
                """,
                (usage_entry_id, user_id, job_id, float(requested_minutes), now),
            )
        conn.commit()
    return get_job(job_id) or {}


def get_job(job_id: str) -> dict[str, Any] | None:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM jobs WHERE id = %s", (job_id,))
            row = cur.fetchone()
    data = _row_to_dict(row)
    if not data:
        return None
    for key in ("transcript_json", "clips_json", "selected_clip_json"):
        if data.get(key):
            data[key] = json.loads(data[key])
    return data


def list_jobs_by_user(user_id: str) -> list[dict[str, Any]]:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM jobs WHERE user_id = %s ORDER BY created_at DESC", (user_id,))
            rows = cur.fetchall() or []
    out: list[dict[str, Any]] = []
    for row in rows:
        item = _row_to_dict(row) or {}
        for key in ("transcript_json", "clips_json", "selected_clip_json"):
            if item.get(key):
                item[key] = json.loads(item[key])
        out.append(item)
    return out


def claim_job_for_processing(job_id: str) -> bool:
    now = _now()
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, status FROM jobs WHERE id = %s FOR UPDATE", (job_id,))
            row = cur.fetchone()
            if not row or row["status"] != "queued":
                conn.rollback()
                return False
            cur.execute(
                """
                UPDATE jobs
                SET status = 'processing',
                    attempts = attempts + 1,
                    started_at = COALESCE(started_at, %s),
                    heartbeat_at = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (now, now, now, job_id),
            )
        conn.commit()
    return True


def touch_job_heartbeat(job_id: str) -> None:
    now = _now()
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                "UPDATE jobs SET heartbeat_at = %s, updated_at = %s WHERE id = %s AND status = 'processing'",
                (now, now, job_id),
            )
        conn.commit()


def _get_reserved_minutes_for_job(cur, job_id: str) -> tuple[str, float]:
    cur.execute("SELECT user_id, reserved_minutes FROM jobs WHERE id = %s FOR UPDATE", (job_id,))
    row = cur.fetchone()
    if not row:
        raise ValueError("Job not found")
    return str(row["user_id"]), float(row["reserved_minutes"] or 0.0)


def refund_minutes_if_needed(job_id: str) -> None:
    now = _now()
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            user_id, reserved_minutes = _get_reserved_minutes_for_job(cur, job_id)
            cur.execute(
                """
                INSERT INTO usage_ledger (id, user_id, job_id, minutes, entry_type, created_at)
                VALUES (%s, %s, %s, %s, 'adjust', %s)
                ON CONFLICT (job_id, entry_type) DO NOTHING
                """,
                (str(uuid4()), user_id, job_id, -reserved_minutes, now),
            )
        conn.commit()


def create_dead_letter(job_id: str, user_id: str, reason: str, payload: dict[str, Any] | None = None) -> None:
    now = _now()
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO dead_letter_jobs (id, job_id, user_id, reason, payload_json, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id) DO NOTHING
                """,
                (str(uuid4()), job_id, user_id, reason[:1000], json.dumps(payload or {}), now),
            )
        conn.commit()


def mark_job_completed(
    *,
    job_id: str,
    transcript: list[dict[str, Any]],
    clips: list[dict[str, Any]],
    selected_clip: dict[str, Any],
    output_key: str,
    output_size_bytes: int,
    storage_limit_bytes: int,
) -> None:
    now = _now()
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM jobs WHERE id = %s FOR UPDATE", (job_id,))
            job = cur.fetchone()
            if not job:
                raise ValueError("Job not found")
            if job["status"] == "completed":
                conn.rollback()
                return
            if job["status"] != "processing":
                raise ValueError("Job is not processing")

            user_id = str(job["user_id"])
            reserved_current = int(job.get("reserved_storage_bytes") or 0)

            cur.execute(
                """
                SELECT id, status, reserved_storage_bytes, output_size_bytes
                FROM jobs
                WHERE user_id = %s
                FOR UPDATE
                """,
                (user_id,),
            )
            user_jobs = cur.fetchall() or []
            used_bytes = 0
            for item in user_jobs:
                if item["status"] in {"queued", "processing"}:
                    used_bytes += int(item.get("reserved_storage_bytes") or 0)
                elif item["status"] == "completed":
                    used_bytes += int(item.get("output_size_bytes") or 0)
            projected = used_bytes - reserved_current + int(output_size_bytes)
            if projected > int(storage_limit_bytes):
                raise ValueError("Storage limit exceeded on completion")

            cur.execute(
                """
                UPDATE jobs
                SET status = 'completed',
                    transcript_json = %s,
                    clips_json = %s,
                    selected_clip_json = %s,
                    output_key = %s,
                    output_size_bytes = %s,
                    reserved_storage_bytes = 0,
                    heartbeat_at = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (
                    json.dumps(transcript, ensure_ascii=False),
                    json.dumps(clips, ensure_ascii=False),
                    json.dumps(selected_clip, ensure_ascii=False),
                    output_key,
                    int(output_size_bytes),
                    now,
                    now,
                    job_id,
                ),
            )
        conn.commit()


def mark_job_failed(job_id: str, error_message: str, to_dead_letter: bool = True) -> None:
    now = _now()
    user_id = ""
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT id, user_id, status, reserved_minutes FROM jobs WHERE id = %s FOR UPDATE", (job_id,))
            job = cur.fetchone()
            if not job:
                conn.rollback()
                return
            user_id = str(job["user_id"])
            cur.execute(
                """
                UPDATE jobs
                SET status = 'failed',
                    error_message = %s,
                    heartbeat_at = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (error_message[:2000], now, now, job_id),
            )
            cur.execute(
                """
                INSERT INTO usage_ledger (id, user_id, job_id, minutes, entry_type, created_at)
                VALUES (%s, %s, %s, %s, 'adjust', %s)
                ON CONFLICT (job_id, entry_type) DO NOTHING
                """,
                (str(uuid4()), user_id, job_id, -float(job["reserved_minutes"] or 0.0), now),
            )
        conn.commit()
    if to_dead_letter and user_id:
        create_dead_letter(job_id, user_id, error_message)


def add_usage_entry(user_id: str, job_id: str, minutes: float, entry_type: str) -> dict[str, Any]:
    entry_id = str(uuid4())
    now = _now()
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO usage_ledger (id, user_id, job_id, minutes, entry_type, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (job_id, entry_type) DO NOTHING
                RETURNING *
                """,
                (entry_id, user_id, job_id, float(minutes), entry_type, now),
            )
            row = cur.fetchone()
            if not row:
                cur.execute(
                    "SELECT * FROM usage_ledger WHERE job_id = %s AND entry_type = %s",
                    (job_id, entry_type),
                )
                row = cur.fetchone()
        conn.commit()
    return _row_to_dict(row) or {}


def get_usage_totals(user_id: str) -> dict[str, float]:
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    COALESCE(SUM(CASE WHEN entry_type = 'reserve' THEN minutes ELSE 0 END), 0) AS reserved_minutes,
                    COALESCE(SUM(CASE WHEN entry_type = 'adjust' THEN minutes ELSE 0 END), 0) AS adjusted_minutes
                FROM usage_ledger
                WHERE user_id = %s
                """,
                (user_id,),
            )
            row = cur.fetchone() or {}
    reserved = float(row.get("reserved_minutes") or 0.0)
    adjusted = float(row.get("adjusted_minutes") or 0.0)
    return {"reserved_minutes": reserved, "total_minutes": max(0.0, reserved + adjusted)}


def register_stripe_event_once(event_id: str, event_type: str, payload: dict[str, Any]) -> bool:
    now = _now()
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                INSERT INTO stripe_events (id, event_id, event_type, payload_json, created_at)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (event_id) DO NOTHING
                RETURNING id
                """,
                (str(uuid4()), event_id, event_type, json.dumps(payload, ensure_ascii=False, default=str), now),
            )
            row = cur.fetchone()
        conn.commit()
    return bool(row)


def recover_timed_out_jobs(timeout_seconds: int, max_attempts: int) -> dict[str, list[str]]:
    now = _now()
    stale_before = now - timedelta(seconds=int(timeout_seconds))
    requeued: list[str] = []
    dead_lettered: list[str] = []

    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT id, user_id, attempts, reserved_minutes
                FROM jobs
                WHERE status = 'processing'
                  AND heartbeat_at IS NOT NULL
                  AND heartbeat_at < %s
                FOR UPDATE SKIP LOCKED
                """,
                (stale_before,),
            )
            stale_jobs = cur.fetchall() or []

            for job in stale_jobs:
                jid = str(job["id"])
                user_id = str(job["user_id"])
                attempts = int(job["attempts"] or 0)
                if attempts < int(max_attempts):
                    cur.execute(
                        """
                        UPDATE jobs
                        SET status = 'queued', heartbeat_at = %s, updated_at = %s
                        WHERE id = %s
                        """,
                        (now, now, jid),
                    )
                    requeued.append(jid)
                else:
                    cur.execute(
                        """
                        UPDATE jobs
                        SET status = 'failed',
                            error_message = %s,
                            heartbeat_at = %s,
                            updated_at = %s
                        WHERE id = %s
                        """,
                        ("Job timed out", now, now, jid),
                    )
                    cur.execute(
                        """
                        INSERT INTO usage_ledger (id, user_id, job_id, minutes, entry_type, created_at)
                        VALUES (%s, %s, %s, %s, 'adjust', %s)
                        ON CONFLICT (job_id, entry_type) DO NOTHING
                        """,
                        (str(uuid4()), user_id, jid, -float(job["reserved_minutes"] or 0.0), now),
                    )
                    cur.execute(
                        """
                        INSERT INTO dead_letter_jobs (id, job_id, user_id, reason, payload_json, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s)
                        ON CONFLICT (job_id) DO NOTHING
                        """,
                        (str(uuid4()), jid, user_id, "Job timed out", json.dumps({}), now),
                    )
                    dead_lettered.append(jid)
        conn.commit()
    return {"requeued": requeued, "dead_lettered": dead_lettered}

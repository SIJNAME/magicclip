from redis import Redis
from rq import Queue

from src.config import settings


def get_queue() -> Queue:
    if not settings.redis_url:
        raise RuntimeError("REDIS_URL is required")
    conn = Redis.from_url(settings.redis_url)
    return Queue(settings.queue_name, connection=conn)


def get_dead_letter_queue() -> Queue:
    if not settings.redis_url:
        raise RuntimeError("REDIS_URL is required")
    conn = Redis.from_url(settings.redis_url)
    return Queue(settings.dead_letter_queue_name, connection=conn)


def enqueue_dead_letter(job_id: str, reason: str) -> None:
    if not settings.redis_url:
        raise RuntimeError("REDIS_URL is required")
    conn = Redis.from_url(settings.redis_url)
    lock_key = f"dlq:enqueued:{job_id}"
    if not conn.set(lock_key, "1", nx=True, ex=86400):
        return
    queue = Queue(settings.dead_letter_queue_name, connection=conn)
    queue.enqueue("src.queue.worker.dead_letter_sink", job_id, reason, job_timeout="5m")

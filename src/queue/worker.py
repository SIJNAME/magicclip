import os
import logging

from redis import Redis
from rq import Connection, Worker

from src.config import settings
from src.db.repository import recover_timed_out_jobs
from src.queue.service import enqueue_dead_letter, get_queue

logger = logging.getLogger(__name__)


def dead_letter_sink(job_id: str, reason: str) -> None:
    logger.error("dead_letter_job", extra={"job_id": job_id, "reason": reason})


def main() -> None:
    recovery = recover_timed_out_jobs(settings.job_timeout_sec, settings.job_max_attempts)
    if recovery["requeued"]:
        queue = get_queue()
        for job_id in recovery["requeued"]:
            queue.enqueue("src.pipeline.worker.process_pipeline_job", job_id, job_timeout="60m")
    if recovery["dead_lettered"]:
        for job_id in recovery["dead_lettered"]:
            enqueue_dead_letter(job_id, "Job timed out")
        logger.warning("jobs_dead_lettered", extra={"job_ids": recovery["dead_lettered"]})

    if not settings.redis_url:
        raise RuntimeError("REDIS_URL is required")
    redis_conn = Redis.from_url(settings.redis_url)
    queue_name = settings.queue_name
    with Connection(redis_conn):
        worker = Worker([queue_name])
        worker.work(with_scheduler=False)


if __name__ == "__main__":
    os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    main()

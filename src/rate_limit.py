import threading
import time
from collections import defaultdict

from fastapi import HTTPException
from redis import Redis

from src.config import settings

_fallback_lock = threading.Lock()
_fallback_buckets: dict[str, list[float]] = defaultdict(list)


def _redis_client() -> Redis | None:
    if not settings.redis_url:
        return None
    return Redis.from_url(settings.redis_url)


def enforce_user_rate_limit(user_id: str, route_key: str) -> None:
    now = int(time.time())
    window = int(settings.rate_limit_window_sec)
    limit = int(settings.rate_limit_per_minute)
    bucket_key = f"rl:{user_id}:{route_key}:{now // window}"

    client = _redis_client()
    if client is not None:
        current = client.incr(bucket_key)
        if current == 1:
            client.expire(bucket_key, window + 2)
        if current > limit:
            raise HTTPException(status_code=429, detail="Rate limit exceeded")
        return

    cutoff = time.time() - window
    with _fallback_lock:
        bucket = _fallback_buckets[bucket_key]
        bucket.append(time.time())
        _fallback_buckets[bucket_key] = [ts for ts in bucket if ts >= cutoff]
        if len(_fallback_buckets[bucket_key]) > limit:
            raise HTTPException(status_code=429, detail="Rate limit exceeded")


from src.config import settings
from src.db.repository import get_subscription_by_user_id, get_usage_totals, reserve_and_create_job_atomic


def _plan_policy(user_id: str) -> dict:
    sub = get_subscription_by_user_id(user_id)
    if sub and sub.get("plan_code") == "pro":
        return {
            "monthly_minutes": settings.plan_pro_monthly_minutes,
            "max_concurrent_jobs": settings.plan_pro_max_concurrent_jobs,
            "storage_limit_bytes": int(settings.plan_pro_storage_limit_gb) * 1024 * 1024 * 1024,
        }
    return {
        "monthly_minutes": settings.plan_free_monthly_minutes,
        "max_concurrent_jobs": settings.plan_free_max_concurrent_jobs,
        "storage_limit_bytes": int(settings.plan_free_storage_limit_gb) * 1024 * 1024 * 1024,
    }


def get_plan_policy(user_id: str) -> dict:
    return _plan_policy(user_id)


def reserve_job_capacity(
    *,
    user_id: str,
    source_key: str,
    requested_minutes: float,
    reserved_storage_bytes: int,
) -> dict:
    policy = _plan_policy(user_id)
    return reserve_and_create_job_atomic(
        user_id=user_id,
        source_key=source_key,
        requested_minutes=requested_minutes,
        reserved_storage_bytes=reserved_storage_bytes,
        monthly_minutes_limit=policy["monthly_minutes"],
        max_concurrent_jobs=policy["max_concurrent_jobs"],
        storage_limit_bytes=policy["storage_limit_bytes"],
    )


def get_usage_summary(user_id: str) -> dict:
    policy = _plan_policy(user_id)
    usage = get_usage_totals(user_id)
    return {
        "monthly_minutes": policy["monthly_minutes"],
        "max_concurrent_jobs": policy["max_concurrent_jobs"],
        "storage_limit_bytes": policy["storage_limit_bytes"],
        "used_minutes": round(usage["total_minutes"], 3),
        "remaining_minutes": round(max(0.0, policy["monthly_minutes"] - usage["total_minutes"]), 3),
    }

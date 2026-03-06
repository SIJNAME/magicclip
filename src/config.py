import os


def _int_env(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    return int(raw)


class Settings:
    database_url = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/saas_core")
    db_pool_min = _int_env("DB_POOL_MIN", 2)
    db_pool_max = _int_env("DB_POOL_MAX", 20)
    jwt_secret = os.getenv("JWT_SECRET")
    jwt_algorithm = os.getenv("JWT_ALGORITHM", "HS256")
    jwt_expires_minutes = _int_env("JWT_EXPIRES_MINUTES", 60 * 24)

    redis_url = os.getenv("REDIS_URL")
    queue_name = os.getenv("QUEUE_NAME", "saas-core")
    dead_letter_queue_name = os.getenv("DEAD_LETTER_QUEUE_NAME", "saas-core-dead-letter")

    stripe_api_key = os.getenv("STRIPE_API_KEY")
    stripe_webhook_secret = os.getenv("STRIPE_WEBHOOK_SECRET")

    s3_endpoint_url = os.getenv("S3_ENDPOINT_URL")
    s3_region = os.getenv("S3_REGION", "us-east-1")
    s3_access_key_id = os.getenv("S3_ACCESS_KEY_ID")
    s3_secret_access_key = os.getenv("S3_SECRET_ACCESS_KEY")
    s3_bucket = os.getenv("S3_BUCKET")
    s3_input_prefix = os.getenv("S3_INPUT_PREFIX", "inputs")
    s3_output_prefix = os.getenv("S3_OUTPUT_PREFIX", "outputs")

    openai_api_key = os.getenv("OPENAI_API_KEY")
    groq_api_key = os.getenv("GROQ_API_KEY")
    stt_model = os.getenv("STT_MODEL", "gpt-4o-mini-transcribe")
    clip_model = os.getenv("CLIP_MODEL", "gpt-4o-mini")

    plan_free_monthly_minutes = _int_env("PLAN_FREE_MONTHLY_MINUTES", 60)
    plan_free_max_concurrent_jobs = _int_env("PLAN_FREE_MAX_CONCURRENT_JOBS", 1)
    plan_free_storage_limit_gb = _int_env("PLAN_FREE_STORAGE_LIMIT_GB", 5)

    plan_pro_monthly_minutes = _int_env("PLAN_PRO_MONTHLY_MINUTES", 300)
    plan_pro_max_concurrent_jobs = _int_env("PLAN_PRO_MAX_CONCURRENT_JOBS", 3)
    plan_pro_storage_limit_gb = _int_env("PLAN_PRO_STORAGE_LIMIT_GB", 100)
    stripe_price_free = os.getenv("STRIPE_PRICE_FREE", "")
    stripe_price_pro = os.getenv("STRIPE_PRICE_PRO", "")

    job_heartbeat_sec = _int_env("JOB_HEARTBEAT_SEC", 10)
    job_timeout_sec = _int_env("JOB_TIMEOUT_SEC", 600)
    job_max_attempts = _int_env("JOB_MAX_ATTEMPTS", 3)

    rate_limit_per_minute = _int_env("RATE_LIMIT_PER_MINUTE", 60)
    rate_limit_window_sec = _int_env("RATE_LIMIT_WINDOW_SEC", 60)


settings = Settings()

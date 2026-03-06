from fastapi import FastAPI
from redis import Redis

from src.auth.routes import router as auth_router
from src.billing.routes import router as billing_router
from src.config import settings
from src.db import init_db
from src.db.database import close_pool, get_connection
from src.logging_config import configure_logging
from src.pipeline.routes import router as pipeline_router
from src.usage.routes import router as usage_router

app = FastAPI(title="Minimal SaaS Core")


@app.on_event("startup")
def startup() -> None:
    configure_logging()
    init_db()


@app.on_event("shutdown")
def shutdown() -> None:
    close_pool()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/ready")
def ready() -> dict[str, str]:
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            _ = cur.fetchone()
        conn.commit()
    if settings.redis_url:
        Redis.from_url(settings.redis_url).ping()
    return {"status": "ready"}


app.include_router(auth_router)
app.include_router(billing_router)
app.include_router(usage_router)
app.include_router(pipeline_router)

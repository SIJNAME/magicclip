from fastapi import FastAPI

from backend.api.clips import router as clips_router
from backend.api.projects import router as projects_router
from backend.api.render import router as render_router
from backend.db import init_db
from backend.video_renderer.worker import RenderWorker

app = FastAPI()
_worker: RenderWorker | None = None


@app.on_event("startup")
def startup():
    global _worker
    init_db()
    if _worker is None:
        _worker = RenderWorker()
        _worker.start()


@app.on_event("shutdown")
def shutdown():
    global _worker
    if _worker is not None:
        _worker.stop()
        _worker = None


app.include_router(projects_router, prefix="/projects", tags=["Projects"])
app.include_router(clips_router, prefix="/clips", tags=["Clips"])
app.include_router(render_router, prefix="/render", tags=["Render"])


@app.get("/")
def root():
    return {"status": "MagicClip AI Running"}

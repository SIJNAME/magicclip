from fastapi import FastAPI
from api.projects import router as projects_router
from db import init_db

app = FastAPI()

@app.on_event("startup")
def startup():
    init_db()


app.include_router(projects_router, prefix="/projects", tags=["Projects"])

@app.get("/")
def root():
    return {"status": "MagicClip AI Running"}

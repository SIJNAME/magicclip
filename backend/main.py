from fastapi import FastAPI
from api.clips import router as clips_router

app = FastAPI()

app.include_router(clips_router, prefix="/clips", tags=["Clips"])

@app.get("/")
def root():
    return {"status": "MagicClip AI Running"}
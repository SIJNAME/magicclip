import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.api.render import router as render_router
from backend.core.project_service import update_video_render_job
from backend.db import get_connection, init_db


class RenderApiTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        init_db()
        app = FastAPI()
        app.include_router(render_router, prefix="/render")
        cls.client = TestClient(app)

    def _seed(self):
        pid = str(uuid4())
        cid = str(uuid4())
        with tempfile.TemporaryDirectory() as td:
            pass
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO projects (id,name,source_type,input_file,video_file,status,transcript_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (pid, "proj", "upload", "/tmp/in.mp4", "/tmp/in.mp4", "ready", "[]", "2024-01-01", "2024-01-01"),
            )
            conn.execute(
                "INSERT INTO clips (id,project_id,start_time,end_time,score,title,summary,created_at) VALUES (?,?,?,?,?,?,?,?)",
                (cid, pid, 2.0, 8.0, 90, "t", "s", "2024-01-01"),
            )
            conn.commit()
        return pid, cid

    def test_render_endpoints(self):
        pid, cid = self._seed()
        resp = self.client.post(
            "/render",
            json={"project_id": pid, "clip_id": cid, "start": 2.0, "end": 8.0},
        )
        self.assertEqual(resp.status_code, 200)
        job = resp.json()
        self.assertIn(job["status"], {"queued", "processing"})
        job_id = job["id"]

        status = self.client.get(f"/render/{job_id}/status")
        self.assertEqual(status.status_code, 200)

    def test_download_endpoint(self):
        pid, cid = self._seed()
        resp = self.client.post(
            "/render",
            json={"project_id": pid, "clip_id": cid, "start": 2.0, "end": 8.0},
        )
        job = resp.json()
        job_id = job["id"]

        with tempfile.TemporaryDirectory() as td:
            output_path = str(Path(td) / "out.mp4")
            Path(output_path).write_bytes(b"video")
            update_video_render_job(job_id, status="completed", output_url=f"/render/{job_id}/download")
            with get_connection() as conn:
                conn.execute("UPDATE video_render_jobs SET output_file = ? WHERE id = ?", (output_path, job_id))
                conn.commit()
            dl = self.client.get(f"/render/{job_id}/download")
            self.assertEqual(dl.status_code, 200)


if __name__ == "__main__":
    unittest.main()

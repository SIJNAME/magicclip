import unittest
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.api.clips import router as clips_router
from backend.core.intelligence.scoring import combine_with_retention_model
from backend.db import get_connection, init_db


class SelfLearningTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        init_db()
        app = FastAPI()
        app.include_router(clips_router, prefix="/clips")
        cls.client = TestClient(app)

    def _seed_clip(self):
        pid = str(uuid4())
        cid = str(uuid4())
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO projects (id,name,source_type,status,transcript_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?)",
                (pid, "proj", "upload", "ready", "[]", "2024-01-01", "2024-01-01"),
            )
            conn.execute(
                "INSERT INTO clips (id,project_id,start_time,end_time,score,title,summary,created_at) VALUES (?,?,?,?,?,?,?,?)",
                (cid, pid, 0.0, 10.0, 80, "t", "s", "2024-01-01"),
            )
            conn.commit()
        return cid

    def test_combined_score_formula(self):
        score = combine_with_retention_model(80, 70, 60, 90)
        self.assertEqual(score, 74)

    def test_performance_endpoint(self):
        cid = self._seed_clip()
        resp = self.client.post(
            f"/clips/{cid}/performance",
            json={"avg_watch_time": 8.0, "completion_rate": 0.5, "rewatch_rate": 0.2},
        )
        self.assertEqual(resp.status_code, 200)
        payload = resp.json()
        self.assertAlmostEqual(payload["engagementScore"], 0.47, places=2)


if __name__ == "__main__":
    unittest.main()

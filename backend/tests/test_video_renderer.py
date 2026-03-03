import shutil
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch
from uuid import uuid4

from core.project_service import get_video_render_job
from db import get_connection, init_db
from video_renderer.caption_engine import render_ass_subtitles
from video_renderer.ffmpeg_helpers import (
    ass_to_ffmpeg_filter,
    cut_and_crop,
    encode_for_tiktok,
    ffprobe_video_info,
    overlay_subtitles,
)
from video_renderer.smart_crop import build_smart_crop_filter
from video_renderer.worker import enqueue_render_job, process_job_by_id

try:
    import cv2
    import numpy as np
except Exception:  # pragma: no cover
    cv2 = None
    np = None


class VideoRendererTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        init_db()
        cls.ffmpeg_available = shutil.which("ffmpeg") is not None and shutil.which("ffprobe") is not None

    def _seed_project_clip(self):
        pid = str(uuid4())
        cid = str(uuid4())
        with get_connection() as conn:
            conn.execute(
                "INSERT INTO projects (id,name,source_type,input_file,video_file,status,transcript_json,created_at,updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (pid, "p", "upload", "/tmp/in.mp4", "/tmp/in.mp4", "ready", "[]", "2024-01-01", "2024-01-01"),
            )
            conn.execute(
                "INSERT INTO clips (id,project_id,start_time,end_time,score,title,summary,created_at) VALUES (?,?,?,?,?,?,?,?)",
                (cid, pid, 1.0, 9.0, 88, "t", "s", "2024-01-01"),
            )
            conn.commit()
        return pid, cid

    def _make_static_video(self, path: str, width: int = 640, height: int = 360, frames: int = 30):
        if cv2 is None or np is None:
            self.skipTest("opencv/numpy unavailable for static video test")
        writer = cv2.VideoWriter(path, cv2.VideoWriter_fourcc(*"mp4v"), 15.0, (width, height))
        for i in range(frames):
            frame = np.zeros((height, width, 3), dtype=np.uint8)
            cv2.rectangle(frame, (220, 80), (420, 300), (255, 255, 255), -1)
            writer.write(frame)
        writer.release()

    def test_smart_crop_with_static_video(self):
        with tempfile.TemporaryDirectory() as td:
            vid = str(Path(td) / "static.mp4")
            self._make_static_video(vid)
            vf = build_smart_crop_filter(vid)
            self.assertIn("scale=1080:1920", vf)

    def test_subtitle_timing_matches_word_timestamps(self):
        words = [
            {"word": "hello", "startTime": 1.0, "endTime": 1.5},
            {"word": "world", "startTime": 1.5, "endTime": 2.0},
        ]
        with tempfile.TemporaryDirectory() as td:
            ass_path = str(Path(td) / "sub.ass")
            render_ass_subtitles(words, start=1.0, end=3.0, output_ass_path=ass_path)
            content = Path(ass_path).read_text(encoding="utf-8")
            self.assertIn("0:00:00.00", content)
            self.assertIn("0:00:01.00", content)

    def test_fallback_center_crop_works(self):
        vf = build_smart_crop_filter("/non/existing.mp4")
        self.assertIn("scale=1080:1920", vf)

    @patch("video_renderer.ffmpeg_helpers.run_ffmpeg", return_value=(0, "", ""))
    def test_ffmpeg_stage_helpers(self, mocked_run):
        cut_and_crop("in.mp4", 1.0, 4.0, "crop=ih*(9/16):ih,scale=1080:1920", "raw.mp4")
        overlay_subtitles("raw.mp4", "sub.ass", "subtitled.mp4")
        encode_for_tiktok("subtitled.mp4", "out.mp4", crf=19)
        self.assertEqual(mocked_run.call_count, 3)

    def test_ffmpeg_output_resolution_1080x1920(self):
        if not self.ffmpeg_available:
            self.skipTest("ffmpeg/ffprobe unavailable")
        with tempfile.TemporaryDirectory() as td:
            src = str(Path(td) / "src.mp4")
            if cv2 is None or np is None:
                self.skipTest("opencv/numpy unavailable")
            self._make_static_video(src, width=1280, height=720, frames=45)
            out = str(Path(td) / "out.mp4")
            code, _, _ = encode_for_tiktok(src, out, crf=20)
            self.assertEqual(code, 0)
            info = ffprobe_video_info(out)
            self.assertEqual(info["width"], 1080)
            self.assertEqual(info["height"], 1920)

    def test_end_to_end_render_and_concurrent_jobs(self):
        pid, cid = self._seed_project_clip()
        with tempfile.TemporaryDirectory() as td:
            out1 = str(Path(td) / "render1.mp4")
            out2 = str(Path(td) / "render2.mp4")
            j1 = enqueue_render_job(project_id=pid, clip_id=cid, output_file=out1)
            j2 = enqueue_render_job(project_id=pid, clip_id=cid, output_file=out2)

            def fake_renderer(**kwargs):
                Path(kwargs["output_file"]).parent.mkdir(parents=True, exist_ok=True)
                Path(kwargs["output_file"]).write_bytes(b"abcd")
                return {"status": "completed", "log": "ok", "output_file": kwargs["output_file"]}

            t1 = threading.Thread(target=process_job_by_id, args=(j1["id"], fake_renderer))
            t2 = threading.Thread(target=process_job_by_id, args=(j2["id"], fake_renderer))
            t1.start(); t2.start(); t1.join(); t2.join()

            r1 = get_video_render_job(j1["id"])
            r2 = get_video_render_job(j2["id"])
            self.assertEqual(r1["status"], "completed")
            self.assertEqual(r2["status"], "completed")
            self.assertEqual(r1["progress"], 1.0)
            self.assertEqual(r2["progress"], 1.0)
            self.assertIsNotNone(r1["output_file_size"])
            self.assertIsNotNone(r2["output_file_size"])


if __name__ == "__main__":
    unittest.main()

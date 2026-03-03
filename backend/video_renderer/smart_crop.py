from __future__ import annotations

import logging
from collections import deque

from backend.video_renderer.ffmpeg_helpers import ffprobe_video_info

try:
    import cv2
except Exception:  # pragma: no cover
    cv2 = None

logger = logging.getLogger(__name__)
TARGET_RATIO = 9 / 16


def _center_crop_filter(width: int, height: int) -> str:
    if width <= 0 or height <= 0:
        return "crop=ih*9/16:ih:(iw-ih*9/16)/2:0,scale=1080:1920"
    input_ratio = width / height
    if input_ratio > TARGET_RATIO:
        crop_w = int(height * TARGET_RATIO)
        return f"crop={crop_w}:{height}:(iw-{crop_w})/2:0,scale=1080:1920"
    crop_h = int(width / TARGET_RATIO)
    return f"crop={width}:{crop_h}:0:(ih-{crop_h})/2,scale=1080:1920"


def _moving_average(points: list[tuple[int, int]], window: int = 7) -> list[tuple[int, int]]:
    out: list[tuple[int, int]] = []
    q: deque[tuple[int, int]] = deque(maxlen=window)
    for p in points:
        q.append(p)
        avg_x = int(sum(x for x, _ in q) / len(q))
        avg_y = int(sum(y for _, y in q) / len(q))
        out.append((avg_x, avg_y))
    return out


def _detect_subject_centers(video_path: str, sample_stride: int = 3, max_frames: int = 600) -> tuple[list[tuple[int, int]], int, int, int]:
    if cv2 is None:
        return [], 0, 0, 0

    cap = cv2.VideoCapture(video_path)
    if not cap.isOpened():
        return [], 0, 0, 0

    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH) or 0)
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT) or 0)
    face = cv2.CascadeClassifier(cv2.data.haarcascades + "haarcascade_frontalface_default.xml")

    centers: list[tuple[int, int]] = []
    frame_idx = 0
    inspected = 0
    while frame_idx < max_frames:
        ok, frame = cap.read()
        if not ok:
            break
        if frame_idx % sample_stride != 0:
            frame_idx += 1
            continue

        inspected += 1
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        faces = face.detectMultiScale(gray, scaleFactor=1.1, minNeighbors=4, minSize=(40, 40))
        if len(faces) > 0:
            x, y, w, h = max(faces, key=lambda b: b[2] * b[3])
            centers.append((x + w // 2, y + h // 2))
        frame_idx += 1

    cap.release()
    return centers, width, height, inspected


def build_smart_crop_filter(video_path: str) -> str:
    centers, width, height, inspected = _detect_subject_centers(video_path)
    logger.info("smart_crop_detection", extra={"video": video_path, "detection_frames": inspected, "detections": len(centers)})
    if not centers:
        info = ffprobe_video_info(video_path)
        fallback = _center_crop_filter(info["width"], info["height"])
        logger.info("smart_crop_decision", extra={"mode": "center_fallback", "filter": fallback})
        return fallback

    smoothed = _moving_average(centers)
    center_x = int(sum(x for x, _ in smoothed) / len(smoothed))

    crop_w = int(height * TARGET_RATIO)
    if crop_w <= 0 or width <= 0 or height <= 0:
        fallback = _center_crop_filter(width, height)
        logger.info("smart_crop_decision", extra={"mode": "invalid_dimensions_fallback", "filter": fallback})
        return fallback

    x = max(0, min(width - crop_w, center_x - crop_w // 2))
    decided = f"crop={crop_w}:{height}:{x}:0,scale=1080:1920"
    logger.info("smart_crop_decision", extra={"mode": "tracked", "crop_x": x, "crop_w": crop_w, "filter": decided})
    return decided

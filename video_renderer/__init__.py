"""Compatibility package exposing backend.video_renderer as top-level video_renderer."""

from backend import video_renderer as _backend_video_renderer

__path__ = _backend_video_renderer.__path__

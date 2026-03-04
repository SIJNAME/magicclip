"""Compatibility ASGI entrypoint for `python -m uvicorn main:app --reload`."""

from backend.main import app

__all__ = ["app"]

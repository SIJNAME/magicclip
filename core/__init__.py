"""Compatibility package exposing backend.core as top-level core."""

from backend import core as _backend_core

__path__ = _backend_core.__path__

"""Compatibility package exposing backend.api as top-level api."""

from backend import api as _backend_api

__path__ = _backend_api.__path__

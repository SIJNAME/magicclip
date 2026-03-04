"""Compatibility package exposing backend.schemas as top-level schemas."""

from backend import schemas as _backend_schemas

__path__ = _backend_schemas.__path__

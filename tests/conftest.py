"""
Shared test configuration.

Register sfm_data_provider as a namespace package WITHOUT executing its
__init__.py, AND stub heavy third-party dependencies so that transitive
imports (instrument_factory → client → bloomberg → blpapi) don't fail.
"""
import pathlib
import sys
import types
from unittest.mock import MagicMock

# ---------------------------------------------------------------------------
# 1. Stub ALL heavy third-party modules that appear in the import chain.
#    Use a special AutoMock that supports arbitrary submodule access.
# ---------------------------------------------------------------------------
class _DeepMock(MagicMock):
    """MagicMock that returns itself for any attribute / submodule access."""
    def __getattr__(self, name):
        # Avoid infinite recursion on dunder attrs
        if name.startswith("_"):
            return super().__getattr__(name)
        child = _DeepMock()
        setattr(self, name, child)
        return child

_STUBS = [
    "blpapi",
    "joblib",
    "tqdm", "tqdm.auto",
    # sfm_datalibrary and all its known submodules
    "sfm_datalibrary",
    "sfm_datalibrary.connections",
    "sfm_datalibrary.connections.db_connections",
    "sfm_datalibrary.connections.db_connections.DbConnectionParameters",
    "sfm_datalibrary.connections.db_connections.DatabaseTypes",
    "sfm_datalibrary.connections.db_connections.TimescaleConnection",
    "sfm_datalibrary.connections.redis_connections",
    "sfm_datalibrary.connections.redis_connections.RedisConnectionParameters",
    "sfm_datalibrary.connections.redis_connections.RedisConnection",
]

for mod in _STUBS:
    if mod not in sys.modules:
        sys.modules[mod] = _DeepMock()

# ---------------------------------------------------------------------------
# 2. Register sfm_data_provider as a bare namespace package so its
#    __init__.py (which pulls in bloomberg, oracle, etc.) is NOT executed.
# ---------------------------------------------------------------------------
_SRC = pathlib.Path(__file__).resolve().parent.parent / "src"

def _ensure_namespace(dotted: str) -> types.ModuleType:
    parts = dotted.split(".")
    for i in range(1, len(parts) + 1):
        name = ".".join(parts[:i])
        if name not in sys.modules:
            mod = types.ModuleType(name)
            mod.__path__ = [str(_SRC / name.replace(".", "/"))]
            mod.__package__ = name
            sys.modules[name] = mod
    return sys.modules[dotted]

_ensure_namespace("sfm_data_provider")

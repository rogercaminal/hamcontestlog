"""
Ham contest log analysis toolkit.

Loads contest logs + RBN data into DuckDB and provides analysis utilities.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("hamcontestlog")
except PackageNotFoundError:  # pragma: no cover - during development
    __version__ = "0.0.0"


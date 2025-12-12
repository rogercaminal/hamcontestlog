# tests/conftest.py
from __future__ import annotations

import os
from pathlib import Path

import pytest


@pytest.fixture()
def temp_db_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """
    Provide an isolated DuckDB file for each test.

    The package reads DB location from HAMCONTESTLOG_DB. We set that to a temp path
    so tests do not touch the user's real ~/.hamcontestlog database.
    """
    db_path = tmp_path / "hamcontestlog_test.duckdb"
    monkeypatch.setenv("HAMCONTESTLOG_DB", str(db_path))

    # Reset schema initialization flag between tests
    import hamcontestlog.db as db

    db._SCHEMA_INITIALIZED = False  # type: ignore[attr-defined]

    return db_path

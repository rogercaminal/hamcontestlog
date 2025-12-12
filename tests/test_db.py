# tests/test_db.py
from __future__ import annotations

from hamcontestlog.db import connect


def test_db_connect_creates_schema(temp_db_path) -> None:
    """
    connect() should create the schema on first use.
    """
    with connect() as con:
        # Check a few core tables exist
        tables = {
            r[0]
            for r in con.execute("SHOW TABLES;").fetchall()
        }

    assert "contests" in tables
    assert "logs" in tables
    assert "qsos" in tables
    assert "call_enrichment" in tables
    assert "qso_scoring" in tables
    assert "rbn_spots" in tables


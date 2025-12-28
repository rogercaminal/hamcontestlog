# src/hamcontestlog/db.py
from __future__ import annotations

from contextlib import contextmanager
from pathlib import Path
from typing import Iterator, Optional
import os

import duckdb

# Default DB location under home
DEFAULT_DB_PATH = Path.home() / ".hamcontestlog" / "hamcontestlog.duckdb"
DB_ENV_VAR = "HAMCONTESTLOG_DB"

# Module-level flag so we don't re-run schema creation unnecessarily
_SCHEMA_INITIALIZED = False


def _resolve_db_path(explicit: Optional[Path] = None) -> Path:
    """
    Determine which DB path to use, in this order:

    1. Explicit path (if provided)
    2. HAMCONTESTLOG_DB environment variable (if set)
    3. Default path under ~/.hamcontestlog/hamcontestlog.duckdb
    """
    if explicit is not None:
        return explicit

    env = os.getenv(DB_ENV_VAR)
    if env:
        return Path(env)

    return DEFAULT_DB_PATH


def init_db(db_path: Optional[Path] = None) -> Path:
    """
    Ensure the DB file and schema exist; return the resolved path.

    You *can* call this at program start if you want an explicit path,
    but it's not required because connect() also ensures the schema.
    """
    path = _resolve_db_path(db_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Open once and ensure schema
    con = duckdb.connect(str(path))
    try:
        _create_schema(con)
    finally:
        con.close()

    return path


@contextmanager
def connect(path: Optional[Path] = None) -> Iterator[duckdb.DuckDBPyConnection]:
    """
    Context manager for a DuckDB connection.

    - Resolves DB path (explicit → env var → default)
    - Ensures parent directory exists
    - Ensures schema exists on first use
    """
    global _SCHEMA_INITIALIZED

    real_path = _resolve_db_path(path)
    real_path.parent.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(real_path))

    if not _SCHEMA_INITIALIZED:
        _create_schema(con)
        _SCHEMA_INITIALIZED = True

    try:
        yield con
    finally:
        con.close()


def _create_schema(con: duckdb.DuckDBPyConnection) -> None:
    """
    Create tables and sequences if they do not exist.
    Safe to call multiple times.
    """

    # Contests
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS contests (
            contest_id  TEXT PRIMARY KEY,
            name        TEXT,
            sponsor     TEXT,
            mode        TEXT,
            start_time  TIMESTAMP,
            end_time    TIMESTAMP,
            bands       TEXT,
            metadata    JSON
        );
        """
    )

    # Logs (one per submitted log)
    con.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS log_seq;
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS logs (
            log_id          BIGINT PRIMARY KEY DEFAULT nextval('log_seq'),
            contest_id      TEXT REFERENCES contests(contest_id),
            callsign        TEXT,
            operator        TEXT,
            club            TEXT,
            category        TEXT,
            claimed_score   BIGINT,
            raw_metadata    JSON
        );
        """
    )

    # QSOs
    con.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS qso_seq;
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS qsos (
            qso_id          BIGINT PRIMARY KEY DEFAULT nextval('qso_seq'),
            log_id          BIGINT REFERENCES logs(log_id),
            qso_time        TIMESTAMP,
            band            TEXT,
            mode            TEXT,
            freq_hz         DOUBLE,
            my_call         TEXT,
            their_call      TEXT,
            rst_sent        TEXT,
            rst_rcvd        TEXT,
            exch_sent       TEXT,
            exch_rcvd       TEXT,
            mult_flags      JSON
        );
        """
    )

    # RBN
    con.execute(
        """
        CREATE SEQUENCE IF NOT EXISTS rbn_spot_seq;
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS rbn_spots (
            spot_id         BIGINT PRIMARY KEY DEFAULT nextval('rbn_spot_seq'),
            contest_id      TEXT REFERENCES contests(contest_id),
            spot_time       TIMESTAMP,
            spotter_call    TEXT,
            dx_call         TEXT,
            freq_hz         DOUBLE,
            snr_db          DOUBLE,
            speed_wpm       DOUBLE,
            band            TEXT
        );
        """
    )
    # Call enrichment: per callsign static info (contest-agnostic)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS call_enrichment (
            callsign   TEXT PRIMARY KEY,
            dxcc       TEXT,
            cq_zone    INTEGER,
            itu_zone   INTEGER,
            continent  TEXT,
            prefix     TEXT,
            state      TEXT,
            province   TEXT
        );
        """
    )

    # QSO scoring/multipliers: per QSO, contest-specific
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS qso_scoring (
            qso_id          BIGINT PRIMARY KEY,
            contest_id      TEXT,
            dxcc            TEXT,
            cq_zone         INTEGER,
            itu_zone        INTEGER,
            prefix          TEXT,
            state           TEXT,
            province        TEXT,
            hq              TEXT,
            points          INTEGER,
            is_mult_dxcc    BOOLEAN,
            is_mult_cq_zone BOOLEAN,
            is_mult_itu     BOOLEAN,
            is_mult_prefix  BOOLEAN,
            is_mult_state   BOOLEAN,
            is_mult_hq      BOOLEAN
        );
        """
    )
    con.execute(
        """
        ALTER TABLE qso_scoring ADD COLUMN IF NOT EXISTS hq TEXT;
        """
    )
    con.execute(
        """
        ALTER TABLE qso_scoring ADD COLUMN IF NOT EXISTS is_mult_hq BOOLEAN DEFAULT FALSE;
        """
    )


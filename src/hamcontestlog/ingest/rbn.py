# src/hamcontestlog/ingest/rbn.py
from __future__ import annotations

from datetime import datetime
from typing import Tuple
from pathlib import Path
import tempfile
import zipfile
import requests

from ..db import connect
from ..fetch.rbn import rbn_zip_urls_for_contest


def _get_contest_window(contest_id: str) -> Tuple[datetime, datetime]:
    """
    Read start_time and end_time for a contest from the DuckDB 'contests' table.
    """
    with connect() as con:
        row = con.execute(
            """
            SELECT start_time, end_time
            FROM contests
            WHERE contest_id = ?;
            """,
            [contest_id],
        ).fetchone()

    if row is None:
        raise ValueError(f"Contest '{contest_id}' not found in contests table.")

    start, end = row
    if not isinstance(start, datetime) or not isinstance(end, datetime):
        raise TypeError(
            f"Invalid start/end in contests for '{contest_id}': {start!r}, {end!r}"
        )

    return start, end


def ingest_rbn_for_contest(contest_id: str) -> int:
    """
    Fetch and insert RBN spots for the contest window.

    Uses DuckDB's read_csv_auto to bulk-load the daily CSVs.

    Returns number of rows inserted (approximate, from counting before/after).
    """
    start, end = _get_contest_window(contest_id)
    urls = rbn_zip_urls_for_contest(start, end)

    total_inserted = 0

    with connect() as con:
        # Get initial count so we can compute how many we add
        before = con.execute(
            "SELECT COUNT(*) FROM rbn_spots WHERE contest_id = ?;",
            [contest_id],
        ).fetchone()[0]

        for url in urls:
            print(f"[RBN] Processing {url}")

            try:
                resp = requests.get(url, timeout=120)
            except Exception as e:
                print(f"[RBN]   -> request failed: {e}")
                continue

            if resp.status_code != 200:
                print(f"[RBN]   -> HTTP {resp.status_code}, skipping")
                continue

            # Work in a temp directory for this file
            with tempfile.TemporaryDirectory() as tmpdir:
                tmpdir_path = Path(tmpdir)

                # Write ZIP to disk
                zip_path = tmpdir_path / "rbn.zip"
                zip_path.write_bytes(resp.content)

                # Extract first CSV-like file from ZIP
                with zipfile.ZipFile(zip_path) as zf:
                    names = [n for n in zf.namelist() if not n.endswith("/")]
                    if not names:
                        print("[RBN]   -> no files in ZIP, skipping")
                        continue

                    inner_name = names[0]
                    csv_path = tmpdir_path / inner_name
                    csv_path.write_bytes(zf.read(inner_name))

                print(f"[RBN]   -> extracted to {csv_path}")

                # Now let DuckDB read the CSV and insert rows in bulk
                # CSV columns (from your sample):
                # callsign,de_pfx,de_cont,freq,band,dx,dx_pfx,dx_cont,
                # mode,db,date,speed,tx_mode
                con.execute(
                    """
                    INSERT INTO rbn_spots (
                        contest_id, spot_time,
                        spotter_call, dx_call, freq_hz,
                        snr_db, speed_wpm, band
                    )
                    SELECT
                        ? AS contest_id,
                        "date" AS spot_time,
                        callsign AS spotter_call,
                        dx AS dx_call,
                        freq * 1000.0 AS freq_hz,
                        db AS snr_db,
                        speed AS speed_wpm,
                        band
                    FROM read_csv(
                        ?,
                        header=true,
                        columns={
                            'callsign': 'VARCHAR',
                            'de_pfx': 'VARCHAR',
                            'de_cont': 'VARCHAR',
                            'freq': 'DOUBLE',
                            'band': 'VARCHAR',
                            'dx': 'VARCHAR',
                            'dx_pfx': 'VARCHAR',
                            'dx_cont': 'VARCHAR',
                            'mode': 'VARCHAR',
                            'db': 'DOUBLE',
                            'date': 'TIMESTAMP',
                            'speed': 'DOUBLE',
                            'tx_mode': 'VARCHAR'
                        },
                        ignore_errors=true,
                        null_padding=true,
                        strict_mode=false
                    )
                    WHERE "date" BETWEEN ? AND ?;
                    """,
                    [
                        contest_id,
                        str(csv_path),
                        start,
                        end,
                    ],
                )

        after = con.execute(
            "SELECT COUNT(*) FROM rbn_spots WHERE contest_id = ?;",
            [contest_id],
        ).fetchone()[0]

        total_inserted = after - before

    return total_inserted


# src/hamcontestlog/ingest/rbn.py
from typing import Iterable

from ..db import connect


def ingest_rbn_urls(contest_id: str, urls: Iterable[str]) -> None:
    """
    For each URL, use DuckDB to read CSV and append to rbn_spots.
    Expect columns: timestamp, spotter, dx, freq, snr, speed, band (or adapt).
    """
    with connect() as con:
        for url in urls:
            con.execute(
                """
                INSERT INTO rbn_spots (
                    contest_id, spot_time, spotter_call, dx_call,
                    freq_hz, snr_db, speed_wpm, band
                )
                SELECT
                    ? AS contest_id,
                    timestamp_col AS spot_time,   -- TODO: adapt column names
                    spotter_call,
                    dx_call,
                    freq_hz,
                    snr_db,
                    speed_wpm,
                    band
                FROM read_csv_auto(?, header=True);
                """,
                [contest_id, url],
            )


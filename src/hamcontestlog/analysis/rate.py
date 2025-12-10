# src/hamcontestlog/analysis/rate.py
import pandas as pd

from ..db import connect


def hourly_rate(contest_id: str, callsign: str) -> pd.DataFrame:
    with connect() as con:
        df = con.execute(
            """
            SELECT
                date_trunc('hour', qso_time) AS hour,
                count(*) AS qso_count
            FROM qsos q
            JOIN logs l ON q.log_id = l.log_id
            WHERE l.contest_id = ? AND l.callsign = ?
            GROUP BY hour
            ORDER BY hour;
            """,
            [contest_id, callsign],
        ).fetch_df()
    return df


def band_mode_breakdown(contest_id: str, callsign: str) -> pd.DataFrame:
    with connect() as con:
        df = con.execute(
            """
            SELECT
                band,
                mode,
                count(*) AS qsos
            FROM qsos q
            JOIN logs l ON q.log_id = l.log_id
            WHERE l.contest_id = ? AND l.callsign = ?
            GROUP BY band, mode
            ORDER BY band, mode;
            """,
            [contest_id, callsign],
        ).fetch_df()
    return df


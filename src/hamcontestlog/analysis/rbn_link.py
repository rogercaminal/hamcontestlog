# src/hamcontestlog/analysis/rbn_link.py
import pandas as pd

from ..db import connect


def rbn_matches_for_station(
    contest_id: str,
    callsign: str,
    max_time_delta_sec: int = 120,
    max_freq_delta_hz: float = 500.0,
) -> pd.DataFrame:
    """
    For each QSO, find nearest RBN spot for this station within a window.
    """
    with connect() as con:
        df = con.execute(
            """
            SELECT
                q.qso_id,
                q.qso_time,
                q.band,
                q.freq_hz,
                r.spot_time,
                r.spotter_call,
                r.snr_db,
                r.speed_wpm
            FROM qsos q
            JOIN logs l ON q.log_id = l.log_id
            LEFT JOIN rbn_spots r
                ON r.contest_id = l.contest_id
               AND r.dx_call = l.callsign
               AND abs(epoch(r.spot_time) - epoch(q.qso_time)) <= ?
               AND abs(r.freq_hz - q.freq_hz) <= ?
            WHERE l.contest_id = ? AND l.callsign = ?;
            """,
            [max_time_delta_sec, max_freq_delta_hz, contest_id, callsign],
        ).fetch_df()
    return df


import pandas as pd

from ..db import connect


def hourly_rate(contest_id: str, callsign: str) -> pd.DataFrame:
    """
    Return a DataFrame with hour, qso_count, and total_points (if scoring exists).
    """
    with connect() as con:
        df = con.execute(
            """
            SELECT
                date_trunc('hour', q.qso_time) AS hour,
                COUNT(*) AS qso_count,
                COALESCE(SUM(s.points), 0) AS total_points
            FROM qsos q
            JOIN logs l ON q.log_id = l.log_id
            LEFT JOIN qso_scoring s
                ON s.qso_id = q.qso_id
               AND s.contest_id = l.contest_id
            WHERE l.contest_id = ? AND l.callsign = ?
            GROUP BY hour
            ORDER BY hour;
            """,
            [contest_id, callsign],
        ).fetch_df()
    return df


def band_mode_breakdown(contest_id: str, callsign: str) -> pd.DataFrame:
    """
    Return band/mode breakdown, including total QSOs, points and multipliers.
    """
    with connect() as con:
        df = con.execute(
            """
            SELECT
                q.band,
                q.mode,
                COUNT(*) AS qsos,
                COALESCE(SUM(s.points), 0) AS points,
                SUM(CASE WHEN s.is_mult_dxcc THEN 1 ELSE 0 END) AS dxcc_mults,
                SUM(CASE WHEN s.is_mult_cq_zone THEN 1 ELSE 0 END) AS cq_zone_mults
            FROM qsos q
            JOIN logs l ON q.log_id = l.log_id
            LEFT JOIN qso_scoring s
                ON s.qso_id = q.qso_id
               AND s.contest_id = l.contest_id
            WHERE l.contest_id = ? AND l.callsign = ?
            GROUP BY q.band, q.mode
            ORDER BY q.band, q.mode;
            """,
            [contest_id, callsign],
        ).fetch_df()
    return df


# src/hamcontestlog/scoring/cqww.py
from __future__ import annotations

import duckdb

from ..db import connect


def score_cqww_station(contest_id: str, callsign: str) -> None:
    """
    Compute CQWW-style points and multiplier flags for one station in a contest.

    QSO points (simplified from official rules):

        - 0 points: same country
        - 2 points: North America to North America (different countries)
        - 1 point: same continent (non-NA), different countries
        - 3 points: different continents

    We rely on call_enrichment:
        - my_call     -> call_enrichment (my.*)
        - their_call  -> call_enrichment (dx.*)

    Multipliers:

        - is_mult_dxcc: first QSO (by time) with a given DXCC on a given band
        - is_mult_cq_zone: first QSO (by time) with a given CQ zone on a given band
    """
    with connect() as con:
        _set_points(con, contest_id, callsign)
        _reset_mult_flags(con, contest_id, callsign)
        _apply_cq_zones_from_exchange(con, contest_id, callsign)
        _set_dxcc_multipliers(con, contest_id, callsign)
        _set_cq_zone_multipliers(con, contest_id, callsign)


def _set_points(con: duckdb.DuckDBPyConnection, contest_id: str, callsign: str) -> None:
    """
    Set qso_scoring.points according to CQWW rules, using:

        my.dxcc, my.continent
        dx.dxcc, dx.continent

    Logic:

        - same country          -> 0 pts
        - NA to NA (diff country) -> 2 pts
        - same continent (non-NA) -> 1 pt
        - different continents     -> 3 pts
        - missing continent/dxcc   -> 0 pts
    """
    con.execute(
        """
        UPDATE qso_scoring AS s
        SET points = CASE
            -- Missing info: be conservative, 0 points
            WHEN my.continent IS NULL OR dx.continent IS NULL
                 OR my.dxcc IS NULL OR dx.dxcc IS NULL THEN 0

            -- Same country: 0 points
            WHEN my.dxcc = dx.dxcc THEN 0

            -- North America to North America (different countries): 2 points
            WHEN my.continent = 'NA' AND dx.continent = 'NA'
                 AND my.dxcc <> dx.dxcc THEN 2

            -- Same continent (non-NA), different countries: 1 point
            WHEN my.continent = dx.continent THEN 1

            -- Different continents: 3 points
            ELSE 3
        END
        FROM qsos q
        JOIN logs l ON q.log_id = l.log_id
        LEFT JOIN call_enrichment my ON l.callsign = my.callsign
        LEFT JOIN call_enrichment dx ON q.their_call = dx.callsign
        WHERE s.qso_id = q.qso_id
          AND s.contest_id = l.contest_id
          AND l.contest_id = ?
          AND l.callsign = ?;
        """,
        [contest_id, callsign],
    )


def _apply_cq_zones_from_exchange(con: duckdb.DuckDBPyConnection, contest_id: str, callsign: str) -> None:
    """
    For CQWW the zone multiplier comes from the copied exchange, not lookup.

    Normalize cq_zone from exch_rcvd when it parses as an integer, otherwise
    leave whatever enrichment provided as a fallback.
    """
    con.execute(
        """
        UPDATE qso_scoring AS s
        SET cq_zone = COALESCE(TRY_CAST(q.exch_rcvd AS INTEGER), s.cq_zone)
        FROM qsos q
        JOIN logs l ON q.log_id = l.log_id
        WHERE s.qso_id = q.qso_id
          AND s.contest_id = l.contest_id
          AND l.contest_id = ?
          AND l.callsign = ?;
        """,
        [contest_id, callsign],
    )


def _reset_mult_flags(con: duckdb.DuckDBPyConnection, contest_id: str, callsign: str) -> None:
    """
    Clear all multiplier flags for this station in this contest so we can recompute.
    """
    con.execute(
        """
        UPDATE qso_scoring AS s
        SET is_mult_dxcc = FALSE,
            is_mult_cq_zone = FALSE
        FROM qsos q
        JOIN logs l ON q.log_id = l.log_id
        WHERE s.qso_id = q.qso_id
          AND s.contest_id = l.contest_id
          AND l.contest_id = ?
          AND l.callsign = ?;
        """,
        [contest_id, callsign],
    )


def _set_dxcc_multipliers(con: duckdb.DuckDBPyConnection, contest_id: str, callsign: str) -> None:
    """
    Flag DXCC multipliers: first QSO with a given DXCC on each band.
    """
    con.execute(
        """
        WITH ordered AS (
            SELECT
                s.qso_id,
                ROW_NUMBER() OVER (
                    PARTITION BY s.contest_id, l.callsign, q.band, s.dxcc
                    ORDER BY q.qso_time, q.qso_id
                ) AS rn
            FROM qso_scoring s
            JOIN qsos q ON q.qso_id = s.qso_id
            JOIN logs l ON q.log_id = l.log_id
            WHERE s.contest_id = ?
              AND l.contest_id = ?
              AND l.callsign = ?
              AND s.dxcc IS NOT NULL
        )
        UPDATE qso_scoring AS s
        SET is_mult_dxcc = TRUE
        FROM ordered o
        WHERE s.qso_id = o.qso_id
          AND o.rn = 1;
        """,
        [contest_id, contest_id, callsign],
    )


def _set_cq_zone_multipliers(con: duckdb.DuckDBPyConnection, contest_id: str, callsign: str) -> None:
    """
    Flag CQ zone multipliers: first QSO with a given CQ zone on each band.
    """
    con.execute(
        """
        WITH ordered AS (
            SELECT
                s.qso_id,
                ROW_NUMBER() OVER (
                    PARTITION BY s.contest_id, l.callsign, q.band, s.cq_zone
                    ORDER BY q.qso_time, q.qso_id
                ) AS rn
            FROM qso_scoring s
            JOIN qsos q ON q.qso_id = s.qso_id
            JOIN logs l ON q.log_id = l.log_id
            WHERE s.contest_id = ?
              AND l.contest_id = ?
              AND l.callsign = ?
              AND s.cq_zone IS NOT NULL
        )
        UPDATE qso_scoring AS s
        SET is_mult_cq_zone = TRUE
        FROM ordered o
        WHERE s.qso_id = o.qso_id
          AND o.rn = 1;
        """,
        [contest_id, contest_id, callsign],
    )

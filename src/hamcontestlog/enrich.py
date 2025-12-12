# src/hamcontestlog/enrich.py
from __future__ import annotations

from typing import Iterable

import duckdb
from pyhamtools import LookupLib, Callinfo

from .db import connect


def _get_distinct_calls_for_contest(con: duckdb.DuckDBPyConnection, contest_id: str) -> list[str]:
    """
    Return distinct callsigns seen in QSOs for a given contest
    (both my_call and their_call).
    """
    rows = con.execute(
        """
        SELECT DISTINCT call FROM (
            SELECT q.their_call AS call
            FROM qsos q
            JOIN logs l ON q.log_id = l.log_id
            WHERE l.contest_id = ?

            UNION

            SELECT q.my_call AS call
            FROM qsos q
            JOIN logs l ON q.log_id = l.log_id
            WHERE l.contest_id = ?
        ) t
        WHERE call IS NOT NULL;
        """,
        [contest_id, contest_id],
    ).fetchall()
    return [r[0] for r in rows]


def _build_callinfo() -> Callinfo:
    """
    Construct a pyhamtools Callinfo object.

    You can customize LookupLib depending on what you prefer:
    - "countryfile" for a local cty.dat
    - "clublog" if you configure that, etc.

    For now we use the default "countryfile" lookup.
    """
    lookup = LookupLib(lookuptype="countryfile")
    return Callinfo(lookup)


def enrich_calls_for_contest(contest_id: str) -> None:
    """
    Enrich all distinct callsigns (their_call) for a contest.

    - Uses pyhamtools to get dxcc, zones, continent, prefix, etc.
    - Stores results in call_enrichment.
    """
    with connect() as con:
        calls = _get_distinct_calls_for_contest(con, contest_id)
        if not calls:
            return

        ci = _build_callinfo()

        records: list[tuple] = []
        for call in calls:
            try:
                info = ci.get_all(call)
            except Exception:
                # If lookup fails, store minimal info
                records.append((call, None, None, None, None, None, None, None))
                continue

            dxcc = info.get("country", None)        # or "dxcc" depending on your lookup
            cq_zone = info.get("cqz", None)
            itu_zone = info.get("ituz", None)
            continent = info.get("continent", None)
            prefix = info.get("prefix", None)
            state = info.get("state", None)
            province = info.get("province", None)

            records.append(
                (call, dxcc, cq_zone, itu_zone, continent, prefix, state, province)
            )

        # Upsert into call_enrichment
        con.executemany(
            """
            INSERT INTO call_enrichment (
                callsign, dxcc, cq_zone, itu_zone, continent, prefix, state, province
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (callsign) DO UPDATE SET
                dxcc      = EXCLUDED.dxcc,
                cq_zone   = EXCLUDED.cq_zone,
                itu_zone  = EXCLUDED.itu_zone,
                continent = EXCLUDED.continent,
                prefix    = EXCLUDED.prefix,
                state     = EXCLUDED.state,
                province  = EXCLUDED.province;
            """,
            records,
        )


def populate_qso_scoring_for_contest(contest_id: str) -> None:
    """
    Fill or refresh qso_scoring rows for a given contest.

    This step uses call_enrichment + qsos + logs and writes per-QSO info.
    It does NOT decide what multipliers 'count' yet; that’s scorer-specific.
    """
    with connect() as con:
        # Delete existing scoring rows for this contest (so we can re-run)
        con.execute(
            "DELETE FROM qso_scoring WHERE contest_id = ?;",
            [contest_id],
        )

        # Insert new rows based on current QSOs and enrichment
        con.execute(
            """
            INSERT INTO qso_scoring (
                qso_id, contest_id, dxcc, cq_zone, itu_zone, prefix, state, province, points,
                is_mult_dxcc, is_mult_cq_zone, is_mult_itu, is_mult_prefix, is_mult_state
            )
            SELECT
                q.qso_id,
                l.contest_id,
                e.dxcc,
                e.cq_zone,
                e.itu_zone,
                e.prefix,
                e.state,
                e.province,
                NULL::INTEGER AS points,
                FALSE AS is_mult_dxcc,
                FALSE AS is_mult_cq_zone,
                FALSE AS is_mult_itu,
                FALSE AS is_mult_prefix,
                FALSE AS is_mult_state
            FROM qsos q
            JOIN logs l ON q.log_id = l.log_id
            LEFT JOIN call_enrichment e
                ON q.their_call = e.callsign
            WHERE l.contest_id = ?;
            """,
            [contest_id],
        )


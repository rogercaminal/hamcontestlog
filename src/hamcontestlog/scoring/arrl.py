from __future__ import annotations

from typing import Dict, Optional, Set

from hamcontestlog.db import connect


WVE_DXCC = {"USA", "United States", "United States of America", "Canada"}


def _is_wve(dxcc: Optional[str]) -> bool:
    return bool(dxcc and dxcc.strip() in WVE_DXCC)


def score_arrldx_station(contest_id: str, callsign: str) -> Dict[str, int]:
    """
    ARRL DX scoring (CW and SSB):

    - Valid QSOs only between W/VE and DX
    - 3 points per valid QSO
    - Multipliers per band:
        * W/VE station: DXCC entities
        * DX station: US states + VE provinces
    """
    call = callsign.upper()

    with connect() as con:
        row = con.execute(
            "SELECT dxcc FROM call_enrichment WHERE callsign=?",
            [call],
        ).fetchone()
        my_is_wve = _is_wve(row[0] if row else None)

        qsos = con.execute(
            """
            SELECT
              q.qso_id,
              q.band,
              ce.dxcc,
              ce.state,
              ce.province
            FROM qsos q
            JOIN logs l ON q.log_id=l.log_id
            LEFT JOIN call_enrichment ce ON ce.callsign=q.their_call
            WHERE l.contest_id=? AND l.callsign=?
            ORDER BY q.qso_id
            """,
            [contest_id, call],
        ).fetchall()

        seen_dxcc: dict[str, Set[str]] = {}
        seen_state: dict[str, Set[str]] = {}

        total_pts = 0
        total_mults = 0
        updates = []

        for qso_id, band, dx_dxcc, dx_state, dx_prov in qsos:
            band = band or "UNKNOWN"
            seen_dxcc.setdefault(band, set())
            seen_state.setdefault(band, set())

            dx_is_wve = _is_wve(dx_dxcc)

            if my_is_wve == dx_is_wve:
                points = 0
                is_mult_dxcc = False
                is_mult_state = False
            else:
                points = 3
                is_mult_dxcc = False
                is_mult_state = False

                if my_is_wve:
                    if dx_dxcc and dx_dxcc not in seen_dxcc[band]:
                        seen_dxcc[band].add(dx_dxcc)
                        is_mult_dxcc = True
                else:
                    sp = dx_state or dx_prov
                    if sp and sp not in seen_state[band]:
                        seen_state[band].add(sp)
                        is_mult_state = True

            total_pts += points
            total_mults += int(is_mult_dxcc) + int(is_mult_state)

            updates.append((points, is_mult_dxcc, is_mult_state, contest_id, qso_id))

        con.executemany(
            """
            UPDATE qso_scoring
            SET points=?, is_mult_dxcc=?, is_mult_state=?
            WHERE contest_id=? AND qso_id=?
            """,
            updates,
        )

    return {
        "qso_points": total_pts,
        "multipliers": total_mults,
        "score": total_pts * total_mults,
    }


from __future__ import annotations

from typing import Dict, Optional, Set

from hamcontestlog.db import connect


def _is_hq_exchange(exch: Optional[str]) -> bool:
    """
    IARU exchange:
      - normal station: numeric ITU zone
      - HQ station: society abbreviation (non-numeric)
    """
    if not exch:
        return False
    return not exch.strip().isdigit()


def score_iaru_station(contest_id: str, callsign: str) -> Dict[str, int]:
    """
    IARU HF scoring (simplified but correct core rules):

    Points:
      - 1 pt: same ITU zone OR QSO with HQ station
      - 3 pts: different ITU zone, same continent
      - 5 pts: different continent

    Multipliers (per band):
      - ITU zones
      - HQ stations (society abbreviations)
    """
    call = callsign.upper()

    with connect() as con:
        row = con.execute(
            "SELECT continent, itu_zone FROM call_enrichment WHERE callsign=?",
            [call],
        ).fetchone()

        my_cont, my_itu = row if row else (None, None)

        qsos = con.execute(
            """
            SELECT
              q.qso_id,
              q.band,
              q.exch_rcvd,
              ce.continent,
              ce.itu_zone
            FROM qsos q
            JOIN logs l ON q.log_id=l.log_id
            LEFT JOIN call_enrichment ce ON ce.callsign=q.their_call
            WHERE l.contest_id=? AND l.callsign=?
            ORDER BY q.qso_id
            """,
            [contest_id, call],
        ).fetchall()

        seen_itu: dict[str, Set[int]] = {}
        seen_hq: dict[str, Set[str]] = {}

        total_pts = 0
        total_mults = 0
        updates = []

        for qso_id, band, exch, dx_cont, dx_itu in qsos:
            band = band or "UNKNOWN"
            seen_itu.setdefault(band, set())
            seen_hq.setdefault(band, set())

            hq = exch.strip().upper() if _is_hq_exchange(exch) else None

            # ----- points -----
            if hq:
                points = 1
            elif my_itu and dx_itu and int(my_itu) == int(dx_itu):
                points = 1
            elif my_cont and dx_cont and my_cont == dx_cont:
                points = 3
            else:
                points = 5

            # ----- multipliers -----
            is_mult_itu = False
            is_mult_hq = False

            if hq:
                if hq not in seen_hq[band]:
                    seen_hq[band].add(hq)
                    is_mult_hq = True
            elif dx_itu:
                z = int(dx_itu)
                if z not in seen_itu[band]:
                    seen_itu[band].add(z)
                    is_mult_itu = True

            total_pts += points
            total_mults += int(is_mult_itu) + int(is_mult_hq)

            updates.append((points, hq, is_mult_itu, is_mult_hq, contest_id, qso_id))

        con.executemany(
            """
            UPDATE qso_scoring
            SET points=?, hq=?, is_mult_itu=?, is_mult_hq=?
            WHERE contest_id=? AND qso_id=?
            """,
            updates,
        )

    return {
        "qso_points": total_pts,
        "multipliers": total_mults,
        "score": total_pts * total_mults,
    }


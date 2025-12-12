# tests/test_scoring_cqww.py
from __future__ import annotations

from datetime import datetime

from hamcontestlog.db import connect
from hamcontestlog.scoring.cqww import score_cqww_station


def _insert_contest() -> None:
    with connect() as con:
        con.execute(
            """
            INSERT INTO contests (contest_id, name, sponsor, mode, start_time, end_time, bands, metadata)
            VALUES (
                '2024cw', 'CQWW CW 2024', 'CQ', 'CW',
                TIMESTAMP '2024-11-23 00:00:00',
                TIMESTAMP '2024-11-24 23:59:59',
                '160m,80m,40m,20m,15m,10m',
                '{}'
            )
            ON CONFLICT (contest_id) DO NOTHING;
            """
        )


def _insert_call_enrichment() -> None:
    # Minimal fields needed by scorer: callsign, dxcc, cq_zone, continent
    rows = [
        ("K1ABC", "USA", 5, "NA"),
        ("K2DEF", "USA", 5, "NA"),
        ("VE2XYZ", "CAN", 4, "NA"),
        ("EA3XYZ", "EA", 14, "EU"),
        ("DL1AAA", "DL", 14, "EU"),
    ]
    with connect() as con:
        con.executemany(
            """
            INSERT INTO call_enrichment (callsign, dxcc, cq_zone, itu_zone, continent, prefix, state, province)
            VALUES (?, ?, ?, NULL, ?, NULL, NULL, NULL)
            ON CONFLICT (callsign) DO UPDATE SET
                dxcc=EXCLUDED.dxcc,
                cq_zone=EXCLUDED.cq_zone,
                continent=EXCLUDED.continent;
            """,
            rows,
        )


def _insert_log_and_qsos() -> None:
    with connect() as con:
        log_id = con.execute(
            """
            INSERT INTO logs (contest_id, callsign, operator, club, category, claimed_score, raw_metadata)
            VALUES ('2024cw', 'K1ABC', 'K1ABC', NULL, 'SINGLE-OP', NULL, '{}')
            RETURNING log_id;
            """
        ).fetchone()[0]

        # Three QSOs on same band with different scenarios:
        # 1) K1ABC -> K2DEF same country -> 0 points
        # 2) K1ABC -> VE2XYZ NA<->NA diff country -> 2 points
        # 3) K1ABC -> EA3XYZ NA->EU -> 3 points
        qso_rows = [
            (log_id, datetime(2024, 11, 23, 0, 1), "20m", "CW", 14025000.0, "K1ABC", "K2DEF", "599", "599", "05", "05"),
            (log_id, datetime(2024, 11, 23, 0, 2), "20m", "CW", 14026000.0, "K1ABC", "VE2XYZ", "599", "599", "05", "02"),
            (log_id, datetime(2024, 11, 23, 0, 3), "20m", "CW", 14027000.0, "K1ABC", "EA3XYZ", "599", "599", "05", "14"),
            # Same DXCC as EA3XYZ but different qso_time later, to test multiplier per band:
            (log_id, datetime(2024, 11, 23, 0, 4), "20m", "CW", 14028000.0, "K1ABC", "DL1AAA", "599", "599", "05", "14"),
            # Same DXCC (EA) but on a different band -> should be multiplier again
            (log_id, datetime(2024, 11, 23, 1, 0), "40m", "CW", 7025000.0, "K1ABC", "EA3XYZ", "599", "599", "05", "14"),
        ]

        qso_ids = []
        for r in qso_rows:
            qso_id = con.execute(
                """
                INSERT INTO qsos (
                    log_id, qso_time, band, mode, freq_hz,
                    my_call, their_call, rst_sent, rst_rcvd,
                    exch_sent, exch_rcvd, mult_flags
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}')
                RETURNING qso_id;
                """,
                list(r),
            ).fetchone()[0]
            qso_ids.append(qso_id)

        # Seed qso_scoring rows (normally done by an "enrich qsos" step).
        # Multipliers are computed based on these dxcc/cq_zone fields.
        # We set dxcc/cq_zone based on the worked station.
        scoring_seed = [
            (qso_ids[0], "2024cw", "USA", 5),
            (qso_ids[1], "2024cw", "CAN", 4),
            (qso_ids[2], "2024cw", "EA", 14),
            (qso_ids[3], "2024cw", "DL", 14),
            (qso_ids[4], "2024cw", "EA", 14),
        ]
        con.executemany(
            """
            INSERT INTO qso_scoring (qso_id, contest_id, dxcc, cq_zone, points,
                                    is_mult_dxcc, is_mult_cq_zone, is_mult_itu, is_mult_prefix, is_mult_state)
            VALUES (?, ?, ?, ?, NULL, FALSE, FALSE, FALSE, FALSE, FALSE)
            ON CONFLICT (qso_id) DO UPDATE SET contest_id=EXCLUDED.contest_id;
            """,
            scoring_seed,
        )


def test_cqww_points_and_multipliers(temp_db_path) -> None:
    _insert_contest()
    _insert_call_enrichment()
    _insert_log_and_qsos()

    # Run scoring
    score_cqww_station("2024cw", "K1ABC")

    with connect() as con:
        rows = con.execute(
            """
            SELECT q.qso_time, q.band, q.their_call, s.points, s.is_mult_dxcc, s.is_mult_cq_zone
            FROM qsos q
            JOIN logs l ON q.log_id=l.log_id
            JOIN qso_scoring s ON q.qso_id=s.qso_id
            WHERE l.contest_id='2024cw' AND l.callsign='K1ABC'
            ORDER BY q.qso_time;
            """
        ).fetchall()

    # Points
    # K2DEF same country => 0
    assert rows[0][3] == 0
    # VE2XYZ NA<->NA diff country => 2
    assert rows[1][3] == 2
    # EA3XYZ NA->EU => 3
    assert rows[2][3] == 3
    # DL1AAA NA->EU => 3
    assert rows[3][3] == 3

    # Multipliers (DXCC and CQ zone per band):
    # 20m: first USA, first CAN, first EA, first DL => each first should be TRUE
    # But note: same country USA QSO is still a QSO; multiplier logic is purely dxcc/cq_zone-based.
    # For 20m: USA first seen -> TRUE, CAN first -> TRUE, EA first -> TRUE, DL first -> TRUE
    assert rows[0][4] is True
    assert rows[1][4] is True
    assert rows[2][4] is True
    assert rows[3][4] is True

    # CQ zone multipliers (20m): zone 5,4,14 -> zone 14 appears twice (EA3XYZ and DL1AAA), only first should be TRUE
    assert rows[0][5] is True   # zone 5
    assert rows[1][5] is True   # zone 4
    assert rows[2][5] is True   # zone 14 first
    assert rows[3][5] is False  # zone 14 repeated on same band

    # 40m entry should be dxcc multiplier for EA again because different band
    assert rows[4][1] == "40m"
    assert rows[4][2] == "EA3XYZ"
    assert rows[4][4] is True
    assert rows[4][5] is True


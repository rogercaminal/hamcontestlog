# tests/test_ingest_logs.py
from __future__ import annotations

from io import BytesIO
from datetime import datetime

from hamcontestlog.ingest.logs import parse_cabrillo_stream, ingest_cabrillo_stream
from hamcontestlog.db import connect


SAMPLE_CABRILLO = b"""\
START-OF-LOG: 3.0
CONTEST: CQ-WW-CW
CALLSIGN: K1ABC
OPERATOR: K1ABC
CATEGORY-OPERATOR: SINGLE-OP
CATEGORY-BAND: ALL
CATEGORY-POWER: HIGH
CATEGORY-MODE: CW
CLAIMED-SCORE: 12345
END-OF-LOG:
QSO:  14025 CW 2024-11-23 0001 K1ABC 599 05 EA3XYZ 599 14
QSO:   7025 CW 2024-11-23 0010 K1ABC 599 05 VE2XYZ 599 02
QSO:  28025 CW 2024-11-23 0020 K1ABC 599 05 K2DEF  599 05
"""


def test_parse_cabrillo_stream_header_and_qsos() -> None:
    header, qsos = parse_cabrillo_stream(BytesIO(SAMPLE_CABRILLO))

    assert header.callsign == "K1ABC"
    assert header.operator == "K1ABC"
    assert header.claimed_score == 12345

    assert len(qsos) == 3

    q0 = qsos[0]
    assert q0.my_call == "K1ABC"
    assert q0.their_call == "EA3XYZ"
    assert q0.mode.value == "CW"
    assert q0.band == "20m"
    assert q0.time_on == datetime(2024, 11, 23, 0, 1)

    q1 = qsos[1]
    assert q1.band == "40m"

    q2 = qsos[2]
    assert q2.band == "10m"


def test_ingest_cabrillo_stream_inserts_logs_and_qsos(temp_db_path) -> None:
    # create contest
    with connect() as con:
        con.execute(
            """
            INSERT INTO contests (contest_id, name, start_time, end_time, mode, bands, metadata)
            VALUES ('2024cw', 'CQWW CW 2024', TIMESTAMP '2024-11-23 00:00:00', TIMESTAMP '2024-11-24 23:59:59',
                    'CW', '160m,80m,40m,20m,15m,10m', '{}');
            """
        )

    ingest_cabrillo_stream("2024cw", BytesIO(SAMPLE_CABRILLO))

    with connect() as con:
        n_logs = con.execute("SELECT COUNT(*) FROM logs WHERE contest_id='2024cw';").fetchone()[0]
        n_qsos = con.execute("""
            SELECT COUNT(*) FROM qsos q
            JOIN logs l ON q.log_id = l.log_id
            WHERE l.contest_id='2024cw' AND l.callsign='K1ABC';
        """).fetchone()[0]

    assert n_logs == 1
    assert n_qsos == 3


# tests/test_rbn.py
from __future__ import annotations

from datetime import datetime
from io import BytesIO
import zipfile

import pytest

from hamcontestlog.db import connect
from hamcontestlog.fetch.rbn import rbn_zip_urls_for_contest
from hamcontestlog.ingest.rbn import ingest_rbn_for_contest


class _FakeResp:
    def __init__(self, status_code: int, content: bytes):
        self.status_code = status_code
        self.content = content


def _make_rbn_zip(csv_name: str, csv_text: str) -> bytes:
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, csv_text)
    return buf.getvalue()


def test_rbn_zip_urls_midnight_exclusive() -> None:
    start = datetime(2024, 11, 23, 0, 0, 0)
    end = datetime(2024, 11, 25, 0, 0, 0)  # midnight end should not include 25th
    urls = rbn_zip_urls_for_contest(start, end)
    assert urls == [
        "https://data.reversebeacon.net/rbn_history/20241123.zip",
        "https://data.reversebeacon.net/rbn_history/20241124.zip",
    ]


def test_rbn_ingest_bulk_duckdb_parsing_skips_bad_rows(temp_db_path, monkeypatch: pytest.MonkeyPatch) -> None:
    """
    Ensure:
    - DuckDB bulk CSV ingest works with the observed 13-column RBN format
    - Malformed rows are skipped (ignore_errors / strict_mode=false)
    - Time window filter is applied
    - Total inserted count matches unique in-window rows across multiple days
    """

    # Contest in DB
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
            );
            """
        )

    # CSV format as observed in your RBN history files:
    # callsign,de_pfx,de_cont,freq,band,dx,dx_pfx,dx_cont,mode,db,date,speed,tx_mode

    csv_day_23 = """\
callsign,de_pfx,de_cont,freq,band,dx,dx_pfx,dx_cont,mode,db,date,speed,tx_mode
OK4QRO,OK,EU,3537,80m,OK4YL,OK,EU,CQ,22,2024-11-23 00:00:00,28,CW
S50ARX,S5,EU,7043.6,40m,PJ4K,PJ4,SA,CQ,30,2024-11-23 12:00:00,35,CW
"""

    # Include a malformed row + one valid row on day 24, plus one outside the window (25 00:00:00)
    csv_day_24 = """\
callsign,de_pfx,de_cont,freq,band,dx,dx_pfx,dx_cont,mode,db,date,speed,tx_mode
#THIS IS A MALFORMED LINE WITH NO COMMAS
S50ARX,S5,EU,1818.8,160m,S56X,S5,EU,CQ,35,2024-11-24 23:59:59,33,CW
S50ARX,S5,EU,1818.8,160m,S56X,S5,EU,CQ,35,2024-11-25 00:00:00,33,CW
"""

    zip_23 = _make_rbn_zip("20241123.csv", csv_day_23)
    zip_24 = _make_rbn_zip("20241124.csv", csv_day_24)

    def fake_get(url, timeout=120):
        if url.endswith("20241123.zip"):
            return _FakeResp(200, zip_23)
        if url.endswith("20241124.zip"):
            return _FakeResp(200, zip_24)
        return _FakeResp(404, b"")

    monkeypatch.setattr("requests.get", fake_get)

    inserted = ingest_rbn_for_contest("2024cw")

    # In-window rows should be:
    # 2024-11-23 00:00:00 -> OK4YL
    # 2024-11-23 12:00:00 -> PJ4K
    # 2024-11-24 23:59:59 -> S56X
    # malformed -> skipped
    # 2024-11-25 00:00:00 -> outside end_time -> excluded
    assert inserted == 3

    with connect() as con:
        n = con.execute("SELECT COUNT(*) FROM rbn_spots WHERE contest_id='2024cw';").fetchone()[0]
        assert n == 3

        dx = {r[0] for r in con.execute("SELECT dx_call FROM rbn_spots WHERE contest_id='2024cw';").fetchall()}
        assert dx == {"OK4YL", "PJ4K", "S56X"}


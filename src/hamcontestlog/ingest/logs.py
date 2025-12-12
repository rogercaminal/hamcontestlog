# src/hamcontestlog/ingest/logs.py
from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from io import BytesIO, TextIOWrapper
from typing import Iterable, Tuple, List

from ..db import connect
from ..models import LogHeader, Qso, Mode


def ingest_cabrillo_stream(contest_id: str, fileobj: BytesIO) -> None:
    """
    Parse a Cabrillo log from a byte stream and insert it into DuckDB.

    - Creates/updates one row in `logs`
    - Inserts one row per QSO into `qsos`
    """
    header, qsos = parse_cabrillo_stream(fileobj)

    if not qsos:
        # Nothing to insert; you may want to log/raise instead
        return

    with connect() as con:
        # Insert log header
        log_id = con.execute(
            """
            INSERT INTO logs (
                contest_id, callsign, operator, club, category, claimed_score, raw_metadata
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            RETURNING log_id;
            """,
            [
                contest_id,
                header.callsign,
                header.operator,
                header.club,
                header.category,
                header.claimed_score,
                header.raw_metadata,
            ],
        ).fetchone()[0]

        # Insert QSOs
        con.executemany(
            """
            INSERT INTO qsos (
                log_id, qso_time, band, mode, freq_hz,
                my_call, their_call, rst_sent, rst_rcvd,
                exch_sent, exch_rcvd
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            [
                (
                    log_id,
                    q.time_on,
                    q.band,
                    q.mode.value,
                    q.freq_hz,
                    q.my_call,
                    q.their_call,
                    q.rst_sent,
                    q.rst_rcvd,
                    q.exch_sent,
                    q.exch_rcvd,
                )
                for q in qsos
            ],
        )


def parse_cabrillo_stream(fileobj: BytesIO) -> Tuple[LogHeader, List[Qso]]:
    """
    Very simple Cabrillo parser tailored for CQWW-style logs.

    Assumptions for QSO lines (typical CQWW style, space-separated):

        QSO:  FREQ  MODE  YYYY-MM-DD  HHMM  MYCALL  RST_S  EXCH_S  THEIR  RST_R  EXCH_R  ...

    - We don't try to parse every possible header keyword yet.
    - We treat unknown header lines as raw_metadata entries.
    - You will almost certainly want to refine this against real logs.
    """
    # Wrap bytes into text
    text = TextIOWrapper(fileobj, encoding="utf-8", errors="replace")
    raw_metadata: dict[str, str] = {}
    qsos: list[Qso] = []

    callsign = None
    operator = None
    club = None
    category = None
    claimed_score = None

    for line in text:
        line = line.rstrip("\r\n")
        if not line:
            continue

        if line.upper().startswith("QSO:"):
            qso = _parse_cabrillo_qso_line(line)
            if qso:
                qsos.append(qso)
            continue

        # Header line: KEY: value
        if ":" in line:
            key, value = line.split(":", 1)
            key = key.strip().upper()
            value = value.strip()

            raw_metadata[key] = value

            if key == "CALLSIGN":
                callsign = value
            elif key == "OPERATOR":
                operator = value
            elif key == "CLUB":
                club = value
            elif key.startswith("CATEGORY"):
                # You might want to parse CATEGORY-OPERATOR, CATEGORY-BAND, etc.
                # For now we just store a composite category string.
                if category:
                    category = f"{category} {value}"
                else:
                    category = value
            elif key == "CLAIMED-SCORE":
                try:
                    claimed_score = int(value)
                except ValueError:
                    claimed_score = None

    if callsign is None:
        callsign = raw_metadata.get("CALL", "UNKNOWN")

    header = LogHeader(
        callsign=callsign,
        operator=operator,
        club=club,
        category=category,
        claimed_score=claimed_score,
        raw_metadata=raw_metadata,
    )

    return header, qsos


def _parse_cabrillo_qso_line(line: str) -> Qso | None:
    """
    Parse a single 'QSO:' line into a Qso dataclass.

    This is intentionally simple and may need adjustments based on actual
    CQWW log formats you inspect.

    Expected layout (tokens):

      0: 'QSO:'
      1: freq (kHz or MHz)
      2: mode
      3: date (YYYY-MM-DD or YY-MM-DD)
      4: time (HHMM or HHMMSS)
      5: my_call
      6: rst_sent
      7: exch_sent
      8: their_call
      9: rst_rcvd
      10: exch_rcvd
      (rest ignored)
    """
    parts = line.split()
    if len(parts) < 11:
        # malformed or not a standard QSO line
        return None

    _, freq_str, mode_str, date_str, time_str, my_call, rst_s, ex_s, their_call, rst_r, ex_r, *rest = parts

    # Frequency
    try:
        freq_hz = float(freq_str) * 1e3
    except ValueError:
        freq_hz = None

    # Mode
    mode = _parse_mode(mode_str)

    # Datetime
    time_on = _parse_cabrillo_datetime(date_str, time_str)

    # Band: you might want a proper freq->band mapping later
    band = _infer_band_from_freq(freq_hz)

    return Qso(
        time_on=time_on,
        band=band,
        mode=mode,
        freq_hz=freq_hz,
        my_call=my_call.upper(),
        their_call=their_call.upper(),
        rst_sent=rst_s,
        rst_rcvd=rst_r,
        exch_sent=ex_s,
        exch_rcvd=ex_r,
    )


def _parse_mode(mode_str: str) -> Mode:
    m = mode_str.upper()
    if m == "CW":
        return Mode.CW
    if m in {"SSB", "PH"}:
        return Mode.SSB
    if m in {"FT8", "FT4"}:
        return Mode.FT8
    return Mode.OTHER


def _parse_cabrillo_datetime(date_str: str, time_str: str) -> datetime:
    """
    Convert Cabrillo date/time into a datetime.

    Accepts:
      - date: YYYY-MM-DD or YY-MM-DD
      - time: HHMM or HHMMSS (UTC assumed)
    """
    # Normalize date
    if len(date_str) == 8 and "-" not in date_str:
        # e.g. 241123 -> 2024-11-23 (very rough)
        yy = int(date_str[:2])
        mm = int(date_str[2:4])
        dd = int(date_str[4:6])
        year = 2000 + yy if yy < 80 else 1900 + yy
        date_part = f"{year:04d}-{mm:02d}-{dd:02d}"
    else:
        date_part = date_str

    # Normalize time
    t = time_str.strip()
    if len(t) == 4:
        # HHMM
        hh = t[:2]
        mm = t[2:4]
        ss = "00"
    elif len(t) == 6:
        hh = t[:2]
        mm = t[2:4]
        ss = t[4:6]
    else:
        # Fallback
        hh = "00"
        mm = "00"
        ss = "00"

    return datetime.fromisoformat(f"{date_part}T{hh}:{mm}:{ss}")


def _infer_band_from_freq(freq_hz: float | None) -> str:
    """
    Very rough frequency→band mapping.

    If freq_hz is None, return 'UNKNOWN'.
    """
    if freq_hz is None:
        return "UNKNOWN"

    mhz = freq_hz / 1e6

    if 1.8 <= mhz < 2.0:
        return "160m"
    if 3.5 <= mhz < 4.0:
        return "80m"
    if 7.0 <= mhz < 7.3:
        return "40m"
    if 14.0 <= mhz < 14.35:
        return "20m"
    if 21.0 <= mhz < 21.45:
        return "15m"
    if 28.0 <= mhz < 29.7:
        return "10m"

    return "UNKNOWN"


# src/hamcontestlog/models.py
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any


class Mode(str, Enum):
    CW = "CW"
    SSB = "SSB"
    FT8 = "FT8"
    OTHER = "OTHER"


@dataclass
class Qso:
    time_on: datetime
    band: str
    mode: Mode
    freq_hz: Optional[float]
    my_call: str
    their_call: str
    rst_sent: str
    rst_rcvd: str
    exch_sent: str
    exch_rcvd: str


@dataclass
class LogHeader:
    callsign: str
    operator: str | None
    club: str | None
    category: str | None
    claimed_score: int | None
    raw_metadata: Dict[str, Any]


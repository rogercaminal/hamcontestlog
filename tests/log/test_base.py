import pytest
import pandas as pd
import io
from datetime import datetime
from hamcontestlog.log.base import LogBase


class LogMock(LogBase):
    """Mock implementation of LogBase for testing."""

    def open_file(self, path: str) -> io.StringIO:
        """Mock implementation that returns a file-like object."""
        sample_log = """\
START-OF-LOG: 3.0
CONTEST: CQ-WW-CW
CALLSIGN: EF6T
LOCATION: DX
CATEGORY-OPERATOR: SINGLE-OP
CATEGORY-ASSISTED: NON-ASSISTED
CATEGORY-BAND: ALL
CATEGORY-POWER: HIGH
CATEGORY-MODE: CW
CATEGORY-TRANSMITTER: ONE
CATEGORY-STATION: FIXED
CATEGORY-OVERLAY:
GRID-LOCATOR: JM08PW
CLAIMED-SCORE: 12486666
NAME: Roger Caminal Armadans
OPERATORS: EA3M
CLUB: CATALONIA CONTEST CLUB
CREATED-BY: DXLog.net v2.6.10
QSO:    7044 CW 2024-11-23 0000 EF6T             599 14    YR8D             599  20      0
QSO:    7044 CW 2024-11-23 0000 EF6T             599 14    N1IX             599  05      0
QSO:   14041 CW 2024-11-23 0001 EF6T             599 14    W0EAR            599  04      1
QSO:   14041 CW 2024-11-23 0001 EF6T             599 14    NC3Y             599  05      1
QSO:   14041 CW 2024-11-23 0001 EF6T             599 14    KB4DX            599  05      1
QSO:    7044 CW 2024-11-23 0002 EF6T             599 14    PC5Q             599  14      0
QSO:   14041 CW 2024-11-23 0002 EF6T             599 14    K1AR             599  05      1
QSO:   14041 CW 2024-11-23 0003 EF6T             599 14    AB9YC            599  04      1
QSO:    7044 CW 2024-11-23 0003 EF6T             599 14    VE3NNT           599  04      0
QSO:   14041 CW 2024-11-23 0003 EF6T             599 14    K3TS             599  05      1
"""
        return io.StringIO(sample_log)


def test_store_log():
    """Test parsing of log file and extraction of metadata and QSOs."""
    log = LogMock("mock_path")
    metadata_df, qsos_df = log.store_log("mock_path")

    # Validate metadata extraction
    assert isinstance(metadata_df, pd.DataFrame)
    assert not metadata_df.empty
    assert metadata_df.loc[0, "CONTEST"] == "CQ-WW-CW"
    assert metadata_df.loc[0, "CALLSIGN"] == "EF6T"
    assert metadata_df.loc[0, "LOCATION"] == "DX"
    assert metadata_df.loc[0, "CATEGORY-OPERATOR"] == "SINGLE-OP"
    assert metadata_df.loc[0, "CATEGORY-ASSISTED"] == "NON-ASSISTED"
    assert metadata_df.loc[0, "CATEGORY-BAND"] == "ALL"
    assert metadata_df.loc[0, "CATEGORY-POWER"] == "HIGH"
    assert metadata_df.loc[0, "CATEGORY-MODE"] == "CW"
    assert metadata_df.loc[0, "CATEGORY-TRANSMITTER"] == "ONE"
    assert metadata_df.loc[0, "CATEGORY-STATION"] == "FIXED"
    assert metadata_df.loc[0, "CATEGORY-OVERLAY"] == ""
    assert metadata_df.loc[0, "GRID-LOCATOR"] == "JM08PW"
    assert metadata_df.loc[0, "CLAIMED-SCORE"] == "12486666"
    assert metadata_df.loc[0, "NAME"] == "Roger Caminal Armadans"
    assert metadata_df.loc[0, "OPERATORS"] == "EA3M"
    assert metadata_df.loc[0, "CLUB"] == "CATALONIA CONTEST CLUB"
    assert metadata_df.loc[0, "CREATED-BY"] == "DXLog.net v2.6.10"

    # Validate QSO extraction
    assert isinstance(qsos_df, pd.DataFrame)
    assert not qsos_df.empty
    assert len(qsos_df) == 10
    assert qsos_df.loc[0, "frequency"] == 7044
    assert qsos_df.loc[0, "mode"] == "CW"
    assert qsos_df.loc[0, "mycall"] == "EF6T"
    assert qsos_df.loc[0, "call"] == "YR8D"
    assert qsos_df.loc[0, "exch"] == "20"
    assert qsos_df.loc[0, "radio"] == "0"
    expected_datetime = datetime.strptime("2024-11-23 0000", "%Y-%m-%d %H%M")
    assert qsos_df.loc[0, "datetime"] == expected_datetime


def test_open_file():
    """Test that open_file correctly returns a file-like object."""
    log = LogMock("mock_path")
    buffer = log.open_file("mock_path")
    assert isinstance(buffer, io.StringIO)
    content = buffer.read()
    assert "CONTEST: CQ-WW-CW" in content
    assert "QSO:    7044 CW 2024-11-23 0003 EF6T             599 14    VE3NNT           599  04      0" in content


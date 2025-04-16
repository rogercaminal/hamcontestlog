import pytest
import pandas as pd
from datetime import datetime
from hamcontestlog.log.local import LogLocal
import tempfile


# Sample log content
SAMPLE_LOG = """\
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
"""


def test_loglocal_parses_metadata_and_qsos():
    """Test LogLocal correctly parses metadata and QSO records from a file."""
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp:
        tmp.write(SAMPLE_LOG)
        tmp.seek(0)
        log = LogLocal(tmp.name)

    metadata_df = log.metadata
    qsos_df = log.log

    # Metadata checks
    assert isinstance(metadata_df, pd.DataFrame)
    assert not metadata_df.empty
    assert metadata_df.loc[0, "CALLSIGN"] == "EF6T"
    assert metadata_df.loc[0, "CLUB"] == "CATALONIA CONTEST CLUB"

    # QSO checks
    assert isinstance(qsos_df, pd.DataFrame)
    assert not qsos_df.empty
    assert len(qsos_df) == 3
    assert qsos_df.iloc[0]["call"] == "YR8D"
    assert qsos_df.iloc[0]["mode"] == "CW"
    assert qsos_df.iloc[0]["radio"] == "0"
    assert qsos_df.iloc[2]["call"] == "W0EAR"
    assert qsos_df.iloc[2]["radio"] == "1"
    expected_datetime = datetime.strptime("2024-11-23 0000", "%Y-%m-%d %H%M")
    assert qsos_df.iloc[0]["datetime"] == expected_datetime
    assert qsos_df.iloc[0]["id"] == "EF6T_0"


def test_loglocal_open_file_reads_correctly():
    """Test that LogLocal.open_file reads actual local file content."""
    with tempfile.NamedTemporaryFile(mode="w+", delete=False) as tmp:
        tmp.write(SAMPLE_LOG)
        tmp.seek(0)
        log = LogLocal(tmp.name)

    buffer = log.open_file(tmp.name)
    content = buffer.read()
    assert "CQ-WW-CW" in content
    assert "VE3NNT" not in content  # Confirming only a subset of QSOs were included


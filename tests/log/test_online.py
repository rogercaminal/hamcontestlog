import pytest
import pandas as pd
import io
from datetime import datetime
from unittest.mock import patch, Mock
from hamcontestlog.log.online import LogOnline


# Sample Cabrillo log content
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
QSO:   14041 CW 2024-11-23 0001 EF6T             599 14    W0EAR            599  04      1
"""


@patch("hamcontestlog.log.online.requests.get")
def test_logonline_parses_log_correctly(mock_get):
    """Test that LogOnline parses metadata and QSO data from a mocked HTTP response."""
    mock_response = Mock()
    mock_response.status_code = 200
    mock_response.text = SAMPLE_LOG
    mock_get.return_value = mock_response

    log = LogOnline("http://example.com/mock.log")
    metadata_df = log.metadata
    qsos_df = log.log

    # Check metadata
    assert isinstance(metadata_df, pd.DataFrame)
    assert metadata_df.loc[0, "CALLSIGN"] == "EF6T"
    assert metadata_df.loc[0, "CLUB"] == "CATALONIA CONTEST CLUB"
    assert metadata_df.loc[0, "CATEGORY-MODE"] == "CW"

    # Check QSO content
    assert isinstance(qsos_df, pd.DataFrame)
    assert len(qsos_df) == 2
    assert qsos_df.iloc[0]["call"] == "YR8D"
    assert qsos_df.iloc[1]["call"] == "W0EAR"
    assert qsos_df.iloc[1]["radio"] == "1"
    expected_datetime = datetime.strptime("2024-11-23 0000", "%Y-%m-%d %H%M")
    assert qsos_df.iloc[0]["datetime"] == expected_datetime


@patch("hamcontestlog.log.online.requests.get")
def test_logonline_raises_on_bad_status(mock_get):
    """Test that LogOnline raises a ValueError when URL fetch fails."""
    mock_response = Mock()
    mock_response.status_code = 404
    mock_get.return_value = mock_response

    with pytest.raises(ValueError, match="Link does not exist"):
        LogOnline("http://example.com/404.log")


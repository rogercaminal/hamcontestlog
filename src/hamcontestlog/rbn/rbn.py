"""
RBN Data Reader

This module provides a class for downloading, extracting, and processing
Reverse Beacon Network (RBN) data from daily historical ZIP archives.

The data is fetched from https://data.reversebeacon.net/rbn_history/ and includes
information such as callsign, frequency, signal report, mode, speed, and continent info.

The core class, `ReverseBeaconReader`, handles downloading and parsing the data into
a structured pandas DataFrame, with continent enrichment using external call info utilities.
"""

from abc import ABC
import datetime
import hashlib
import pandas as pd
import zipfile
import requests
import tempfile
from typing import Dict, ClassVar
from hamcontestlog.utils import get_call_info


call_info = get_call_info()


class ReverseBeaconReader(ABC):
    """
    Abstract base class for reading and processing Reverse Beacon Network (RBN) data.

    Attributes:
        dtypes (ClassVar[Dict[str, str]]): Expected dtypes of the cleaned DataFrame.
        date (datetime.date): The date of the RBN data to load.
        url (str): URL pointing to the ZIP archive for the specified date.
        data (pd.DataFrame): Parsed and cleaned DataFrame after `load()` is called.
    """

    dtypes: ClassVar[Dict[str, str]] = {
        "callsign": "str",
        "freq": "float",
        "band": "int",
        "dx": "str",
        "mode": "str",
        "db": "int",
        "date": "str",
        "speed": "int",
        "de_cont": "str",
        "dx_cont": "str",
    }

    def __init__(self, date: datetime.date):
        """
        Initializes the RBN reader for a specific date.

        Args:
            date (datetime.date): Date corresponding to the daily RBN ZIP archive.
        """
        self.date = date
        self.url = f"https://data.reversebeacon.net/rbn_history/{datetime.datetime.strftime(self.date, '%Y%m%d')}.zip"
        self.data = self.load()

    @staticmethod
    def get_raw_data(url: str) -> pd.DataFrame:
        """
        Downloads and extracts raw CSV data from a Reverse Beacon Network ZIP file.

        Args:
            url (str): Direct URL to the .zip archive containing the RBN data.

        Returns:
            pd.DataFrame: Combined DataFrame of all CSVs found in the archive.

        Raises:
            ValueError: If no CSV file is found in the archive.
            requests.HTTPError: If the download request fails.
            zipfile.BadZipFile: If the downloaded file is not a valid ZIP.
        """
        data = None
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with tempfile.NamedTemporaryFile(suffix=".zip") as tmp_file:
                for chunk in r.iter_content(chunk_size=8192):
                    tmp_file.write(chunk)
                tmp_file.flush()

                with zipfile.ZipFile(tmp_file.name, 'r') as z:
                    csv_files = [f for f in z.namelist() if f.lower().endswith(".csv")]
                    if not csv_files:
                        raise ValueError("No .csv file found in ZIP archive")

                    dfs = []
                    for csv_file in csv_files:
                        with z.open(csv_file) as f:
                            dfs.append(pd.read_csv(f))
                    data = pd.concat(dfs, ignore_index=True)
        return data

    def load(self) -> pd.DataFrame:
        """
        Loads and processes the RBN data for the given date.

        This includes:
        - Downloading and parsing raw data
        - Filling in missing continent fields using `get_call_info()`
        - Filtering out invalid rows
        - Converting band and date formats
        - Final type enforcement and cleanup

        The result is returned as pandas dataframe.
        """
        raw_data = self.get_raw_data(url=self.url)

        # Fill missing continent info for spotter prefix (de_pfx)
        for p in raw_data.query("de_cont.isnull()")["de_pfx"].unique():
            try:
                raw_data.loc[(raw_data["de_pfx"] == p), "de_cont"] = call_info.get_continent(f"{p}1AA")
            except KeyError:
                continue

        # Fill missing continent info for spotted prefix (dx_pfx)
        for p in raw_data.query("dx_cont.isnull()")["dx_pfx"].unique():
            try:
                raw_data.loc[(raw_data["dx_pfx"] == p), "dx_cont"] = call_info.get_continent(f"{p}1AA")
            except KeyError:
                continue

        # Final cleanup and transformation
        data = (
            raw_data.loc[:, self.dtypes.keys()]
            .dropna(subset=["dx"])
            .query("(band.str.contains('m') & ~(band.str.contains('cm')))")
            .assign(
                band=lambda x: x["band"].str.replace("m", "").astype(int),
                datetime=lambda x: pd.to_datetime(x["date"]),
                dummy=lambda x: x["dx"] + x["callsign"] + x["date"] + x["freq"].astype(str),
                id=lambda x: x["dummy"].apply(lambda val: hashlib.sha256(val.encode()).hexdigest())
            )
            .astype(self.dtypes)
            .drop(columns=["date", "dummy"])
        )
        return data


"""Base class for cabrillo logs"""

from abc import ABC
from abc import abstractmethod
from datetime import datetime
import pandas as pd
from typing import IO, Tuple


class LogBase(ABC):
    """
    Abstract base class for processing log files.

    Parameters
    ----------
    path : str
        The file path or URL to the log file.
    """

    def __init__(self, path: str):
        self.path = path
        self.buffer: IO[str] | None = None
        self.log: pd.DataFrame
        self.metadata: pd.DataFrame

    @abstractmethod
    def open_file(self, path: str) -> IO[str]:
        """
        Opens a log file from a given path.

        Parameters
        ----------
        path : str
            The file path or URL to open.

        Returns
        -------
        IO[str]
            A file-like object containing the log data.
        """
        ...

    def store_log(self, path: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Parses a log file and extracts metadata and QSO (contact) information.

        Parameters
        ----------
        path : str
            The file path or URL to process.

        Returns
        -------
        Tuple[pd.DataFrame, pd.DataFrame]
            A tuple containing:
            - metadata_df: DataFrame with metadata information.
            - qsos_df: DataFrame with parsed QSO records.
        """
        self.buffer = self.open_file(path=path)
        metadata = {}
        qsos = []
        with self.buffer as f:  # type: ignore
            for line in f.readlines():
                if line.startswith("QSO:"):
                    qso = line.strip().split()
                    qsos.append(
                        {
                            "frequency": int(qso[1]),
                            "mode": qso[2],
                            "datetime": datetime.strptime(
                                f"{qso[3]} {qso[4]}", "%Y-%m-%d %H%M"
                            ),
                            "mycall": qso[5],
                            "myrst": int(qso[6]),
                            "myexch": qso[7],
                            "call": qso[8],
                            "rst": qso[9],
                            "exch": qso[10],
                            "radio": 0 if len(qso) < 12 else qso[11],
                        }
                    )
                elif not line.startswith("X-QSO"):
                    meta_line = line.strip().split(":")
                    metadata[meta_line[0]] = meta_line[1].strip()
                else:
                    continue
        metadata_df = pd.DataFrame([metadata])
        qsos_df = pd.DataFrame(qsos).assign(
                id=lambda x: x["mycall"] + "_" + x.index.astype(str)
        )
        return metadata_df, qsos_df

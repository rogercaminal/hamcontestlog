"""Local cabrillo logs"""
from typing import IO
from hamcontestlog.log.base import LogBase


class LogLocal(LogBase):
    """
    Class for processing log files stored locally.

    Parameters
    ----------
    path : str
        The file path to the log file.
    """
    
    def __init__(self, path: str):
        """
        Initializes the LogLocal class and processes the log file.

        Parameters
        ----------
        path : str
            The file path to the log file.
        """
        super().__init__(path=path)
        self.metadata, self.log = self.store_log(path=path)

    def open_file(self, path: str) -> IO[str]:
        """
        Opens a local log file.

        Parameters
        ----------
        path : str
            The file path to open.

        Returns
        -------
        IO[str]
            A file object for reading the log data.
        """
        return open(path, "r")

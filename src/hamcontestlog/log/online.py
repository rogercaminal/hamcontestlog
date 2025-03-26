"""Online cabrillo logs"""

import requests
import io
from hamcontestlog.log.base import LogBase



class LogOnline(LogBase):
    """
    Class for processing log files stored online.

    This class fetches log files from a given URL and processes their contents.

    Parameters
    ----------
    path : str
        The URL of the log file.
    """

    def __init__(self, path: str):
        """
        Initializes the LogOnline class and processes the online log file.

        Parameters
        ----------
        path : str
            The URL of the log file.
        """
        super().__init__(path=path)
        self.metadata, self.log = self.store_log(path=path)

    def open_file(self, path: str) -> io.StringIO:
        """
        Fetches a log file from a URL and returns its contents as an in-memory file-like object.

        Parameters
        ----------
        path : str
            The URL of the log file.

        Returns
        -------
        io.StringIO
            A file-like object containing the log data.

        Raises
        ------
        ValueError
            If the URL does not exist or returns a non-200 status code.
        """
        response = requests.get(path)
        if response.status_code == 200:
            return io.StringIO(response.text)  # Create an in-memory file-like object
        else:
            raise ValueError("Link does not exist")

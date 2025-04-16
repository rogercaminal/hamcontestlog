"""Contest class for CQWW"""
from typing import Dict
import requests
import re

from hamcontestlog.contest.base import ContestBase


class ContestCQWW(ContestBase):
    url_contest_participants: str = "https://cqww.com/publiclogs/{year}{mode}/"
    url_contest_log: str = "https://cqww.com/publiclogs/{year}{mode}/{call_hash}"

    def __init__(self, storage_path: str):
        super().__init__(storage_path=storage_path)

    @classmethod
    def list_cabrillo_files(cls, year: int, mode: str) -> Dict[str, str]:
        response = requests.get(cls.url_contest_participants.format(year=year, mode=mode.lower()))
        if response.status_code == 200:
            page = response.text
        else:
            raise Exception("Page not found!")
        
        pattern = re.compile(r"<a href='(.*?)'>(.*?)</a>")
        matches = pattern.findall(page)
        log_files = {call: link for link, call in matches}
        return log_files


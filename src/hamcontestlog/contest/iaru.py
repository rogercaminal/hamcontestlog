"""Contest class for IARU HF"""
from typing import Dict
import requests
import re

from hamcontestlog.contest.base import ContestBase


class ContestIARU(ContestBase):
    url_contest_participants: str = "https://contests.arrl.org/publiclogs.php?eid=4&iid=1053"
    url_contest_log: str = "https://contests.arrl.org/showpubliclog.php?q={call_hash}"
    # I need a dictionary for eid<-->contest, iid<-->year, and call_hash<-->contest-year-call

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


    @staticmethod
    def get_year_urls(text: str) -> Dict[int, str]:
       matches = re.findall(r'<a href="(publiclogs\.php\?eid=4&iid=\d+)">(\d{4})</a>', text) 
       return {int(year): f"https://contests.arrl.org/{link}" for link, year in matches}


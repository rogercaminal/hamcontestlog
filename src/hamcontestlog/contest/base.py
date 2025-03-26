"""Base class for contests"""
from abc import ABC
from typing import List
import requests
import re


class ContestBase(ABC):
    @staticmethod
    def list_cabrillo_files() -> List[str]:
        response = requests.get("https://cqww.com/publiclogs/2024cw/")
        if response.status_code == 200:
            page = response.text
        else:
            raise Exception("Page not found!")
        log_files = re.findall(r"href='([\w\d]+\.log)'", page)
        return log_files

# src/hamcontestlog/fetch/cqww.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup  # add to pyproject dependencies

from .base import ContestSource

BASE_INDEX_URL = "https://cqww.com/publiclogs/{year}{mode}/"


@dataclass(frozen=True)
class CqwwPublicLogsConfig:
    """
    Configuration for CQ WW public logs.

    year : contest year shown on the public logs page
    mode  : mode of the contest: cw or ph
    """
    year: int
    mode: str


class CqwwContestSource(ContestSource):
    """
    CQ WW public logs source.
    """

    def __init__(self, contest_id: str, cfg: CqwwPublicLogsConfig) -> None:
        self.contest_id = contest_id
        self.year = cfg.year
        self.mode = cfg.mode

    # ----- URLs -------------------------------------------------------------

    @property
    def index_url(self) -> str:
        """URL of the public logs index page."""
        return BASE_INDEX_URL.format(year=self.year, mode=self.mode)

    def log_url_for_callsign(self, callsign: str) -> str:
        """
        URL for a single station log.

        On cqww.com, logs are stored as lowercase callsign with `.log` extension:
          https://cqww.com/publiclogs/2024cw/f4fgb.log
        """
        filename = f"{callsign.lower()}.log"
        return urljoin(self.index_url, filename)

    # ----- Bulk listing -----------------------------------------------------

    def iter_log_urls(self) -> Iterable[str]:
        """
        Yield URLs of all available logs for this contest.

        Implementation:
          * GET the index page (e.g. https://cqww.com/publiclogs/2024cw/)
          * Parse all <a> tags with href ending in '.log'
          * Resolve them against the index URL
        """
        html = self._fetch_index_html()
        return self._extract_log_urls(html)

    # ----- Internal helpers -------------------------------------------------

    def _fetch_index_html(self) -> str:
        resp = requests.get(self.index_url, timeout=30)
        resp.raise_for_status()
        return resp.text

    def _extract_log_urls(self, html: str) -> List[str]:
        soup = BeautifulSoup(html, "html.parser")
        urls: List[str] = []

        # Each callsign on the page is an <a> pointing to '<CALL>.log'
        for a in soup.find_all("a", href=True):
            href = a["href"]
            if not href.lower().endswith(".log"):
                continue
            full_url = urljoin(self.index_url, href)
            urls.append(full_url)

        return urls


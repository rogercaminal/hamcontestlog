from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, List, Optional
from urllib.parse import urljoin, urlparse, parse_qs

import requests
from bs4 import BeautifulSoup


@dataclass(frozen=True)
class ArrlPublicLogsConfig:
    """
    Configuration for ARRL public logs.

    eid  : contest selector (4=IARU, 13=ARRL DX CW, 14=ARRL DX SSB)
    year : contest year shown on the public logs page
    iid  : optional instance id (resolved automatically if omitted)
    """
    eid: int
    year: int
    iid: Optional[int] = None


class ArrlContestSource:
    """
    ARRL public logs source.

    Flow:
      1) https://contests.arrl.org/publiclogs.php?eid=<eid>
         -> contains links for years, each with an iid
      2) https://contests.arrl.org/publiclogs.php?eid=<eid>&iid=<iid>
         -> contains callsign links to showpubliclog.php?q=<hash>
    """

    BASE = "https://contests.arrl.org/"

    def __init__(self, contest_id: str, cfg: ArrlPublicLogsConfig):
        self.contest_id = contest_id
        self.eid = int(cfg.eid)
        self.year = int(cfg.year)
        self._iid = int(cfg.iid) if cfg.iid is not None else None

    # ---------- public API ----------

    @property
    def iid(self) -> int:
        if self._iid is None:
            self._iid = self._resolve_iid()
        return self._iid

    @property
    def index_url(self) -> str:
        return f"{self.BASE}publiclogs.php?eid={self.eid}&iid={self.iid}"

    def iter_log_urls(self) -> Iterable[str]:
        html = self._get(self.index_url)
        return self._extract_log_urls(html)

    def log_url_for_callsign(self, callsign: str) -> str:
        target = callsign.strip().upper()
        html = self._get(self.index_url)
        soup = BeautifulSoup(html, "html.parser")

        for a in soup.find_all("a", href=True):
            if a.get_text(strip=True).upper() == target:
                return urljoin(self.BASE, a["href"])

        raise ValueError(
            f"Callsign {target} not found for ARRL contest eid={self.eid}, year={self.year}, iid={self.iid}"
        )

    # ---------- internals ----------

    def _get(self, url: str) -> str:
        r = requests.get(url, timeout=60)
        r.raise_for_status()
        return r.text

    def _resolve_iid(self) -> int:
        url = f"{self.BASE}publiclogs.php?eid={self.eid}"
        html = self._get(url)
        soup = BeautifulSoup(html, "html.parser")

        for a in soup.find_all("a", href=True):
            if a.get_text(strip=True) != str(self.year):
                continue
            qs = parse_qs(urlparse(a["href"]).query)
            if "iid" in qs:
                return int(qs["iid"][0])

        raise ValueError(f"Could not resolve iid for eid={self.eid}, year={self.year}")

    def _extract_log_urls(self, html: str) -> List[str]:
        soup = BeautifulSoup(html, "html.parser")
        urls: List[str] = []

        for a in soup.find_all("a", href=True):
            href = a["href"]
            if "showpubliclog.php?q=" in href:
                urls.append(urljoin(self.BASE, href))

        # deduplicate preserving order
        seen = set()
        out = []
        for u in urls:
            if u not in seen:
                seen.add(u)
                out.append(u)
        return out


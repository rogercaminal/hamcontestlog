# src/hamcontestlog/fetch/base.py
from typing import Iterable, Protocol


class ContestSource(Protocol):
    contest_id: str

    def iter_log_urls(self) -> Iterable[str]:
        """Yield URLs to all available logs for this contest."""
        ...

    def log_url_for_callsign(self, callsign: str) -> str:
        """Return URL for a single station log."""
        ...


class RbnSource(Protocol):
    def urls_for_contest(self, contest_id: str) -> Iterable[str]:
        """Return URLs to RBN CSV/ZIP files covering this contest."""
        ...


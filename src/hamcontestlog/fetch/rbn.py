# src/hamcontestlog/fetch/rbn.py
from datetime import timedelta
from typing import Iterable

from .base import RbnSource
from ..config import ContestConfig, load_contest_config

# Again, placeholder; you’ll plug actual RBN URLs here.
RBN_URL_TEMPLATE = "https://example.com/rbn/{date}.csv.gz"


class SimpleRbnSource(RbnSource):
    """Very simple heuristic: one RBN file per UTC day."""

    def urls_for_contest(self, contest_id: str) -> Iterable[str]:
        cfg: ContestConfig = _load_cfg(contest_id)
        day = cfg.start_time.date()
        end_day = cfg.end_time.date()

        while day <= end_day:
            yield RBN_URL_TEMPLATE.format(date=day.isoformat())
            day += timedelta(days=1)


def _load_cfg(contest_id: str) -> ContestConfig:
    # In a real implementation, you’d load from DB instead of YAML.
    # For now, just reuse the YAML loader with a known location if you want.
    raise NotImplementedError("Implement contest config loading for RBN source.")


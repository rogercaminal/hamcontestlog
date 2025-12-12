# src/hamcontestlog/config.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional
from datetime import datetime, timezone

import importlib.resources as pkg_resources
import yaml

from .db import connect
import hamcontestlog.data.contests as builtin_contests


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class ContestConfig:
    contest_id: str
    name: str
    sponsor: str
    mode: str
    start_time: datetime
    end_time: datetime
    bands: list[str]
    metadata: Dict[str, Any]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _parse_dt(value: Any) -> datetime:
    """Parse a datetime from YAML.

    We deliberately treat all times as *naive UTC* and ignore timezone
    offsets to avoid surprises when storing/reading from DuckDB.
    """
    if isinstance(value, datetime):
        # If it's tz-aware, drop tzinfo and treat as UTC
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value

    if isinstance(value, str):
        text = value.strip()
        # If ends with 'Z' or has an offset, strip it and parse as naive
        if text.endswith("Z"):
            text = text[:-1]  # drop trailing Z, keep "YYYY-MM-DDTHH:MM:SS"
        # You can also add logic here to strip "+00:00" etc. if you like.
        return datetime.fromisoformat(text)

    raise TypeError(f"Unsupported datetime value: {value!r} (type {type(value)})")


# ---------------------------------------------------------------------------
# YAML loading helpers
# ---------------------------------------------------------------------------


def load_yaml_from_path(path: Path) -> Dict[str, Any]:
    """Load a YAML file from a user-provided file path."""
    return yaml.safe_load(path.read_text(encoding="utf8"))


def load_yaml_from_package(package_path: str) -> Optional[Dict[str, Any]]:
    """
    Load a YAML file shipped inside the package.

    The package path must be relative to hamcontestlog.data.contests.
    Example: "cqww/2024cw.yaml"
    """
    try:
        root = pkg_resources.files(builtin_contests)
        resource = root.joinpath(package_path)
    except Exception:
        return None

    if not resource.is_file():
        return None

    with resource.open("r", encoding="utf8") as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Public loading API
# ---------------------------------------------------------------------------


def load_contest_config(source: Path | str) -> ContestConfig:
    """
    Load a contest definition.

    - If `source` is a Path → load user YAML (override)
    - If `source` is a string → load built-in YAML from package defaults
    """
    if isinstance(source, Path):
        data = load_yaml_from_path(source)
    else:
        data = load_yaml_from_package(source)
        if data is None:
            raise FileNotFoundError(
                f"Default contest YAML '{source}' not found inside packaged defaults."
            )

    return _parse_config(data)


def _parse_config(data: Dict[str, Any]) -> ContestConfig:
    """
    Convert YAML dictionary into a ContestConfig.
    Extra fields go into metadata.
    """
    start_raw = data["start_time"]
    end_raw = data["end_time"]

    start_time = _parse_dt(start_raw)
    end_time = _parse_dt(end_raw)

    return ContestConfig(
        contest_id=data["contest_id"],
        name=data.get("name", data["contest_id"]),
        sponsor=data.get("sponsor", ""),
        mode=data.get("mode", ""),
        start_time=start_time,
        end_time=end_time,
        bands=list(data.get("bands", [])),
        metadata={
            k: v
            for k, v in data.items()
            if k
            not in {
                "contest_id",
                "name",
                "sponsor",
                "mode",
                "start_time",
                "end_time",
                "bands",
            }
        },
    )


# ---------------------------------------------------------------------------
# DB operations
# ---------------------------------------------------------------------------


def upsert_contest_config(cfg: ContestConfig) -> None:
    """
    Insert or update a contest definition into DuckDB.
    """
    with connect() as con:
        con.execute(
            """
            INSERT INTO contests (contest_id, name, sponsor, mode, start_time, end_time, bands, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (contest_id) DO UPDATE SET
                name = EXCLUDED.name,
                sponsor = EXCLUDED.sponsor,
                mode = EXCLUDED.mode,
                start_time = EXCLUDED.start_time,
                end_time = EXCLUDED.end_time,
                bands = EXCLUDED.bands,
                metadata = EXCLUDED.metadata;
            """,
            [
                cfg.contest_id,
                cfg.name,
                cfg.sponsor,
                cfg.mode,
                cfg.start_time,
                cfg.end_time,
                ",".join(cfg.bands),
                cfg.metadata,
            ],
        )


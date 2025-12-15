# src/hamcontestlog/config.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional, Union

import yaml

from .db import connect


# Package resource root for built-in contest YAMLs
# This is a Python package directory: src/hamcontestlog/data/contests/
BUILTIN_CONTESTS_PKG = "hamcontestlog.data.contests"


@dataclass(frozen=True)
class ContestConfig:
    contest_id: str
    name: str = ""
    sponsor: str = ""
    mode: str = ""
    start_time: datetime = datetime(1970, 1, 1)
    end_time: datetime = datetime(1970, 1, 1)
    bands: tuple[str, ...] = ()
    metadata: Dict[str, Any] = None  # type: ignore[assignment]
    scoring: Dict[str, Any] = None  # type: ignore[assignment]
    notes: str = ""

    def __post_init__(self):
        object.__setattr__(self, "metadata", self.metadata or {})
        object.__setattr__(self, "scoring", self.scoring or {})


def _parse_dt(value: Union[str, datetime]) -> datetime:
    """
    Parse datetimes coming from YAML.

    - Accepts ISO strings with optional trailing 'Z'
    - Accepts datetime objects
    - Returns UTC-naive datetime (tzinfo=None)
    """
    if isinstance(value, datetime):
        dt = value
    else:
        s = str(value).strip()
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)

    if dt.tzinfo is not None:
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt


def load_yaml_from_package(relative_path: str) -> Dict[str, Any]:
    """
    Load YAML from built-in package resources (supports nested paths like 'iaru/2024iaru.yaml').

    IMPORTANT: this uses importlib.resources.files(), which supports subdirectories.
    """
    from importlib import resources

    root = resources.files(BUILTIN_CONTESTS_PKG)
    res = root.joinpath(relative_path)

    if not res.is_file():
        raise FileNotFoundError(
            f"Default contest YAML '{relative_path}' not found inside packaged defaults ({BUILTIN_CONTESTS_PKG})."
        )

    text = res.read_text(encoding="utf-8")
    data = yaml.safe_load(text) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML resource '{relative_path}' did not parse to a mapping.")
    return data


def load_yaml_from_file(path: Union[str, Path]) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"YAML file not found: {p}")
    data = yaml.safe_load(p.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"YAML file '{p}' did not parse to a mapping.")
    return data


def _parse_config(data: Dict[str, Any]) -> ContestConfig:
    contest_id = str(data.get("contest_id", "")).strip()
    if not contest_id:
        raise ValueError("contest_id is required in contest YAML/config")

    start_time = _parse_dt(data["start_time"]) if "start_time" in data else datetime(1970, 1, 1)
    end_time = _parse_dt(data["end_time"]) if "end_time" in data else datetime(1970, 1, 1)

    bands = tuple(data.get("bands") or [])

    return ContestConfig(
        contest_id=contest_id,
        name=str(data.get("name", "")),
        sponsor=str(data.get("sponsor", "")),
        mode=str(data.get("mode", "")),
        start_time=start_time,
        end_time=end_time,
        bands=bands,
        metadata=dict(data.get("metadata") or {}),
        scoring=dict(data.get("scoring") or {}),
        notes=str(data.get("notes", "")),
    )


def load_contest_config(source: str) -> ContestConfig:
    """
    Load contest config in the most convenient way:

    - If `source` looks like a file path that exists -> load YAML from disk
    - Else try to load from DB by contest_id
    - Else try built-in package defaults (relative path like 'cqww/2024cw.yaml', 'iaru/2024iaru.yaml', ...)
    """
    # 1) From disk YAML?
    p = Path(source)
    if p.exists() and p.is_file():
        return _parse_config(load_yaml_from_file(p))

    # 2) From DB as contest_id?
    cfg = get_contest_config(source)
    if cfg is not None:
        return cfg

    # 3) From packaged defaults by relative path
    return _parse_config(load_yaml_from_package(source))


def upsert_contest_config(cfg: ContestConfig) -> None:
    import json

    with connect() as con:
        con.execute(
            """
            INSERT INTO contests (contest_id, name, sponsor, mode, start_time, end_time, bands, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (contest_id) DO UPDATE SET
              name=excluded.name,
              sponsor=excluded.sponsor,
              mode=excluded.mode,
              start_time=excluded.start_time,
              end_time=excluded.end_time,
              bands=excluded.bands,
              metadata=excluded.metadata
            """,
            [
                cfg.contest_id,
                cfg.name,
                cfg.sponsor,
                cfg.mode,
                cfg.start_time,
                cfg.end_time,
                ",".join(cfg.bands),
                json.dumps(cfg.metadata or {}, ensure_ascii=False),
            ],
        )


def get_contest_config(contest_id: str) -> Optional[ContestConfig]:
    import json

    cid = contest_id.strip()
    if not cid:
        return None

    with connect() as con:
        row = con.execute(
            """
            SELECT contest_id, name, sponsor, mode, start_time, end_time, bands, metadata
            FROM contests
            WHERE contest_id = ?
            """,
            [cid],
        ).fetchone()

    if not row:
        return None

    # DuckDB may return JSON as str or as already-parsed Python object depending on version/settings.
    meta_raw = row[7]
    if meta_raw is None:
        metadata = {}
    elif isinstance(meta_raw, dict):
        metadata = meta_raw
    else:
        # assume string
        try:
            metadata = json.loads(str(meta_raw))
        except Exception:
            metadata = {}

    bands = tuple([b for b in (row[6] or "").split(",") if b])

    return ContestConfig(
        contest_id=row[0],
        name=row[1] or "",
        sponsor=row[2] or "",
        mode=row[3] or "",
        start_time=row[4],
        end_time=row[5],
        bands=bands,
        metadata=metadata,
    )


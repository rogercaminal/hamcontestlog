# src/hamcontestlog/cli.py
from __future__ import annotations

from pathlib import Path
from typing import Optional

import click

from . import __version__
from .config import load_contest_config, upsert_contest_config
from .fetch.cqww import CqwwContestSource
from .fetch.http import stream_bytes
from .fetch.rbn import SimpleRbnSource
from .ingest.logs import ingest_cabrillo_stream
from .ingest.rbn import ingest_rbn_urls
from .analysis.rate import hourly_rate, band_mode_breakdown
from .analysis.rbn_link import rbn_matches_for_station


@click.group()
@click.version_option(__version__)
def main() -> None:
    """Ham contest log analysis.

    Fetch public logs and RBN data, store them in DuckDB, and run analyses.
    """


# ---------------------------------------------------------------------------
# Contest subcommands
# ---------------------------------------------------------------------------


@main.group()
def contest() -> None:
    """Contest-related commands."""
    # Subcommands: see below.


@contest.command("add")
@click.argument("config_file", type=click.Path(exists=True, path_type=Path))
def contest_add(config_file: Path) -> None:
    """Add or update a contest definition from YAML.

    CONFIG_FILE should be a YAML file describing the contest, e.g.:

        contest_id: 2024cw
        name: CQ WW DX CW 2024
        sponsor: CQ
        mode: CW
        start_time: 2024-11-23T00:00:00Z
        end_time: 2024-11-24T23:59:59Z
        bands: [160m, 80m, 40m, 20m, 15m, 10m]
        # ... additional metadata ...
    """
    cfg = load_contest_config(config_file)
    upsert_contest_config(cfg)
    click.echo(f"Contest {cfg.contest_id} stored/updated.")


@contest.command("add-default")
@click.argument("default_path", type=str)
def contest_add_default(default_path: str) -> None:
    """Load a default contest config bundled with the package."""
    cfg = load_contest_config(default_path)
    upsert_contest_config(cfg)
    click.echo(f"Default contest {cfg.contest_id} loaded.")


# ---------------------------------------------------------------------------
# Ingest subcommands
# ---------------------------------------------------------------------------


@main.group()
def ingest() -> None:
    """Ingest logs and RBN data into DuckDB."""


@ingest.command("log")
@click.option("--contest", "contest_id", required=True, help="Contest ID, e.g. 2024cw.")
@click.option(
    "--source",
    "source_name",
    default="cqww",
    show_default=True,
    type=click.Choice(["cqww"]),
    help="Public log source.",
)
@click.option("--call", "callsign", required=True, help="Station callsign.")
def ingest_log(contest_id: str, source_name: str, callsign: str) -> None:
    """Ingest a single station log from the public site."""
    # For now, only CQWW is implemented as a source.
    source = CqwwContestSource(contest_id)

    url = source.log_url_for_callsign(callsign)
    fileobj = stream_bytes(url)
    ingest_cabrillo_stream(contest_id, fileobj)
    click.echo(f"Ingested log for {callsign.upper()} ({contest_id}) from {url}")


@ingest.command("logs")
@click.option("--contest", "contest_id", required=True, help="Contest ID, e.g. 2024cw.")
@click.option(
    "--source",
    "source_name",
    default="cqww",
    show_default=True,
    type=click.Choice(["cqww"]),
    help="Public log source.",
)
@click.option(
    "--limit",
    type=int,
    default=None,
    help="Limit number of logs to ingest (for testing).",
)
def ingest_logs(contest_id: str, source_name: str, limit: Optional[int]) -> None:
    """Ingest all public logs for a contest."""
    source = CqwwContestSource(contest_id)

    count = 0
    for url in source.iter_log_urls():
        fileobj = stream_bytes(url)
        ingest_cabrillo_stream(contest_id, fileobj)
        count += 1
        click.echo(f"[{count}] Ingested {url}")
        if limit is not None and count >= limit:
            break

    click.echo(f"Finished ingesting {count} log(s) for {contest_id}.")


@ingest.command("rbn")
@click.option("--contest", "contest_id", required=True, help="Contest ID, e.g. 2024cw.")
def ingest_rbn_cmd(contest_id: str) -> None:
    """Ingest RBN data for the contest.

    Uses a simple RBN source that maps contest dates to one or more CSV/ZIP URLs.
    """
    source = SimpleRbnSource()
    urls = list(source.urls_for_contest(contest_id))
    if not urls:
        click.echo(f"No RBN URLs configured for contest {contest_id}.")
        return

    ingest_rbn_urls(contest_id, urls)
    click.echo(f"Ingested RBN from {len(urls)} URL(s) for {contest_id}.")


# ---------------------------------------------------------------------------
# Analysis subcommands
# ---------------------------------------------------------------------------


@main.group()
def analyze() -> None:
    """Analysis commands."""


@analyze.command("rate")
@click.option("--contest", "contest_id", required=True, help="Contest ID, e.g. 2024cw.")
@click.option("--call", "callsign", required=True, help="Station callsign.")
def analyze_rate(contest_id: str, callsign: str) -> None:
    """Show hourly QSO rates for a station."""
    df = hourly_rate(contest_id, callsign)
    if df.empty:
        click.echo("No QSOs found.")
        return

    click.echo(f"Hourly rate for {callsign.upper()} in {contest_id}:")
    for _, row in df.iterrows():
        click.echo(f"{row['hour']}: {row['qso_count']} QSOs")


@analyze.command("bands")
@click.option("--contest", "contest_id", required=True, help="Contest ID, e.g. 2024cw.")
@click.option("--call", "callsign", required=True, help="Station callsign.")
def analyze_bands(contest_id: str, callsign: str) -> None:
    """Show QSOs per band/mode for a station."""
    df = band_mode_breakdown(contest_id, callsign)
    if df.empty:
        click.echo("No QSOs found.")
        return

    click.echo(f"Band/mode breakdown for {callsign.upper()} in {contest_id}:")
    for _, row in df.iterrows():
        click.echo(f"{row['band']} {row['mode']}: {row['qsos']} QSOs")


@analyze.command("rbn-coverage")
@click.option("--contest", "contest_id", required=True, help="Contest ID, e.g. 2024cw.")
@click.option("--call", "callsign", required=True, help="Station callsign.")
@click.option(
    "--time-window",
    type=int,
    default=120,
    show_default=True,
    help="Max time delta (seconds) between QSO and RBN spot.",
)
@click.option(
    "--freq-window",
    type=float,
    default=500.0,
    show_default=True,
    help="Max frequency delta (Hz) between QSO and RBN spot.",
)
def analyze_rbn_coverage(
    contest_id: str,
    callsign: str,
    time_window: int,
    freq_window: float,
) -> None:
    """Show QSOs with nearest RBN spots for a station."""
    df = rbn_matches_for_station(
        contest_id=contest_id,
        callsign=callsign,
        max_time_delta_sec=time_window,
        max_freq_delta_hz=freq_window,
    )
    if df.empty:
        click.echo("No RBN matches found.")
        return

    click.echo(
        f"RBN coverage for {callsign.upper()} in {contest_id} "
        f"(±{time_window}s, ±{freq_window} Hz):"
    )
    for _, row in df.iterrows():
        click.echo(
            f"{row['qso_time']} {row['band']} @ {row['freq_hz']} Hz -> "
            f"{row['spotter_call']} SNR {row['snr_db']} dB at {row['spot_time']}"
        )


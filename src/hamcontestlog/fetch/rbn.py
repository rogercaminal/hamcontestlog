# src/hamcontestlog/fetch/rbn.py
from __future__ import annotations

import csv
from datetime import datetime, timedelta, time
from io import BytesIO, TextIOWrapper
from typing import Iterator, Dict
import zipfile
import requests


def rbn_zip_urls_for_contest(start: datetime, end: datetime) -> list[str]:
    """
    Build list of daily RBN history ZIP URLs covering the contest window.

    Pattern:
        https://data.reversebeacon.net/rbn_history/YYYYMMDD.zip

    If end is exactly at midnight (00:00:00) we treat it as an exclusive bound,
    so a contest from 23rd 00:00 to 25th 00:00 only fetches 23rd and 24th.
    """
    # Treat exact midnight as "up to but not including this day"
    if end.time() == time(0, 0, 0):
        end_effective = end - timedelta(seconds=1)
    else:
        end_effective = end

    urls: list[str] = []
    cur = start.date()
    last = end_effective.date()

    while cur <= last:
        ymd = f"{cur.year:04d}{cur.month:02d}{cur.day:02d}"
        urls.append(f"https://data.reversebeacon.net/rbn_history/{ymd}.zip")
        cur += timedelta(days=1)

    return urls


def stream_rbn_rows_for_contest(start: datetime, end: datetime) -> Iterator[Dict]:
    """
    Yield RBN spots that fall within the contest time window.

    CSV format inside ZIP (as you showed):

        callsign,de_pfx,de_cont,freq,band,dx,dx_pfx,dx_cont,
        mode,db,date,speed,tx_mode

    Mapping we use:

        spotter  = callsign (col 0)
        dx_call  = dx       (col 5)
        freq_hz  = freq*kHz (col 3 * 1000)
        band     = band     (col 4)
        snr      = db       (col 9)
        speed    = speed    (col 11)
        ts       = date     (col 10, 'YYYY-MM-DD HH:MM:SS')
    """
    urls = rbn_zip_urls_for_contest(start, end)
    print(f"[RBN] Contest window: {start} -> {end}")
    print(f"[RBN] URLs: {urls}")

    for url in urls:
        print(f"[RBN] GET {url}")
        try:
            resp = requests.get(url, timeout=60)
        except Exception as e:
            print(f"[RBN]   -> request failed: {e}")
            continue

        if resp.status_code != 200:
            print(f"[RBN]   -> HTTP {resp.status_code}, skipping")
            continue

        try:
            with zipfile.ZipFile(BytesIO(resp.content)) as zf:
                # Use the first non-directory entry
                names = [n for n in zf.namelist() if not n.endswith("/")]
                if not names:
                    print("[RBN]   -> no files in ZIP, skipping")
                    continue

                inner_name = names[0]
                print(f"[RBN]   -> reading {inner_name}")

                with zf.open(inner_name, "r") as f:
                    text = TextIOWrapper(f, encoding="utf8", errors="replace")
                    reader = csv.reader(text)

                    row_count = 0
                    kept_count = 0

                    for row in reader:
                        row_count += 1
                        if not row:
                            continue

                        # Skip header if present
                        if row[0].strip().lower() == "callsign":
                            continue

                        if len(row) < 12:
                            continue

                        # date column
                        raw_ts = row[10].strip()
                        try:
                            ts = datetime.fromisoformat(raw_ts)
                        except Exception:
                            # fallback: replace space with 'T'
                            try:
                                ts = datetime.fromisoformat(raw_ts.replace(" ", "T"))
                            except Exception:
                                continue

                        if ts < start or ts > end:
                            continue

                        spotter = row[0].strip().upper()
                        dxcall = row[5].strip().upper()
                        band = row[4].strip()

                        try:
                            freq_hz = float(row[3]) * 1000.0  # kHz -> Hz
                        except Exception:
                            continue

                        try:
                            snr = float(row[9])
                        except Exception:
                            snr = None

                        try:
                            speed = float(row[11])
                        except Exception:
                            speed = None

                        kept_count += 1
                        yield {
                            "dx_call": dxcall,
                            "spotter": spotter,
                            "freq_hz": freq_hz,
                            "band": band,
                            "snr": snr,
                            "speed": speed,
                            "timestamp": ts,
                        }

                    print(f"[RBN]   -> read {row_count} rows, kept {kept_count} in window")
        except Exception as e:
            print(f"[RBN]   -> ZIP parse error: {e}")
            continue


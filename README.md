# HamContestLog

`hamcontestlog` is a Python package and CLI tool to ingest, enrich, score, and analyze amateur radio contest logs.
It is designed for **post‑contest analysis**, reproducibility, and large‑scale data exploration using DuckDB.

The project targets radio amateurs who want to combine **contest radio knowledge with data science**.

---

## Features

### Core functionality

- Contest definition management via YAML files
- Download and ingest public contest logs (CQWW + ARRL)
- Cabrillo-style log parsing
- DuckDB-backed storage (single-file database)
- Callsign enrichment (DXCC, zones, continent, prefix)
- Contest scoring (CQWW, ARRL DX, IARU HF)
- Reverse Beacon Network (RBN) historical ingestion
- High-performance bulk CSV ingestion
- SQL‑friendly analytics and Python interoperability

---

## Design philosophy

1. **Database-first**
   - DuckDB is the single source of truth
   - Python orchestrates, SQL computes

2. **Separation of concerns**
   - Raw QSOs are immutable
   - Derived data (scoring, multipliers) is recomputable

3. **Contest-agnostic core**
   - Contest-specific logic is isolated in scoring modules

4. **Reproducibility**
   - Entire pipelines can be re-run end-to-end

---

## Project structure

```text
hamcontestlog/
├── src/hamcontestlog/
│   ├── cli.py                  # CLI entry point
│   ├── db.py                   # DuckDB connection and schema
│   ├── config.py               # Contest config loading
│   ├── data/
│   │   └── contests/
│   │       ├── arrl/
│   │       │   ├── 2024arrldxcw.yaml
│   │       │   └── 2024arrldxssb.yaml
│   │       ├── cqww/
│   │       │   └── 2024cqwwcw.yaml
│   │       └── iaru/
│   │           └── 2024.yaml
│   ├── ingest/
│   │   ├── logs.py              # Contest log ingestion
│   │   └── rbn.py               # RBN ingestion (bulk)
│   ├── fetch/
│   │   ├── arrl.py              # ARRL public logs
│   │   ├── cqww.py              # CQWW public logs
│   │   └── rbn.py               # RBN URL generation
│   ├── enrich.py                # Callsign + QSO enrichment
│   ├── scoring/
│   │   ├── arrl.py              # ARRL DX scoring rules
│   │   ├── cqww.py              # CQWW scoring rules
│   │   └── iaru.py              # IARU HF scoring rules
│   └── analysis/
│       ├── rate.py
│       └── rbn_link.py
├── README.md
├── pyproject.toml
└── ...
```

---
## Installation

### End users (from PyPI)

```bash
pip install hamcontestlog
```

### Development install

```bash
git clone https://github.com/rogercaminal/hamcontestlog
cd hamcontestlog
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e .[dev]
pytest
```
---

## Packaging & tooling notes

This project intentionally keeps tooling **minimal**:

- Standard **PEP 621** metadata in `pyproject.toml`
- Installable and runnable with plain `pip`
- No mandatory use of Poetry, nox, or external coverage services
- Tests run directly with `pytest`

---

## Database

### Location

By default the DuckDB database is created at:

```
~/.hamcontestlog/hamcontestlog.duckdb
```

Override via environment variable:

```bash
export HAMCONTESTLOG_DB=/path/to/hamcontestlog.duckdb
```

---

## Database schema (conceptual)

### contests
Defines contest metadata and time windows.

| column | description |
|------|------------|
| contest_id | unique contest identifier |
| start_time | contest start (UTC, naive) |
| end_time | contest end (UTC, naive) |
| metadata | contest-specific settings |

---

### logs
One row per submitted log.

| column | description |
|------|------------|
| log_id | internal ID |
| contest_id | contest |
| callsign | station callsign |
| category | entry category |
| raw_metadata | Cabrillo header |

---

### qsos
Raw QSO facts only.

| column | description |
|------|------------|
| qso_id | internal ID |
| log_id | owning log |
| qso_time | timestamp |
| band | band |
| mode | mode |
| freq_hz | frequency |
| my_call | logging station |
| their_call | worked station |
| exch_sent | sent exchange |
| exch_rcvd | received exchange |

---

### call_enrichment
Contest-agnostic callsign data.

| column | description |
|------|------------|
| callsign | primary key |
| dxcc | DXCC entity |
| cq_zone | CQ zone |
| itu_zone | ITU zone |
| continent | continent |
| prefix | callsign prefix |
| state | US state |
| province | VE province |

Filled using `pyhamtools`. One lookup per distinct callsign.

---

### qso_scoring
Derived, contest-specific scoring.

| column | description |
|------|------------|
| qso_id | FK to qsos |
| contest_id | contest |
| dxcc | worked DXCC |
| cq_zone | worked CQ zone |
| itu_zone | worked ITU zone |
| prefix | worked prefix |
| state | worked US state |
| province | worked VE province |
| hq | IARU HQ identifier |
| points | QSO points |
| is_mult_dxcc | DXCC multiplier |
| is_mult_cq_zone | CQ zone multiplier |
| is_mult_itu | ITU multiplier |
| is_mult_prefix | WPX multiplier |
| is_mult_state | ARRL multiplier |
| is_mult_hq | IARU HQ multiplier |

This table can be dropped and recomputed at any time.

---

### rbn_spots
Reverse Beacon Network data.

| column | description |
|------|------------|
| contest_id | contest |
| spot_time | timestamp |
| spotter_call | RBN receiver |
| dx_call | spotted station |
| freq_hz | frequency |
| band | band |
| snr_db | signal strength |
| speed_wpm | CW speed |

---

## Contest configuration (YAML)

Contest definitions live in YAML files.

Example: `cqww/2024cqwwcw.yaml`

```yaml
contest_id: 2024cqwwcw
name: CQ World Wide DX Contest CW 2024
mode: CW
start_time: 2024-11-23T00:00:00Z
end_time:   2024-11-25T00:00:00Z

bands:
  - 160m
  - 80m
  - 40m
  - 20m
  - 15m
  - 10m

scoring:
  type: cqww

multipliers:
  by_dxcc: true
  by_zone: true
  per_band: true
```

Times are treated as **UTC, naive**, to avoid timezone issues.

---

## CLI usage

Every command opens the DuckDB database and creates the schema if it does not already exist.
The table impact below describes the command's logical reads and writes after the schema exists.

### Command reference

| command | purpose | tables read | tables written |
|------|------|------|------|
| `hamcontestlog --help` | Show CLI help. | none | none |
| `hamcontestlog --version` | Show installed package version. | none | none |
| `hamcontestlog contest add CONFIG_OR_DEFAULT` | Add or update a contest definition from a YAML file, built-in default path such as `cqww/2024cqwwcw.yaml`, or an existing contest id. | `contests` when resolving an existing contest id | `contests` |
| `hamcontestlog ingest log --contest CONTEST --call CALL --source cqww\|arrl` | Download and ingest one public Cabrillo log. | `contests` | `logs`, `qsos` |
| `hamcontestlog ingest logs --contest CONTEST --source cqww\|arrl [--limit N]` | Download and ingest all discoverable public Cabrillo logs for a contest, optionally capped by `--limit`. | `contests` | `logs`, `qsos` |
| `hamcontestlog rbn ingest --contest CONTEST` | Download historical RBN ZIP files covering the contest window and bulk-load matching spots. | `contests`, `rbn_spots` | `rbn_spots` |
| `hamcontestlog enrich calls --contest CONTEST` | Enrich every distinct callsign seen in the contest QSOs with DXCC, zones, continent, prefix, state, and province. | `logs`, `qsos` | `call_enrichment` |
| `hamcontestlog enrich qsos --contest CONTEST` | Rebuild per-QSO scoring seed rows for the contest. Run this after `enrich calls` and before scoring. | `logs`, `qsos`, `call_enrichment`, `qso_scoring` | `qso_scoring` |
| `hamcontestlog score cqww --contest CONTEST --call CALL` | Compute CQWW points and DXCC/CQ-zone multiplier flags for one submitted log. | `logs`, `qsos`, `call_enrichment`, `qso_scoring` | `qso_scoring` |
| `hamcontestlog score arrl --contest CONTEST --call CALL` | Compute ARRL DX points and DXCC or state/province multiplier flags for one submitted log. | `logs`, `qsos`, `call_enrichment` | `qso_scoring` |
| `hamcontestlog score iaru --contest CONTEST --call CALL` | Compute IARU HF points and ITU/HQ multiplier flags for one submitted log. | `logs`, `qsos`, `call_enrichment` | `qso_scoring` |
| `hamcontestlog analyze rate --contest CONTEST --call CALL` | Print hourly QSO count and scored point totals when scoring exists. | `logs`, `qsos`, `qso_scoring` | none |
| `hamcontestlog analyze bands --contest CONTEST --call CALL` | Print QSO, point, DXCC multiplier, and CQ-zone multiplier totals by band and mode. | `logs`, `qsos`, `qso_scoring` | none |
| `hamcontestlog analyze rbn-coverage --contest CONTEST --call CALL [--time-window SEC] [--freq-window HZ]` | Match the station's QSOs against nearby RBN spots by time and frequency. | `logs`, `qsos`, `rbn_spots` | none |

### Typical contest workflow

```bash
hamcontestlog contest add cqww/2024cqwwcw.yaml
hamcontestlog ingest log --contest 2024cqwwcw --call EF6T --source cqww
hamcontestlog enrich calls --contest 2024cqwwcw
hamcontestlog enrich qsos --contest 2024cqwwcw
hamcontestlog score cqww --contest 2024cqwwcw --call EF6T
hamcontestlog analyze bands --contest 2024cqwwcw --call EF6T
```

For batch ingestion:

```bash
hamcontestlog ingest logs --contest 2024cqwwcw --source cqww --limit 100
```

For RBN analysis:

```bash
hamcontestlog rbn ingest --contest 2024cqwwcw
hamcontestlog analyze rbn-coverage --contest 2024cqwwcw --call EF6T
```

### Scoring notes

`enrich qsos` resets and repopulates `qso_scoring` for the contest. The `score ...` commands then update those rows for one station at a time.

CQWW rules implemented:

- Same country: 0 points
- Same continent, non-NA: 1 point
- North America to North America, different countries: 2 points
- Different continents: 3 points

ARRL DX rules implemented:

- Valid QSOs are between W/VE and DX stations
- Valid QSOs are 3 points
- W/VE stations count DXCC multipliers by band
- DX stations count US state and VE province multipliers by band

IARU HF rules implemented:

- Same ITU zone or HQ station: 1 point
- Different ITU zone, same continent: 3 points
- Different continent: 5 points
- ITU zones and HQ societies are multipliers by band

---

## Python usage

```python
from hamcontestlog.db import connect

with connect() as con:
    df = con.execute("""
        SELECT band, SUM(points) AS pts
        FROM qsos q
        JOIN qso_scoring s USING (qso_id)
        WHERE s.contest_id = '2024cqwwcw'
        GROUP BY band
    """).fetchdf()

print(df)
```

---

## Supported contests

Currently implemented:

- CQWW (CW)
- ARRL DX (CW/SSB)
- IARU HF

Planned/illustrative:

- WPX (prefix multipliers)

Steps to add a contest:

1. Add a YAML config
2. Implement a scorer in `scoring/`
3. Register it in the CLI

---

## Limitations

- No real-time logging
- Limited contest roster (CQWW, ARRL DX, IARU)
- RBN ingestion is historical
- Exchange parsing is contest-specific

---

## Motivation

Contest logs are rich datasets.
Most tools are closed or rigid.
DuckDB + Python enables powerful, reproducible analysis.

Built by and for **radio amateurs who enjoy RF and data**.

---

## License

Distributed under the terms of the [MIT license][license],
_HamContestLog_ is free and open source software.

## Issues

If you encounter any problems,
please [file an issue] along with a detailed description.

## Credits

This project was generated from [@cjolowicz]'s [Hypermodern Python Cookiecutter] template.

[@cjolowicz]: https://github.com/cjolowicz
[pypi]: https://pypi.org/
[hypermodern python cookiecutter]: https://github.com/cjolowicz/cookiecutter-hypermodern-python
[file an issue]: https://github.com/rogercaminal/hamcontestlog/issues
[pip]: https://pip.pypa.io/

<!-- github-only -->

[license]: https://github.com/rogercaminal/hamcontestlog/blob/main/LICENSE
[contributor guide]: https://github.com/rogercaminal/hamcontestlog/blob/main/CONTRIBUTING.md
[command-line reference]: https://hamcontestlog.readthedocs.io/en/latest/usage.html

# HamContestLog

`hamcontestlog` is a Python package and CLI tool to ingest, enrich, score, and analyze amateur radio contest logs.
It is designed for **post‑contest analysis**, reproducibility, and large‑scale data exploration using DuckDB.

The project targets radio amateurs who want to combine **contest radio knowledge with data science**.

---

## Features

### Core functionality

- Contest definition management via YAML files
- Download and ingest public contest logs
- Cabrillo-style log parsing
- DuckDB-backed storage (single-file database)
- Callsign enrichment (DXCC, zones, continent, prefix)
- Contest scoring (CQWW implemented)
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
│   │       └── cqww/
│   │           └── 2024cw.yaml
│   ├── ingest/
│   │   ├── logs.py              # Contest log ingestion
│   │   └── rbn.py               # RBN ingestion (bulk)
│   ├── fetch/
│   │   └── rbn.py               # RBN URL generation
│   ├── enrich.py                # Callsign + QSO enrichment
│   ├── scoring/
│   │   └── cqww.py              # CQWW scoring rules
│   └── analysis/
│       ├── rate.py
│       └── bands.py
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

Filled using `pyhamtools`. One lookup per distinct callsign.

---

### qso_scoring
Derived, contest-specific scoring.

| column | description |
|------|------------|
| qso_id | FK to qsos |
| contest_id | contest |
| points | QSO points |
| is_mult_dxcc | DXCC multiplier |
| is_mult_cq_zone | CQ zone multiplier |
| is_mult_itu | ITU multiplier |
| is_mult_prefix | WPX multiplier |
| is_mult_state | ARRL multiplier |

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

Example: `cqww/2024cw.yaml`

```yaml
contest_id: 2024cw
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
  dxcc: true
  cq_zone: true
```

Times are treated as **UTC, naive**, to avoid timezone issues.

---

## CLI usage

### Add contest

```bash
hamcontestlog contest add-default cqww/2024cw.yaml
```
---

### Log ingestion

```bash
hamcontestlog ingest log --contest 2024cw --call EF6T
```

or, for all available callsigns for the contest,

```bash
hamcontestlog ingest logs --contest 2024cw
```

---

### Callsign enrichment

```bash
hamcontestlog enrich calls --contest 2024cw
```

---

### QSO enrichment

```bash
hamcontestlog enrich qsos --contest 2024cw
```

---

### Scoring (CQWW)

```bash
hamcontestlog score cqww --contest 2024cw --call EF6T
```

or, for all available callsigns for the contest,

```bash
hamcontestlog score cqww --contest 2024cw --all
```

CQWW rules implemented:

- Same country: 0 points
- Same continent (non‑NA): 1 point
- NA ↔ NA (different country): 2 points
- Different continents: 3 points

---

### Reverse Beacon Network ingestion

```bash
hamcontestlog rbn ingest --contest 2024cw
```

- Downloads daily RBN history ZIP files
- Uses DuckDB bulk CSV ingestion
- Skips malformed rows
- Filters by contest window

---

## Python usage

```python
from hamcontestlog.db import connect

with connect() as con:
    df = con.execute("""
        SELECT band, SUM(points) AS pts
        FROM qsos q
        JOIN qso_scoring s USING (qso_id)
        WHERE s.contest_id = '2024cw'
        GROUP BY band
    """).fetchdf()

print(df)
```

---

## Extending to other contests

Supported by design:

- IARU (ITU zones, HQ stations)
- WPX (prefix multipliers)
- ARRL DX (states/provinces)

Steps to add a contest:

1. Add a YAML config
2. Implement a scorer in `scoring/`
3. Register it in the CLI

---

## Limitations

- No real-time logging
- Only CQWW fully implemented
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

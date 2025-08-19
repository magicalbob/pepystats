pepystats
=========

Lightweight Python client & CLI for [pepy.tech](https://pepy.tech) download stats.

-   `overall` --- total downloads over a window (single number)

-   `detailed` --- day-by-day (or resampled) totals across all versions, **zero-filling missing days**

-   `versions` --- per-version series for selected versions

Not affiliated with pepy.tech.

Install
-------

```bash
pip install pepystats
```

Python ≥ 3.8.

CLI
---

```bash
pepystats --help
```

### Common flags

-   `project` --- PyPI package name (e.g. `chunkwrap`)

-   `--months INT` --- lookback window (default: 3). `0` disables trimming.

-   `--api-key KEY` --- pepy API key (or set `$PEPY_API_KEY`)

-   `--no-ci` --- present for parity; **ignored** with public v2 data

-   `--fmt {plain,md,csv}` --- output format (default: `plain`)

### `overall` --- total downloads (sum)

```bash
pepystats overall chunkwrap --months 1
```
> → prints a single integer

```bash
pepystats overall chunkwrap --months 1 --fmt md
```
> | downloads |
> |--------:|
> | 12345   |`

### `detailed` --- totals across all versions

-   **Daily** output includes **every day** in range with 0s for gaps.

-   Resampling is client-side:

    -   weekly buckets end **Saturday** (`W-SAT`)

    -   monthly buckets start **1st of month** (`MS`)

    -   yearly buckets start **Jan 1** (`YS`)

```bash
# Daily, Markdown table
pepystats detailed chunkwrap --months 1 --fmt md

# Weekly, CSV
pepystats detailed chunkwrap --months 3 --granularity weekly --fmt csv

# Plot (requires a display)
pepystats detailed chunkwrap --months 1 --plot`
```

### `recent` — last 7 days of downloads

```bash
pepystats recent chunkwrap --fmt md
pepystats recent chunkwrap --granularity weekly --plot
```

- Uses the same formatting and plotting options as detailed

- Always trims to the last 7 calendar days

### `versions` --- per-version series

```bash
pepystats versions chunkwrap --versions 2.4.1 2.4.0 --months 2 --fmt md
pepystats versions chunkwrap --versions 1.0 --granularity monthly --fmt csv`
```

### Exit codes

-   `0` success

-   `1` error (e.g., HTTP 5xx or unauthorized)

### Auth & rate-limits

-   Public v2 endpoint is used. If your key is required/limited, set it via:

```bash
export PEPY_API_KEY=...    # or use --api-key
```

-   `--no-ci` is retained for future parity but does not affect public v2.

Library usage (Python)
----------------------

```python
import pepystats as ps

# 1) Sum over window (int)
total = ps.get_overall("chunkwrap", months=3)

# 2) Detailed totals (DataFrame with columns: date, downloads, label='total')
df = ps.get_detailed("chunkwrap", months=1, granularity="daily")

# 3) Per-version series (DataFrame with columns: date, downloads, label=<version>)
dv = ps.get_versions("chunkwrap", versions=["2.4.1", "2.4.0"], months=2)

# Formatting helpers
print(ps.to_markdown(df))
csv_text = ps.to_csv(dv)`
```

### Data semantics

-   Dates are parsed as **UTC**, normalized to midnight, then rendered as `YYYY-MM-DD`.

-   `detailed` (daily) **zero-fills** missing days between the min/max dates after trimming.

-   Weekly/monthly/yearly outputs include empty buckets as zeros.

Troubleshooting
---------------

-   **401 Unauthorized**: set `--api-key` or `$PEPY_API_KEY`.

-   **"no data"**: the lookback window may exclude all rows; try `--months 0`.

-   **Plotting errors**: ensure a GUI backend or run without `--plot` on headless systems.

Development
-----------

```bash
# Tests & coverage
pytest -q --cov=pepystats --cov-report=term --cov-report=xml

# Lint (example tools)
ruff check .
pylint pepystats`
```

License
-------

GNU General Public License v3.0 (see `LICENSE` for details).

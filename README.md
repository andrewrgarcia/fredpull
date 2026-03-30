# fredpull

Minimal pipeline to download and build large-scale FRED time series panels.

---

## What is FRED?

[FRED (Federal Reserve Economic Data)](https://fred.stlouisfed.org/) is a database maintained by the Federal Reserve Bank of St. Louis.

It provides:

- Macroeconomic indicators (GDP, inflation, unemployment, interest rates)
- Financial data (yields, spreads, market indicators)
- Thousands of time series (800k+ series)

You can explore it here:
https://fred.stlouisfed.org/

API documentation:
https://fred.stlouisfed.org/docs/api/fred/

To use this project, you need a free API key:
https://fred.stlouisfed.org/docs/api/api_key.html

---

## Overview

`fredpull` lets you:

- Download thousands of FRED series in parallel
- Handle rate limits and network failures automatically
- Build a clean, aligned monthly panel (Parquet)
- Reproduce datasets locally (no prepackaged data required)

---

## Requirements

- Python 3.10+
- [`uv`](https://github.com/astral-sh/uv)

---

## Setup

```bash
git clone https://github.com/andrewrgarcia/fredpull
cd fredpull
uv sync
```

Set your FRED API key:

```bash
export FRED_API_KEY=your_key_here
```

---

## Usage

### Full pipeline (recommended)

```bash
make build START=1950-01-01
```

---

### Step-by-step

#### 1. Build metadata (only once)

```bash
make metadata
```

This creates:

```
data/metadata/fred_full_metadata.parquet
```

This file contains:

* Series IDs
* Titles and descriptions
* Frequency and coverage
* Metadata used to filter valid time series

---

#### 2. Download series

```bash
make download START=1950-01-01
```

Output:

```
data/raw_chunks/chunk_*.parquet
```

Each chunk contains a batch of time series aligned to monthly frequency.

---

#### 3. Merge into panel

```bash
make merge
```

Output:

```
data/raw/fred_monthly_master.parquet
```

This is the final dataset:

* Monthly frequency
* Thousands of aligned time series
* Ready for modeling or analysis

---

## Configuration

Environment variables:

```bash
FRED_API_KEY=your_key
FRED_START_DATE=1950-01-01
FRED_MIN_OBS=180
FRED_CHUNK_SIZE=400
```

---

## Data structure

```
data/
├── metadata/
│   └── fred_full_metadata.parquet
├── raw_chunks/
│   └── chunk_*.parquet
├── raw/
│   └── fred_monthly_master.parquet
└── processed/
```

---

## Notes

* Metadata only needs to be built once
* Downloads are resumable (existing chunks are skipped)
* Output is monthly-aligned and stored as Parquet
* The pipeline filters for sufficiently long, usable time series

---

## Design principles

* Minimal dependencies
* Fail-safe downloading
* Reproducibility over convenience
* No hidden preprocessing

---

## License

MIT

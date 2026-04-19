# Chess Analysis Bot

An interactive chess move-recommendation dashboard powered by the [Lichess Open Database](https://database.lichess.org/). Enter any position and an ELO range to see the top 5 most-played real moves by players at that level.

**Contributors:** Nelly Mossig, Chris Eggenberger

---

## Architecture

```
Lichess database.lichess.org
        │  (YYYY-MM.pgn.zst)
        ▼
┌─────────────────────────────────────────────────┐
│  ETL Stage 1 — Extraction  (choose one)         │
│    lichess_etl_1_extraction_parallel.py  (base) │
│    lichess_etl_1_extraction_dask.py      (Dask) │
│    lichess_etl_1_extraction_c_optimized.py (C)  │
│    lichess_etl_1_extraction_ray.py      (Ray)   │
│  Output: lichess_parquet_chunks_*/chunk_*.parquet│
└──────────────────────┬──────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────┐
│  ETL Stage 2 — Grouping                         │
│    lichess_etl_2_grouping.py                    │
│  Aggregates by ELO bucket, outputs per-ELO      │
│  parquet files.                                 │
└──────────────────────┬──────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────┐
│  ETL Stage 3 — Merging                          │
│    lichess_etl_3_final_merging.py               │
│  Combines partitions → monthly_databases/       │
│  YYYY-MM.parquet (or legacy single file)        │
└──────────────────────┬──────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────┐
│  Flask App  (app.py)                            │
│  DuckDB queries monthly_databases/*.parquet     │
└──────────────────────┬──────────────────────────┘
                       │
                       ▼
             Browser UI  (templates/index.html)
             Chessboard + Move Recommendations
```

---

## Prerequisites

- Python 3.9+
- Install all dependencies:

```bash
pip install -r requirements.txt
```

> **Note:** `ray[default]` requires Linux/macOS for full cluster support. On Windows, Ray works in local mode only.

---

## Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/BDP26/pm4-schach-analyse-bot.git
cd pm4-schach-analyse-bot

# 2. Install dependencies
pip install -r requirements.txt

# 3. Run the full ETL pipeline (once, or use automated update — see below)
python lichess_etl_1_extraction_parallel.py   # ~9h on 16-core desktop
python lichess_etl_2_grouping.py
python lichess_etl_3_final_merging.py

# 4. Start the app
python app.py
# Open http://127.0.0.1:5000
```

---

## ETL Pipeline

### Stage 1 — Extraction (choose one approach)

All Stage 1 scripts read a Lichess `.pgn.zst` file, filter games, parse chess positions, and write chunked parquet files. They are interchangeable — pick the one best suited to your environment.

| Script | Approach | Best For |
|--------|----------|----------|
| `lichess_etl_1_extraction_parallel.py` | `ProcessPoolExecutor` + pandas | Local machine, baseline |
| `lichess_etl_1_extraction_dask.py` | Dask Bag | Local machine, cleaner API |
| `lichess_etl_1_extraction_c_optimized.py` | ProcessPoolExecutor + **Polars** (Rust) | Local machine, faster I/O |
| `lichess_etl_1_extraction_ray.py` | **Ray** distributed tasks | Multi-node cluster |

All scripts output chunked parquet files with schema:

| Column | Type | Description |
|--------|------|-------------|
| `Event` | string | Game type (Blitz, Rapid, etc.) |
| `PlayerEloBucket` | int | ELO rounded to nearest 100 |
| `BaseFEN` | string | Board position (FEN without move counters) |
| `MoveSAN` | string | Move played (Standard Algebraic Notation) |

**Filters applied during extraction:**
- Bullet games excluded
- Abandoned / Rules infraction / Unterminated games excluded
- Both players must have ELO > 600
- Only the first 40 half-moves per game (opening + middlegame focus)

### Stage 2 — Grouping

```bash
python lichess_etl_2_grouping.py
```

Reads all chunk files and aggregates by `(BaseFEN, PlayerEloBucket, MoveSAN)`, counting `TimesPlayed`. Runs one DuckDB query per ELO bucket (100-step increments, 600–3900) to avoid OOM crashes. Outputs to `lichess_moves_partitioned/`.

### Stage 3 — Merging

```bash
python lichess_etl_3_final_merging.py
```

Streams all partitioned files into a single parquet file using DuckDB — no RAM bottleneck.

---

## Running on the Ray Cluster

The cluster has 4 nodes with 4× L4 GPUs, 192 GB RAM, and 64 CPUs each (256 CPUs / 768 GB RAM total).

**Access:**
- JupyterHub: [https://jupyter.jofu.org](https://jupyter.jofu.org)
- Username: `pm4-schach-analyse-bot`

**Steps:**
1. Log into JupyterHub and open a terminal.
2. Upload `lichess_etl_1_extraction_ray.py` and the `.pgn.zst` file.
3. Run:
   ```bash
   python lichess_etl_1_extraction_ray.py
   ```
   Ray auto-connects to the cluster (`address='auto'`). On a local machine it falls back to a local cluster.

---

## Automated Monthly Updates

`lichess_auto_update.py` checks `database.lichess.org` for new monthly files, downloads any that are missing, runs the full ETL pipeline, and saves each month as `monthly_databases/YYYY-MM.parquet`. The Flask app automatically picks up all monthly files via DuckDB glob.

```bash
# Process up to 3 new months
python lichess_auto_update.py

# Or use the shell wrapper (logs to logs/)
bash run_monthly_update.sh

# Options
python lichess_auto_update.py --dry-run          # list new months only
python lichess_auto_update.py --max-months 1     # process just one month
```
---

## App Configuration

The following constants are hardcoded and can be changed directly in the source files:

| Setting | File | Default | Description |
|---------|------|---------|-------------|
| Parquet source | `app.py` `_get_parquet_source()` | `monthly_databases/*.parquet` → fallback `lichess_moves_final.parquet` | Data files queried by the app |
| Max half-moves | ETL Stage 1 scripts | `40` | Stop parsing after this many half-moves per game |
| Min ELO | ETL Stage 1 scripts | `600` | Filter out games below this ELO |
| Chunk size | ETL Stage 1 scripts | `10,000,000` | Positions buffered in RAM before disk write |
| Max months per run | `lichess_auto_update.py` | `3` | Safety cap for automated downloads |
| Flask port | `app.py` | `5000` | Change with `app.run(port=XXXX)` |

---

## Performance Notes

All runtimes are for one month of Lichess standard-rated data (~10–20 GB compressed).

| Approach                                 | Hardware        | Benchmark (20,000 games)       | Notes                          |
| ---------------------------------------- | --------------- | ------------------------------ | ------------------------------ |
| Baseline (ProcessPoolExecutor + pandas)  | 16 logical cores, Windows | 39s / 20,000 games             | Reference |
| Dask Bag                                 | 16 logical cores, Windows | 60s / 20,000 games             | 0.65x vs baseline |
| C-optimized (Polars)                     | 16 logical cores, Windows | 42s / 20,000 games             | 0.94x vs baseline |
| Ray cluster                              | 256 CPUs across 4 nodes   | 0.6ms / 20,000 games           | 3'900'000x vs baseline |

**Key optimizations in the pipeline:**

- **Early stopping** — only first 40 half-moves per game (openings/middlegame only)
- **Data quality filtering** — bullet, abandoned, low-ELO games removed at read time
- **ELO bucketing** — exact ELOs rounded to 100-step increments for grouping
- **Chunking** — 10M positions buffered in RAM before each disk write
- **Horizontal partitioning** — Stage 2 aggregates one ELO bucket at a time to avoid OOM
- **Columnar storage** — Parquet compresses far better than CSV for this schema

---

## Project Structure

```
pm4-schach-analyse-bot/
├── app.py                                      Flask backend
├── templates/index.html                        Web UI
│
├── lichess_etl_1_extraction_parallel.py        Stage 1: baseline
├── lichess_etl_1_extraction_dask.py            Stage 1: Dask
├── lichess_etl_1_extraction_c_optimized.py     Stage 1: Polars/C
├── lichess_etl_1_extraction_ray.py             Stage 1: Ray cluster
├── lichess_etl_2_grouping.py                   Stage 2: aggregation
├── lichess_etl_3_final_merging.py              Stage 3: merge
│
├── lichess_auto_update.py                      Automated monthly fetcher
│
├── monthly_databases/                          Per-month parquet output
│   ├── 2024-01.parquet
│   └── ...
├── processed_months.json                       Tracks completed months
│
├── explore_parquet_files.ipynb                 Data exploration notebook
├── requirements.txt
└── README.md
```

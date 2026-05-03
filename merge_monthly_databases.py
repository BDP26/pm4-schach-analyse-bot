"""
Merge monthly_databases/*.parquet into a single sorted parquet file.

Run this once after adding new monthly databases. The output file is sorted
by BaseFEN so DuckDB's parquet row-group statistics let app.py skip ~99% of
the file on each FEN lookup — no in-memory table or index needed.

Usage:
    python merge_monthly_databases.py
"""

import os
import time

import duckdb

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MONTHLY_GLOB = os.path.join(SCRIPT_DIR, "monthly_databases", "*.parquet").replace("\\", "/")
OUTPUT_PATH = os.path.join(SCRIPT_DIR, "lichess_moves_final.parquet").replace("\\", "/")
TEMP_DIR = os.path.join(SCRIPT_DIR, "_duckdb_spill").replace("\\", "/")

# Cap memory so DuckDB spills sort/group state to TEMP_DIR instead of OOMing.
MEMORY_LIMIT = "16GB"


def main():
    if not any(f.endswith(".parquet") for f in os.listdir(os.path.dirname(MONTHLY_GLOB))):
        print(f"No parquet files found at {MONTHLY_GLOB}")
        return

    os.makedirs(TEMP_DIR, exist_ok=True)
    db = duckdb.connect()
    db.execute(f"SET memory_limit='{MEMORY_LIMIT}'")
    db.execute(f"SET temp_directory='{TEMP_DIR}'")
    # Suggested by DuckDB's OOM diagnostics — lets the engine reorder freely.
    db.execute("SET preserve_insertion_order=false")

    print(f"Merging '{MONTHLY_GLOB}' -> '{OUTPUT_PATH}' (sorted by BaseFEN)...")
    t0 = time.perf_counter()

    db.execute(f"""
        COPY (
            SELECT BaseFEN, PlayerEloBucket, MoveSAN, SUM(TimesPlayed) AS TimesPlayed
            FROM '{MONTHLY_GLOB}'
            GROUP BY BaseFEN, PlayerEloBucket, MoveSAN
            ORDER BY BaseFEN
        ) TO '{OUTPUT_PATH}' (FORMAT PARQUET);
    """)

    elapsed = time.perf_counter() - t0
    size_gb = os.path.getsize(OUTPUT_PATH) / (1024 ** 3)
    print(f"Done in {elapsed:.0f}s ({elapsed/60:.1f} min). Output: {size_gb:.2f} GB")

    if os.path.isdir(TEMP_DIR):
        try:
            for f in os.listdir(TEMP_DIR):
                os.remove(os.path.join(TEMP_DIR, f))
            os.rmdir(TEMP_DIR)
        except OSError:
            pass


if __name__ == "__main__":
    main()

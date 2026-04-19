"""
Lichess Automated Monthly Update
==================================
Checks https://database.lichess.org/ for new standard-rated monthly PGN files,
downloads any that haven't been processed yet, runs the full ETL pipeline on
each one, and saves the result as monthly_databases/YYYY-MM.parquet.

The Flask app queries 'monthly_databases/*.parquet' via DuckDB glob, so each
new month is automatically included without any further app changes.

--- USAGE ---

    python lichess_auto_update.py

    # Optional flags:
    python lichess_auto_update.py --max-months 1   # process at most 1 new month
    python lichess_auto_update.py --dry-run        # list new months without downloading

--- SCHEDULING ON THE CLUSTER ---

    # Run on the 2nd of every month at 03:00 (add to crontab with: crontab -e)
    0 3 2 * * cd /path/to/project && python lichess_auto_update.py >> logs/update.log 2>&1

--- OUTPUT ---

    monthly_databases/
        2024-01.parquet
        2024-02.parquet
        ...
    processed_months.json   (tracking file — do not delete)
"""

import argparse
import json
import os
import re
import shutil
import tempfile
import time

import duckdb
import requests
from tqdm import tqdm

# ==========================================
# CONFIGURATION
# ==========================================
OUTPUT_DIR = "monthly_databases"
TRACKING_FILE = "processed_months.json"
LICHESS_BASE_URL = "https://database.lichess.org/"
MAX_MONTHS_PER_RUN = 3   # safety cap: never download more than this per run
TEMP_CHUNKS_DIR = "_temp_chunks"
TEMP_PARTITIONS_DIR = "_temp_partitions"


# ==========================================
# STEP 1: Discover available months
# ==========================================
def fetch_available_months():
    """Scrape database.lichess.org and return list of (year_month, url) tuples."""
    print(f"Fetching file list from {LICHESS_BASE_URL}...")
    response = requests.get(LICHESS_BASE_URL, timeout=30)
    response.raise_for_status()

    # Actual href format on the page: standard/lichess_db_standard_rated_2024-01.pgn.zst
    # (relative path, no leading slash)
    pattern = r'href="((?:standard/)?lichess_db_standard_rated_(\d{4}-\d{2})\.pgn\.zst)"'
    matches = re.findall(pattern, response.text)

    months = []
    for path, year_month in matches:
        # Build absolute URL from the relative path
        url = LICHESS_BASE_URL.rstrip('/') + '/' + path.lstrip('/')
        months.append((year_month, url))

    months.sort(key=lambda x: x[0])
    print(f"Found {len(months)} months on database.lichess.org")
    return months


# ==========================================
# STEP 2: Track processed months
# ==========================================
def load_processed_months():
    if not os.path.exists(TRACKING_FILE):
        return set()
    with open(TRACKING_FILE, 'r') as f:
        return set(json.load(f))


def save_processed_month(year_month):
    processed = load_processed_months()
    processed.add(year_month)
    with open(TRACKING_FILE, 'w') as f:
        json.dump(sorted(processed), f, indent=2)
    print(f"   Recorded {year_month} in {TRACKING_FILE}")


# ==========================================
# STEP 3: Download
# ==========================================
def download_file(url, dest_path):
    print(f"   Downloading {url}")
    response = requests.get(url, stream=True, timeout=60)
    response.raise_for_status()

    total_size = int(response.headers.get('content-length', 0))
    with open(dest_path, 'wb') as f, tqdm(
        total=total_size,
        unit='B',
        unit_scale=True,
        unit_divisor=1024,
        desc="      Downloading",
        leave=False
    ) as bar:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            bar.update(len(chunk))

    file_size_gb = os.path.getsize(dest_path) / (1024 ** 3)
    print(f"   Download complete: {file_size_gb:.2f} GB")


# ==========================================
# STEP 4: ETL - Stage 1 (extraction)
# ==========================================

# Module-level worker — must be at module scope so ProcessPoolExecutor can pickle it
def _process_game(data):
    import io
    import chess.pgn
    pgn_string, clean_event, w_elo, b_elo = data
    positions = []
    try:
        game = chess.pgn.read_game(io.StringIO(pgn_string))
        if game is not None:
            board = game.board()
            for i, move in enumerate(game.mainline_moves()):
                if i >= 40:
                    break
                is_white = board.turn
                exact_elo = w_elo if is_white else b_elo
                bucketed_elo = (exact_elo // 100) * 100
                base_fen = board.fen().rsplit(' ', 2)[0]
                positions.append({
                    "Event": clean_event,
                    "PlayerEloBucket": bucketed_elo,
                    "BaseFEN": base_fen,
                    "MoveSAN": board.san(move)
                })
                board.push(move)
    except Exception:
        pass
    return positions


def run_stage1_extraction(pgn_zst_path, chunks_dir):
    """Run Stage 1 extraction using the baseline parallel implementation."""
    import zstandard as zstd
    import io
    import pandas as pd
    import concurrent.futures
    import multiprocessing

    print(f"   Stage 1: Extracting positions from {os.path.basename(pgn_zst_path)}...")

    os.makedirs(chunks_dir, exist_ok=True)

    num_cores = max(1, multiprocessing.cpu_count() - 2)
    BATCH = 2000
    CHUNK_LIMIT = 10_000_000

    positions_batch = []
    buffer = []
    current_headers = {}
    chunk_counter = 1
    games_processed = 0

    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
        with open(pgn_zst_path, 'rb') as f:
            dctx = zstd.ZstdDecompressor()
            with dctx.stream_reader(f) as reader:
                for line in io.TextIOWrapper(reader, encoding='utf-8'):
                    line = line.strip()
                    if not line:
                        continue
                    if line.startswith("["):
                        parts = line.split(' ', 1)
                        if len(parts) == 2:
                            current_headers[parts[0][1:]] = parts[1].strip('"]')
                    else:
                        games_processed += 1
                        raw_event = current_headers.get("Event", "")
                        if "Bullet" in raw_event:
                            current_headers = {}
                            continue
                        term = current_headers.get("Termination", "")
                        if term in ["Abandoned", "Rules infraction", "Unterminated"]:
                            current_headers = {}
                            continue
                        w_elo_str = current_headers.get("WhiteElo", "0")
                        b_elo_str = current_headers.get("BlackElo", "0")
                        w_elo = int(w_elo_str) if w_elo_str.isdigit() else 0
                        b_elo = int(b_elo_str) if b_elo_str.isdigit() else 0
                        if w_elo > 600 and b_elo > 600:
                            clean_event = raw_event.split(" https://")[0]
                            pgn_str = (
                                f"[FEN \"{current_headers.get('FEN', '')}\"]\n{line}"
                                if "FEN" in current_headers else line
                            )
                            buffer.append((pgn_str, clean_event, w_elo, b_elo))
                        current_headers = {}

                        if len(buffer) >= BATCH:
                            for out in executor.map(_process_game, buffer):
                                positions_batch.extend(out)
                            buffer = []

                        if len(positions_batch) >= CHUNK_LIMIT:
                            path = os.path.join(chunks_dir, f"chunk_{chunk_counter:04d}.parquet")
                            pd.DataFrame(positions_batch).to_parquet(path, index=False)
                            positions_batch = []
                            chunk_counter += 1

                if buffer:
                    for out in executor.map(_process_game, buffer):
                        positions_batch.extend(out)
                if positions_batch:
                    path = os.path.join(chunks_dir, f"chunk_{chunk_counter:04d}.parquet")
                    pd.DataFrame(positions_batch).to_parquet(path, index=False)

    print(f"   Stage 1 done: {games_processed:,} games, {chunk_counter} chunks")


# ==========================================
# STEP 5: ETL - Stage 2 (grouping)
# ==========================================
def run_stage2_grouping(chunks_dir, partitions_dir):
    print(f"   Stage 2: Aggregating by ELO bucket...")
    os.makedirs(partitions_dir, exist_ok=True)
    con = duckdb.connect()
    chunks_glob = os.path.join(chunks_dir, "*.parquet").replace("\\", "/")

    for elo in range(600, 4000, 100):
        query = f"""
            COPY (
                SELECT BaseFEN, PlayerEloBucket, MoveSAN, COUNT(*) as TimesPlayed
                FROM '{chunks_glob}'
                WHERE PlayerEloBucket = {elo}
                GROUP BY BaseFEN, PlayerEloBucket, MoveSAN
            ) TO '{partitions_dir}/aggregated_{elo}.parquet' (FORMAT PARQUET);
        """
        try:
            con.execute(query)
        except duckdb.IOException:
            pass  # no data for this ELO bucket

    print(f"   Stage 2 done")


# ==========================================
# STEP 6: ETL - Stage 3 (merge to monthly file)
# ==========================================
def run_stage3_merge(partitions_dir, output_parquet_path):
    print(f"   Stage 3: Merging to {output_parquet_path}...")
    partitions_glob = os.path.join(partitions_dir, "*.parquet").replace("\\", "/")
    query = f"""
        COPY (
            SELECT *
            FROM '{partitions_glob}'
        ) TO '{output_parquet_path}' (FORMAT PARQUET);
    """
    duckdb.execute(query)
    size_mb = os.path.getsize(output_parquet_path) / (1024 ** 2)
    print(f"   Stage 3 done: {size_mb:.1f} MB written to {output_parquet_path}")


# ==========================================
# MAIN
# ==========================================
def main():
    parser = argparse.ArgumentParser(description="Auto-update Lichess monthly databases")
    parser.add_argument("--max-months", type=int, default=MAX_MONTHS_PER_RUN,
                        help=f"Max months to process per run (default: {MAX_MONTHS_PER_RUN})")
    parser.add_argument("--dry-run", action="store_true",
                        help="List new months without downloading")
    args = parser.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    available = fetch_available_months()
    processed = load_processed_months()

    new_months = [(ym, url) for ym, url in available if ym not in processed]

    if not new_months:
        print("\n✅ All available months are already processed. Nothing to do.")
        return

    print(f"\nNew months to process: {[ym for ym, _ in new_months]}")

    if args.dry_run:
        print("Dry-run mode — exiting without downloading.")
        return

    to_process = new_months[:args.max_months]
    print(f"Processing {len(to_process)} month(s) this run (cap: {args.max_months})\n")

    for year_month, url in to_process:
        t0 = time.perf_counter()
        print(f"{'='*60}")
        print(f"Processing {year_month}")
        print(f"{'='*60}")

        with tempfile.TemporaryDirectory() as tmp_dir:
            pgn_path = os.path.join(tmp_dir, f"lichess_{year_month}.pgn.zst")
            chunks_dir = os.path.join(tmp_dir, TEMP_CHUNKS_DIR)
            partitions_dir = os.path.join(tmp_dir, TEMP_PARTITIONS_DIR)
            output_path = os.path.join(OUTPUT_DIR, f"{year_month}.parquet")

            try:
                download_file(url, pgn_path)
                run_stage1_extraction(pgn_path, chunks_dir)
                run_stage2_grouping(chunks_dir, partitions_dir)
                run_stage3_merge(partitions_dir, output_path)
                save_processed_month(year_month)

                elapsed = time.perf_counter() - t0
                print(f"\n✅ {year_month} complete in {elapsed:.0f}s ({elapsed/3600:.2f}h)\n")

            except Exception as e:
                print(f"\n❌ Failed to process {year_month}: {e}")
                # Clean up partial output if it exists
                if os.path.exists(output_path):
                    os.remove(output_path)
                # Continue with next month rather than crashing

    remaining = len(new_months) - len(to_process)
    if remaining > 0:
        print(f"ℹ️  {remaining} more month(s) available. Run again to continue.")
    else:
        print("✅ All new months processed.")


if __name__ == '__main__':
    main()

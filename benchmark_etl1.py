"""
ETL Stage 1 Speed Benchmark
============================
Reads the first MAX_GAMES qualifying games from a Lichess PGN file into memory,
then runs all four extraction approaches on that identical in-memory dataset
and compares their wall-clock processing + write time.

Supports both .pgn.zst (compressed) and .pgn (uncompressed) input files.

--- LOCAL (ThinkPad X1) ---
    python benchmark_etl1.py lichess_db_standard_rated_2026-03.pgn.zst

--- CLUSTER (Ray, 256 CPUs) ---
    python benchmark_etl1.py lichess_db_standard_rated_2026-03.pgn.zst --max-games 500000

--- OPTIONS ---
    --max-games N      Number of qualifying games to benchmark (default: 20000)
    --skip-ray         Skip the Ray approach (useful if Ray is not installed)
    --update-readme    Automatically update the Performance Notes table in README.md
"""

import argparse
import io
import multiprocessing
import os
import platform
import re
import shutil
import sys
import tempfile
import time

import chess.pgn
import concurrent.futures
import pandas as pd

# ==========================================
# DEFAULTS
# ==========================================
# 20 000 games ≈ 1-3 min per approach on a ThinkPad X1 (8-12 logical cores).
# Increase to 500 000+ when running on the cluster (256 CPUs).
MAX_GAMES_DEFAULT = 20_000


# ==========================================
# FILE OPENING (supports .pgn and .pgn.zst)
# ==========================================
def open_pgn_stream(file_path):
    """Return a text iterator over the PGN file (compressed or plain)."""
    if file_path.endswith(".zst"):
        import zstandard as zstd
        compressed = open(file_path, "rb")
        dctx = zstd.ZstdDecompressor()
        reader = dctx.stream_reader(compressed)
        return io.TextIOWrapper(reader, encoding="utf-8"), [compressed, reader]
    else:
        f = open(file_path, "r", encoding="utf-8")
        return f, [f]


# ==========================================
# PHASE 1: Load games into memory (shared across all approaches)
# ==========================================
def load_games(file_path, max_games):
    """
    Stream the PGN file and return up to max_games qualifying game tuples.
    Filtering is identical to all ETL Stage 1 scripts.
    Returns list of (pgn_string, clean_event, w_elo, b_elo).
    """
    print(f"Loading up to {max_games:,} qualifying games from {os.path.basename(file_path)}...")
    t0 = time.perf_counter()

    games = []
    current_headers = {}
    text_stream, handles = open_pgn_stream(file_path)

    try:
        for line in text_stream:
            line = line.strip()
            if not line:
                continue

            if line.startswith("["):
                parts = line.split(" ", 1)
                if len(parts) == 2:
                    current_headers[parts[0][1:]] = parts[1].strip('"]')
            else:
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
                    pgn_string = (
                        f"[FEN \"{current_headers.get('FEN', '')}\"]\n{line}"
                        if "FEN" in current_headers
                        else line
                    )
                    games.append((pgn_string, clean_event, w_elo, b_elo))
                    if len(games) >= max_games:
                        break

                current_headers = {}
    finally:
        for h in reversed(handles):
            try:
                h.close()
            except Exception:
                pass

    elapsed = time.perf_counter() - t0
    print(f"Loaded {len(games):,} games in {elapsed:.1f}s\n")
    return games


# ==========================================
# MODULE-LEVEL WORKER (must be at module scope for pickle)
# ==========================================
def _parse_game(data):
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
                base_fen = board.fen().rsplit(" ", 2)[0]
                positions.append({
                    "Event": clean_event,
                    "PlayerEloBucket": bucketed_elo,
                    "BaseFEN": base_fen,
                    "MoveSAN": board.san(move),
                })
                board.push(move)
    except Exception:
        pass
    return positions


# ==========================================
# APPROACH 1: Baseline (ProcessPoolExecutor + pandas)
# ==========================================
def run_baseline(games, output_dir):
    num_cores = max(1, multiprocessing.cpu_count() - 2)
    BATCH = 2000

    positions = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
        for i in range(0, len(games), BATCH):
            batch = games[i:i + BATCH]
            for result in executor.map(_parse_game, batch):
                positions.extend(result)

    pd.DataFrame(positions).to_parquet(
        os.path.join(output_dir, "chunk_0001.parquet"), index=False
    )
    return len(positions)


# ==========================================
# APPROACH 2: Dask Bag
# ==========================================
def run_dask(games, output_dir):
    import dask.bag as db

    num_cores = max(1, multiprocessing.cpu_count() - 2)
    bag = db.from_sequence(games, npartitions=num_cores)
    results = bag.map(_parse_game).compute()

    positions = []
    for r in results:
        positions.extend(r)

    pd.DataFrame(positions).to_parquet(
        os.path.join(output_dir, "chunk_0001.parquet"), index=False
    )
    return len(positions)


# ==========================================
# APPROACH 3: C-optimized (ProcessPoolExecutor + Polars)
# ==========================================
def run_c_optimized(games, output_dir):
    import polars as pl

    num_cores = max(1, multiprocessing.cpu_count() - 2)
    BATCH = 2000

    positions = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
        for i in range(0, len(games), BATCH):
            batch = games[i:i + BATCH]
            for result in executor.map(_parse_game, batch):
                positions.extend(result)

    df = pl.from_records(
        positions,
        schema={
            "Event": pl.Utf8,
            "PlayerEloBucket": pl.Int32,
            "BaseFEN": pl.Utf8,
            "MoveSAN": pl.Utf8,
        },
    )
    df.write_parquet(
        os.path.join(output_dir, "chunk_0001.parquet"), compression="snappy"
    )
    return len(positions)


# ==========================================
# APPROACH 4: Ray
# ==========================================
def run_ray(games, output_dir):
    import ray

    ray.init(address="auto", ignore_reinit_error=True)

    @ray.remote
    def _parse_batch(batch):
        results = []
        for data in batch:
            results.extend(_parse_game(data))
        return results

    BATCH = max(500, len(games) // max(1, multiprocessing.cpu_count() - 2))
    futures = [
        _parse_batch.remote(games[i:i + BATCH])
        for i in range(0, len(games), BATCH)
    ]
    positions = []
    for result in ray.get(futures):
        positions.extend(result)

    pd.DataFrame(positions).to_parquet(
        os.path.join(output_dir, "chunk_0001.parquet"), index=False
    )
    return len(positions)


# ==========================================
# RESULT TABLE
# ==========================================
def print_results(results, max_games):
    
    hw = f"{multiprocessing.cpu_count()} logical cores, {platform.system()}"
    baseline_time = results.get("Baseline", {}).get("time")

    def make_row(key, label, notes_suffix=""):
        data = results.get(key, {})
        t = data.get("time")
        err = data.get("error")
        if err:
            runtime = "ERROR"
            notes = err[:60]
        elif t is not None:
            runtime = f"{t:.0f}s / {max_games:,} games"
            if baseline_time and key != "Baseline":
                speedup = baseline_time / t
                notes = f"{speedup:.2f}x vs baseline on {hw}{notes_suffix}"
            else:
                notes = f"Reference — measured on {hw}{notes_suffix}"
        else:
            runtime = "skipped"
            notes = "Not run"
        return runtime, notes

    rows = {
        "Baseline": make_row("Baseline", "Baseline"),
        "Dask":     make_row("Dask", "Dask Bag"),
        "C-opt":    make_row("C-optimized", "C-optimized (Polars)"),
        "Ray":      make_row("Ray", "Ray cluster"),
    }

    w1, w2, w3, w4 = 40, 15, 30, 30

    new_table = (
        f"| {'Approach':<{w1}} | {'Hardware':<{w2}} | {f'Benchmark ({max_games:,} games)':<{w3}} | {'Notes':<{w4}} |\n"
        f"| {'-' * w1} | {'-' * w2} | {'-' * w3} | {'-' * w4} |\n"
        f"| {'Baseline (ProcessPoolExecutor + pandas)':<{w1}} | {hw:<{w2}} | {str(rows['Baseline'][0]):<{w3}} | {str(rows['Baseline'][1]):<{w4}} |\n"
        f"| {'Dask Bag':<{w1}} | {hw:<{w2}} | {str(rows['Dask'][0]):<{w3}} | {str(rows['Dask'][1]):<{w4}} |\n"
        f"| {'C-optimized (Polars)':<{w1}} | {hw:<{w2}} | {str(rows['C-opt'][0]):<{w3}} | {str(rows['C-opt'][1]):<{w4}} |\n"
        f"| {'Ray cluster':<{w1}} | {hw:<{w2}} | {str(rows['Ray'][0]):<{w3}} | {str(rows['Ray'][1]):<{w4}} |"
    )

    print(new_table)

    


# ==========================================
# MAIN
# ==========================================
def main(file, max_games, skip_ray):
    
    if not os.path.exists(file):
        sys.exit(f"File not found: {file}")

    # --- Phase 1: load games into memory ---
    games = load_games(file, max_games)
    if not games:
        sys.exit("No qualifying games found in the file.")

    approaches = [
        ("Baseline",     run_baseline),
        ("Dask",         run_dask),
        ("C-optimized",  run_c_optimized),
    ]
    if not skip_ray:
        approaches.append(("Ray", run_ray))

    # --- Phase 2: run each approach ---
    results = {}
    for name, fn in approaches:
        print(f"Running {name}...")
        with tempfile.TemporaryDirectory() as tmp:
            try:
                t0 = time.perf_counter()
                n_positions = fn(games, tmp)
                elapsed = time.perf_counter() - t0
                results[name] = {"time": elapsed, "positions": n_positions}
                print(f"  {name}: {elapsed:.1f}s → {n_positions:,} positions")
            except Exception as e:
                results[name] = {"error": str(e)}
                print(f"  {name}: ERROR — {e}")

    # --- Phase 3: report ---
    print_results(results, len(games))


if __name__ == "__main__":
    file = r"C:\Users\nelly\GitHub\pm4-schach-analyse-bot\lichess_db_standard_rated_2026-03.pgn"
    max_games = 20_000
    skip_ray = True
    main(file, max_games, skip_ray)

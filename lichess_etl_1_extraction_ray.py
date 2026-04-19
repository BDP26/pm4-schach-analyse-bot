"""
Lichess ETL Stage 1 - Ray Cluster Approach
===========================================
Distributes chess game processing across the 4-node Ray cluster
(total: 256 CPUs, ~768 GB RAM across all nodes).

Produces identical output to lichess_etl_1_extraction_parallel.py:
  chunks of parquet files with columns: Event, PlayerEloBucket, BaseFEN, MoveSAN

Output directory: lichess_parquet_chunks_ray/

--- HOW TO RUN ON THE CLUSTER ---

1. Open JupyterHub: https://jupyter.jofu.org
   Username: pm4-schach-analyse-bot
   Password: ZHAW123

2. Upload this script and the .pgn.zst file to your JupyterHub workspace.

3. Run in a notebook cell or terminal:
      python lichess_etl_1_extraction_ray.py

   Ray will auto-connect to the cluster (address='auto') when run inside the
   JupyterHub environment. On a local machine it falls back to a local cluster.

4. (Optional) To view the Ray Dashboard, open an SSH tunnel first:
      ssh -N -L 8265:127.0.0.1:8265 ray_viewer@160.85.253.224
      Password: bigdataprojekt2026
   Then open http://localhost:8265 in your browser.

--- ARCHITECTURE ---
The file is read sequentially (unavoidable: streaming zstd decompression).
Each batch of 2000 games is submitted as Ray remote tasks, distributing the
CPU-intensive chess.pgn parsing across all available cluster nodes.

Timing is printed at the end for comparison with other approaches.
"""

import zstandard as zstd
import io
import os
import time
import pandas as pd
import chess.pgn

try:
    import ray
except ImportError:
    raise ImportError("Ray is not installed. Run: pip install 'ray[default]'")


# ==========================================
# WORKER FUNCTION (Ray remote task)
# ==========================================
@ray.remote
def process_single_game_remote(data):
    pgn_string, clean_event, w_elo, b_elo = data
    positions = []

    try:
        pgn_io = io.StringIO(pgn_string)
        game = chess.pgn.read_game(pgn_io)

        if game is not None:
            board = game.board()

            for i, move in enumerate(game.mainline_moves()):
                if i >= 40:
                    break

                is_white_turn = board.turn
                exact_elo = w_elo if is_white_turn else b_elo
                bucketed_elo = (exact_elo // 100) * 100
                base_fen = board.fen().rsplit(' ', 2)[0]
                played_move_san = board.san(move)

                positions.append({
                    "Event": clean_event,
                    "PlayerEloBucket": bucketed_elo,
                    "BaseFEN": base_fen,
                    "MoveSAN": played_move_san
                })
                board.push(move)
    except Exception:
        pass

    return positions


@ray.remote
def process_batch_remote(batch):
    """Process a batch of games on a single Ray worker to reduce task overhead."""
    all_positions = []
    for data in batch:
        pgn_string, clean_event, w_elo, b_elo = data
        try:
            pgn_io = io.StringIO(pgn_string)
            game = chess.pgn.read_game(pgn_io)
            if game is not None:
                board = game.board()
                for i, move in enumerate(game.mainline_moves()):
                    if i >= 40:
                        break
                    is_white_turn = board.turn
                    exact_elo = w_elo if is_white_turn else b_elo
                    bucketed_elo = (exact_elo // 100) * 100
                    base_fen = board.fen().rsplit(' ', 2)[0]
                    played_move_san = board.san(move)
                    all_positions.append({
                        "Event": clean_event,
                        "PlayerEloBucket": bucketed_elo,
                        "BaseFEN": base_fen,
                        "MoveSAN": played_move_san
                    })
                    board.push(move)
        except Exception:
            pass
    return all_positions


# ==========================================
# MAIN FUNCTION
# ==========================================
def process_lichess_ray(file_path, output_dir="lichess_parquet_chunks_ray"):
    # Connect to cluster (auto-detects JupyterHub cluster environment)
    ray.init(address='auto', ignore_reinit_error=True)

    cluster_resources = ray.cluster_resources()
    total_cpus = int(cluster_resources.get('CPU', 1))
    total_memory_gb = cluster_resources.get('memory', 0) / (1024 ** 3)
    print(f"Connected to Ray cluster:")
    print(f"  CPUs available: {total_cpus}")
    print(f"  Memory available: {total_memory_gb:.1f} GB")
    print(f"  Full resources: {cluster_resources}\n")

    print(f"Starting RAY ETL pipeline for {file_path}...\n")

    os.makedirs(output_dir, exist_ok=True)

    # Use more games per batch on the cluster to amortize Ray task overhead
    # Each batch runs as one Ray task on one worker node
    GAMES_PER_BATCH = max(500, min(2000, total_cpus * 30))
    CHUNK_SIZE_LIMIT = 10_000_000

    raw_games_buffer = []
    positions_batch = []
    current_headers = {}
    current_moves = ""
    games_processed = 0
    chunk_counter = 1
    pending_futures = []
    t_start = time.perf_counter()

    with open(file_path, 'rb') as compressed_file:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(compressed_file) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')

            for line in text_stream:
                line = line.strip()
                if not line:
                    continue

                if line.startswith("["):
                    parts = line.split(' ', 1)
                    if len(parts) == 2:
                        key = parts[0][1:]
                        val = parts[1].strip('"]')
                        current_headers[key] = val
                else:
                    current_moves = line
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
                        pgn_string = (
                            f"[FEN \"{current_headers.get('FEN', '')}\"]\n{current_moves}"
                            if "FEN" in current_headers
                            else current_moves
                        )
                        raw_games_buffer.append((pgn_string, clean_event, w_elo, b_elo))

                    current_headers = {}
                    current_moves = ""

                    # --- SUBMIT BATCH TO RAY CLUSTER ---
                    if len(raw_games_buffer) >= GAMES_PER_BATCH:
                        # Submit as a single Ray task (processes the whole batch on one worker)
                        future = process_batch_remote.remote(raw_games_buffer)
                        pending_futures.append(future)
                        raw_games_buffer = []

                        # Collect completed futures when we have enough queued
                        # (keeps pipeline saturated without unbounded memory growth)
                        if len(pending_futures) >= total_cpus * 2:
                            done, pending_futures = pending_futures[:total_cpus], pending_futures[total_cpus:]
                            for result in ray.get(done):
                                positions_batch.extend(result)
                            print(f"Scanned {games_processed} games... Positions in RAM: {len(positions_batch)}")

                    # --- SAVE CHUNK ---
                    if len(positions_batch) >= CHUNK_SIZE_LIMIT:
                        output_filename = os.path.join(output_dir, f"chunk_{chunk_counter:04d}.parquet")
                        print(f"\n--- Saving Chunk {chunk_counter} to disk ---")
                        pd.DataFrame(positions_batch).to_parquet(output_filename, index=False)
                        positions_batch = []
                        chunk_counter += 1

            # Flush remaining buffer
            if raw_games_buffer:
                pending_futures.append(process_batch_remote.remote(raw_games_buffer))

            # Collect all remaining futures
            if pending_futures:
                for result in ray.get(pending_futures):
                    positions_batch.extend(result)

            if positions_batch:
                output_filename = os.path.join(output_dir, f"chunk_{chunk_counter:04d}.parquet")
                pd.DataFrame(positions_batch).to_parquet(output_filename, index=False)

    elapsed = time.perf_counter() - t_start
    print(f"\n✅ RAY EXTRACTION COMPLETE! Total time: {elapsed:.1f}s ({elapsed/3600:.2f}h)")
    print(f"   Games scanned: {games_processed:,}")
    print(f"   Chunks written: {chunk_counter}")
    print(f"   Cluster CPUs used: {total_cpus}")

    ray.shutdown()


if __name__ == '__main__':
    process_lichess_ray("lichess_db_standard_rated_2026-02.pgn.zst")

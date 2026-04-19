"""
Lichess ETL Stage 1 - Dask Approach
====================================
Replaces ProcessPoolExecutor with Dask Bag for parallel game processing.
Produces identical output to lichess_etl_1_extraction_parallel.py:
  chunks of parquet files with columns: Event, PlayerEloBucket, BaseFEN, MoveSAN

Output directory: lichess_parquet_chunks_dask/

Timing is printed at the end for comparison with other approaches.
"""

import zstandard as zstd
import io
import os
import time
import dask.bag as db
import dask.dataframe as dd
import pandas as pd
import chess.pgn
import multiprocessing


# ==========================================
# WORKER FUNCTION (runs inside Dask workers)
# ==========================================
def process_single_game(data):
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


def flatten(nested):
    for inner in nested:
        yield from inner


# ==========================================
# MAIN FUNCTION
# ==========================================
def process_lichess_dask(file_path, output_dir="lichess_parquet_chunks_dask"):
    print(f"Starting DASK ETL pipeline for {file_path}...\n")

    os.makedirs(output_dir, exist_ok=True)

    num_cores = max(1, multiprocessing.cpu_count() - 2)
    print(f"Using {num_cores} Dask workers\n")

    BATCH_SIZE = 2000
    CHUNK_SIZE_LIMIT = 10_000_000

    raw_games_buffer = []
    positions_batch = []
    chunk_counter = 1
    games_processed = 0
    t_start = time.perf_counter()

    with open(file_path, 'rb') as compressed_file:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(compressed_file) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')

            current_headers = {}
            current_moves = ""

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

                    # --- DISPATCH BATCH TO DASK ---
                    if len(raw_games_buffer) >= BATCH_SIZE:
                        bag = db.from_sequence(raw_games_buffer, npartitions=num_cores)
                        results = bag.map(process_single_game).compute()
                        for worker_output in results:
                            positions_batch.extend(worker_output)
                        raw_games_buffer = []
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
                bag = db.from_sequence(raw_games_buffer, npartitions=max(1, num_cores))
                results = bag.map(process_single_game).compute()
                for worker_output in results:
                    positions_batch.extend(worker_output)

            if positions_batch:
                output_filename = os.path.join(output_dir, f"chunk_{chunk_counter:04d}.parquet")
                pd.DataFrame(positions_batch).to_parquet(output_filename, index=False)

    elapsed = time.perf_counter() - t_start
    print(f"\n✅ DASK EXTRACTION COMPLETE! Total time: {elapsed:.1f}s ({elapsed/3600:.2f}h)")
    print(f"   Games scanned: {games_processed:,}")
    print(f"   Chunks written: {chunk_counter}")


if __name__ == '__main__':
    process_lichess_dask("lichess_db_standard_rated_2026-02.pgn.zst")

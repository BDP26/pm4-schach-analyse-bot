import zstandard as zstd
import io
import pandas as pd
import chess.pgn
import os
import concurrent.futures
import multiprocessing


# ==========================================
# WORKER FUNCTION (Runs on multiple cores)
# ==========================================
def process_single_game(data):
    pgn_string, clean_event, w_elo, b_elo = data
    positions = []

    try:
        pgn_io = io.StringIO(pgn_string)
        game = chess.pgn.read_game(pgn_io)

        if game is not None:
            board = game.board()

            # Neu: enumerate nutzen, um mitzuzählen
            for i, move in enumerate(game.mainline_moves()):

                # LIMIT: Nach 40 Halbzügen (20 vollen Zügen) abbrechen!
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


# ==========================================
# BOSS FUNCTION (Reads stream, manages workers)
# ==========================================
def process_custom_lichess_parallel(file_path, output_dir="lichess_parquet_chunks"):
    print(f"Starting PARALLEL ETL pipeline for {file_path}...\n")

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    positions_batch = []
    raw_games_buffer = []  # The "To-Do List" for our workers

    current_headers = {}
    current_moves = ""
    games_processed = 0
    chunk_counter = 1

    CHUNK_SIZE_LIMIT = 10000000
    BATCH_SIZE_FOR_WORKERS = 2000  # Give 2000 games to workers at a time

    # Automatically use most of your CPU cores (leave 2 for Windows background tasks)
    num_cores = max(1, multiprocessing.cpu_count() - 2)
    print(f"🔥 Spawning {num_cores} worker processes! Hold on to your RAM...\n")

    # Start the worker pool
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as executor:
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

                        # --- FAST TEXT FILTERS ---
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
                            pgn_string = f"[FEN \"{current_headers.get('FEN', '')}\"]\n{current_moves}" if "FEN" in current_headers else current_moves

                            # Add to the "To-Do List" instead of calculating immediately
                            raw_games_buffer.append((pgn_string, clean_event, w_elo, b_elo))

                        current_headers = {}
                        current_moves = ""

                        # --- SEND TO WORKERS ---
                        if len(raw_games_buffer) >= BATCH_SIZE_FOR_WORKERS:
                            # Map the buffer to the workers
                            results = executor.map(process_single_game, raw_games_buffer)

                            # Collect results from all workers
                            for worker_output in results:
                                positions_batch.extend(worker_output)

                            raw_games_buffer = []  # Clear the To-Do list
                            print(f"Scanned {games_processed} games... Positions in RAM: {len(positions_batch)}")

                        # --- SAVE CHUNK ---
                        if len(positions_batch) >= CHUNK_SIZE_LIMIT:
                            output_filename = os.path.join(output_dir, f"chunk_{chunk_counter:04d}.parquet")
                            print(f"\n--- Saving Chunk {chunk_counter} to disk ---")
                            df = pd.DataFrame(positions_batch)
                            df.to_parquet(output_filename, index=False)
                            positions_batch = []
                            chunk_counter += 1

                # Clean up any remaining games at the end of the file
                if len(raw_games_buffer) > 0:
                    results = executor.map(process_single_game, raw_games_buffer)
                    for worker_output in results:
                        positions_batch.extend(worker_output)
                if len(positions_batch) > 0:
                    output_filename = os.path.join(output_dir, f"chunk_{chunk_counter:04d}.parquet")
                    df = pd.DataFrame(positions_batch)
                    df.to_parquet(output_filename, index=False)

                print("\n✅ EXTRACTION COMPLETE!")


# Windows requires this block to safely spawn processes!
if __name__ == '__main__':
    process_custom_lichess_parallel("lichess_db_standard_rated_2026-02.pgn.zst")
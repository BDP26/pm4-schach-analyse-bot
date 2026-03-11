import zstandard as zstd
import io
import pandas as pd
import chess.pgn

def process_custom_lichess(file_path):
    print(f"Starting custom ETL pipeline for {file_path}...\n")

    positions_batch = []
    current_headers = {}
    current_moves = ""
    games_processed = 0
    games_saved = 0

    with open(file_path, 'rb') as compressed_file:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(compressed_file) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')

            for line in text_stream:
                line = line.strip()
                if not line:
                    continue

                # 1. PARSE METADATA
                if line.startswith("["):
                    parts = line.split(' ', 1)
                    if len(parts) == 2:
                        key = parts[0][1:]
                        val = parts[1].strip('"]')
                        current_headers[key] = val

                # 2. PARSE MOVES & APPLY FILTERS
                else:
                    current_moves = line
                    games_processed += 1

                    # --- FILTER 1: Event type (No Bullet/UltraBullet) ---
                    raw_event = current_headers.get("Event", "")
                    if "Bullet" in raw_event:
                        current_headers = {}
                        continue # Skip to next game

                    # Clean the event string by removing the URL for our Parquet file
                    clean_event = raw_event.split(" https://")[0]

                    # --- FILTER 2: Termination ---
                    term = current_headers.get("Termination", "")
                    bad_terminations = ["Abandoned", "Rules infraction", "Unterminated"]
                    if term in bad_terminations:
                        current_headers = {}
                        continue

                    # --- FILTER 3: Elo > 600 for BOTH players ---
                    w_elo_str = current_headers.get("WhiteElo", "0")
                    b_elo_str = current_headers.get("BlackElo", "0")

                    w_elo = int(w_elo_str) if w_elo_str.isdigit() else 0
                    b_elo = int(b_elo_str) if b_elo_str.isdigit() else 0

                    if w_elo > 600 and b_elo > 600:

                        # === CONVERT TO FEN (python-chess) ===
                        # We only reach this point if ALL filters are passed!
                        pgn_string = f"[FEN \"{current_headers.get('FEN', '')}\"]\n{current_moves}" if "FEN" in current_headers else current_moves
                        pgn_io = io.StringIO(pgn_string)
                        game = chess.pgn.read_game(pgn_io)

                        if game is not None:
                            board = game.board()
                            for i, move in enumerate(game.mainline_moves()):
                                board.push(move)

                                # Append only the specific columns you requested
                                positions_batch.append({
                                    "Event": clean_event,
                                    "WhiteElo": w_elo,
                                    "BlackElo": b_elo,
                                    "MoveNumber": i + 1,
                                    "MoveSAN": board.san(move),
                                    "PositionFEN": board.fen()
                                })

                            games_saved += 1
                            if games_saved % 100 == 0:
                                print(f"Scanned {games_processed} total games... Found {games_saved} games matching criteria (<600 Elo).")

                    # 3. RESET FOR NEXT GAME
                    current_headers = {}
                    current_moves = ""

                    # 4. SAVE AND STOP
                    if len(positions_batch) >= 100000:
                        print("\nLimit reached! Saving to Parquet...")
                        df = pd.DataFrame(positions_batch)
                        df.to_parquet("test_batch.parquet", index=False)
                        print("Done! File saved as: test_batch.parquet")
                        return

# Run the script (make sure your file name is exactly as it appears on your hard drive)
process_custom_lichess("lichess_db_standard_rated_2026-02.pgn.zst")

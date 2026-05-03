"""
Benchmark: chess.pgn.read_game (baseline) vs fast SAN tokenizer + chess.Board (proposed)
=========================================================================================

Both pipelines run with identical parallelism (ProcessPoolExecutor, same core count)
and the same pre-buffered set of games — so the only variable is the per-game parser.

What the FAST parser does differently:
  * No chess.pgn.read_game / Game / Node tree.
  * No StringIO wrap, no comment/NAG/variation walking.
  * Regex-tokenizes the PGN moves line (strips {clk}, move numbers, NAGs, results).
  * Calls chess.Board.parse_san(token) directly. parse_san hits the same C-extension
    move-generation code that chess.pgn would have called anyway, just without the
    surrounding Python overhead.
  * Output schema is identical to the baseline: (Event, PlayerEloBucket, BaseFEN, MoveSAN).

Optional further win (commented out): trust the input SAN token instead of re-deriving
it via board.san(move). Skips ambiguity / check-status recomputation. Only safe if you
trust the producer (Lichess does emit canonical SAN).

Usage:
    python lichess_etl_1_extraction_fast_parser_benchmark.py [path/to/file.pgn.zst] [n_games]
"""

import zstandard as zstd
import io
import re
import os
import sys
import time
import chess
import chess.pgn
import concurrent.futures
import multiprocessing


# ============================================================
# WORKER A: BASELINE (chess.pgn.read_game)
# ============================================================
def process_baseline(data):
    pgn_string, clean_event, w_elo, b_elo = data
    positions = []
    try:
        pgn_io = io.StringIO(pgn_string)
        game = chess.pgn.read_game(pgn_io)
        if game is None:
            return positions
        board = game.board()
        for i, move in enumerate(game.mainline_moves()):
            if i >= 40:
                break
            exact_elo = w_elo if board.turn else b_elo
            bucketed_elo = (exact_elo // 100) * 100
            base_fen = board.fen().rsplit(' ', 2)[0]
            played_move_san = board.san(move)
            positions.append((clean_event, bucketed_elo, base_fen, played_move_san))
            board.push(move)
    except Exception:
        pass
    return positions


# ============================================================
# WORKER B: FAST PARSER (regex tokenizer + chess.Board)
# ============================================================
# Strip {...} clock/comment annotations. Lichess does not nest braces.
_COMMENT_RE = re.compile(r"\{[^}]*\}")
# Strip move numbers like "1." or "1..." (with optional whitespace before dots)
_MOVENUM_RE = re.compile(r"\d+\.(?:\.\.)?")
# Strip Numeric Annotation Glyphs ($1, $2, ...)
_NAG_RE = re.compile(r"\$\d+")
# Strip result tokens
_RESULT_RE = re.compile(r"\b(?:1-0|0-1|1/2-1/2|\*)\b")


def _tokenize_moves(moves_line: str):
    """Return a list of bare SAN tokens from a Lichess PGN moves line."""
    s = _COMMENT_RE.sub(" ", moves_line)
    s = _MOVENUM_RE.sub(" ", s)
    s = _NAG_RE.sub(" ", s)
    s = _RESULT_RE.sub(" ", s)
    return s.split()


def process_fast(data):
    moves_line, start_fen, clean_event, w_elo, b_elo = data
    positions = []
    board = chess.Board(start_fen) if start_fen else chess.Board()
    try:
        tokens = _tokenize_moves(moves_line)
        for i, token in enumerate(tokens):
            if i >= 40:
                break
            move = board.parse_san(token)
            exact_elo = w_elo if board.turn else b_elo
            bucketed_elo = (exact_elo // 100) * 100
            base_fen = board.fen().rsplit(' ', 2)[0]
            played_move_san = board.san(move)
            # Faster alternative if you trust producer-canonical SAN:
            #   played_move_san = token.rstrip("+#")
            positions.append((clean_event, bucketed_elo, base_fen, played_move_san))
            board.push(move)
    except Exception:
        pass
    return positions


# ============================================================
# SHARED INPUT BUFFERING (one decompression pass, fed to both)
# ============================================================
def _open_pgn_stream(file_path: str):
    """Return (text_stream, close_callbacks) for either a plain .pgn or a .pgn.zst file."""
    if file_path.endswith(".zst"):
        compressed = open(file_path, "rb")
        dctx = zstd.ZstdDecompressor()
        reader = dctx.stream_reader(compressed)
        text_stream = io.TextIOWrapper(reader, encoding="utf-8")
        return text_stream, [text_stream, reader, compressed]
    else:
        text_stream = open(file_path, "r", encoding="utf-8", errors="replace")
        return text_stream, [text_stream]


def buffer_games(file_path: str, n_games: int):
    """Read the .pgn or .pgn.zst, apply the same fast filters as the production
    pipeline, and return TWO buffers of the same games:

      baseline_buffer[i] -> (pgn_string,  event, w_elo, b_elo)
      fast_buffer[i]     -> (moves_line,  start_fen, event, w_elo, b_elo)
    """
    baseline_buffer = []
    fast_buffer = []
    current_headers = {}

    text_stream, closeables = _open_pgn_stream(file_path)
    try:
        for line in text_stream:
            line = line.strip()
            if not line:
                continue

            if line.startswith("["):
                parts = line.split(" ", 1)
                if len(parts) == 2:
                    key = parts[0][1:]
                    val = parts[1].strip('"]')
                    current_headers[key] = val
                continue

            moves_line = line
            raw_event = current_headers.get("Event", "")
            if "Bullet" in raw_event:
                current_headers = {}
                continue

            term = current_headers.get("Termination", "")
            if term in ("Abandoned", "Rules infraction", "Unterminated"):
                current_headers = {}
                continue

            w_elo_str = current_headers.get("WhiteElo", "0")
            b_elo_str = current_headers.get("BlackElo", "0")
            w_elo = int(w_elo_str) if w_elo_str.isdigit() else 0
            b_elo = int(b_elo_str) if b_elo_str.isdigit() else 0

            if w_elo > 600 and b_elo > 600:
                clean_event = raw_event.split(" https://")[0]
                start_fen = current_headers.get("FEN", "")
                pgn_string = (
                    f'[FEN "{start_fen}"]\n{moves_line}' if start_fen else moves_line
                )
                baseline_buffer.append((pgn_string, clean_event, w_elo, b_elo))
                fast_buffer.append((moves_line, start_fen, clean_event, w_elo, b_elo))
                if len(fast_buffer) >= n_games:
                    return baseline_buffer, fast_buffer

            current_headers = {}
    finally:
        for c in closeables:
            try:
                c.close()
            except Exception:
                pass

    return baseline_buffer, fast_buffer


# ============================================================
# BENCHMARK RUNNER
# ============================================================
def run_parallel(worker_fn, buffer, num_cores, label, chunksize=200):
    print(f"\n[{label}] starting — {len(buffer):,} games, {num_cores} workers, chunksize={chunksize}")
    t0 = time.perf_counter()
    total_positions = 0
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_cores) as ex:
        for out in ex.map(worker_fn, buffer, chunksize=chunksize):
            total_positions += len(out)
    elapsed = time.perf_counter() - t0
    rate = len(buffer) / elapsed if elapsed > 0 else 0
    print(f"[{label}] done — {elapsed:.2f}s, {total_positions:,} positions, {rate:,.0f} games/s")
    return elapsed, total_positions


def run_single_threaded(worker_fn, buffer, label):
    """Pure per-game cost, no IPC. Useful to confirm the parser itself is faster."""
    print(f"\n[{label} | 1 thread] starting — {len(buffer):,} games")
    t0 = time.perf_counter()
    total_positions = 0
    for item in buffer:
        total_positions += len(worker_fn(item))
    elapsed = time.perf_counter() - t0
    rate = len(buffer) / elapsed if elapsed > 0 else 0
    print(f"[{label} | 1 thread] done — {elapsed:.2f}s, {total_positions:,} positions, {rate:,.0f} games/s")
    return elapsed, total_positions


# ============================================================
# MAIN
# ============================================================
if __name__ == "__main__":
    INPUT = sys.argv[1] if len(sys.argv) > 1 else "lichess_db_standard_rated_2026-03.pgn"
    N_GAMES = int(sys.argv[2]) if len(sys.argv) > 2 else 20_000

    if not os.path.exists(INPUT):
        print(f"Input file not found: {INPUT}")
        print("Usage: python lichess_etl_1_extraction_fast_parser_benchmark.py [file.pgn.zst] [n_games]")
        sys.exit(1)

    num_cores = max(1, multiprocessing.cpu_count() - 2)

    print(f"Reading & filtering up to {N_GAMES:,} games from {INPUT} ...")
    t0 = time.perf_counter()
    baseline_buf, fast_buf = buffer_games(INPUT, N_GAMES)
    print(f"  buffered {len(baseline_buf):,} games in {time.perf_counter()-t0:.1f}s")

    # Sanity check: both buffers represent the same games
    assert len(baseline_buf) == len(fast_buf)

    # ---- Single-threaded mini-bench on first 1000 (isolates parser cost) ----
    sample = min(1000, len(baseline_buf))
    print(f"\n--- Single-threaded micro-benchmark on first {sample} games ---")
    st_base_t, st_base_n = run_single_threaded(process_baseline, baseline_buf[:sample], "BASELINE")
    st_fast_t, st_fast_n = run_single_threaded(process_fast,     fast_buf[:sample],     "FAST    ")
    print(f"  per-game speedup (parser only): {st_base_t / st_fast_t:.2f}x")
    if st_base_n != st_fast_n:
        delta_pct = (st_base_n - st_fast_n) / st_base_n * 100
        print(f"  ⚠ position-count delta: {st_base_n - st_fast_n} ({delta_pct:.3f}%)")

    # ---- Full parallel benchmark ----
    print(f"\n--- Parallel benchmark on {len(baseline_buf):,} games ---")
    p_base_t, p_base_n = run_parallel(process_baseline, baseline_buf, num_cores,
                                      "BASELINE (chess.pgn.read_game)")
    p_fast_t, p_fast_n = run_parallel(process_fast,     fast_buf,     num_cores,
                                      "FAST     (regex + chess.Board)")

    # ---- Summary ----
    print("\n" + "=" * 60)
    print(f"SUMMARY  ({len(baseline_buf):,} games, {num_cores} cores)")
    print("=" * 60)
    print(f"Baseline:  {p_base_t:7.2f}s   {p_base_n:,} positions")
    print(f"Fast:      {p_fast_t:7.2f}s   {p_fast_n:,} positions")
    print(f"Speedup:   {p_base_t / p_fast_t:.2f}x")
    if p_base_n != p_fast_n:
        delta_pct = (p_base_n - p_fast_n) / p_base_n * 100
        print(f"⚠ Position-count delta vs baseline: {p_base_n - p_fast_n} ({delta_pct:.3f}%)")
        print("  Likely causes: malformed games where chess.pgn is more lenient than parse_san,")
        print("  or annotation tokens our regex didn't strip. Inspect with --diff if it matters.")
    else:
        print("✓ Position counts match exactly.")

from flask import Flask, request, jsonify, render_template
import chess
import duckdb
import os
import glob

app = Flask(__name__)

# Anchor all data paths to this file's directory so the app works regardless
# of which CWD it was launched from (shell, IDE run config, systemd, etc.).
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
MONTHLY_DIR = os.path.join(SCRIPT_DIR, "monthly_databases")
MONTHLY_GLOB = os.path.join(MONTHLY_DIR, "*.parquet").replace("\\", "/")
LEGACY_FILE = os.path.join(SCRIPT_DIR, "lichess_moves_final.parquet").replace("\\", "/")

db = duckdb.connect()

# Determine which parquet source to query. Re-evaluated per request because
# lichess_auto_update.py may drop new monthly files into MONTHLY_DIR while
# the app is running. Returns None if no data source is available.
# The merged file (produced by merge_monthly_databases.py and sorted by
# BaseFEN) is preferred when present — DuckDB's row-group statistics make
# FEN lookups much faster than scanning the per-month glob.
def _get_parquet_source():
    if os.path.exists(LEGACY_FILE):
        return LEGACY_FILE
    if glob.glob(os.path.join(MONTHLY_DIR, "*.parquet")):
        return MONTHLY_GLOB
    return None


def get_base_fen(board):
    """Formats the FEN to match typical database groupings (strips move counters)"""
    return " ".join(board.fen().split(" ")[:4])


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/setup_board', methods=['POST'])
def setup_board():
    """Takes a space-separated sequence of moves and returns the resulting FEN."""
    data = request.get_json(silent=True) or {}
    move_sequence = data.get('moves', '').strip()
    board = chess.Board()

    if move_sequence:
        try:
            for move in move_sequence.split():
                board.push_san(move)
        except ValueError:
            return jsonify({"error": "Invalid move sequence provided."}), 400

    return jsonify({"fen": board.fen()})


@app.route('/get_opponent_move', methods=['POST'])
def get_opponent_move():
    """Queries DuckDB for the top 5 most played moves in the current position and ELO range."""
    data = request.get_json(silent=True) or {}
    fen = data.get('fen')

    if not fen:
        return jsonify({"error": "Missing 'fen' field in request."}), 400

    try:
        board = chess.Board(fen)
    except (ValueError, TypeError):
        return jsonify({"error": "Invalid FEN provided."}), 400

    try:
        elo_min = int(data.get('elo_min', 0))
        elo_max = int(data.get('elo_max', 3000))
    except (TypeError, ValueError):
        return jsonify({"error": "elo_min and elo_max must be integers."}), 400

    base_fen = get_base_fen(board)

    parquet_source = _get_parquet_source()
    if parquet_source is None:
        return jsonify({"error": "No move database available on the server."}), 503

    # Use a CTE to sum the plays across the Elo buckets, then calculate percentages.
    # cursor() gives this request its own DuckDB cursor so concurrent Flask
    # threads don't trample each other on the shared connection.
    query = f"""
            WITH MoveStats AS (SELECT MoveSAN,
                                      SUM(TimesPlayed) as plays
                               FROM '{parquet_source}'
                               WHERE BaseFEN = ? AND PlayerEloBucket >= ? AND PlayerEloBucket <= ?
                               GROUP BY MoveSAN)
            SELECT MoveSAN,
                   plays * 100.0 / (SELECT SUM(plays) FROM MoveStats) as percentage
            FROM MoveStats
            ORDER BY plays DESC LIMIT 5
            """

    results = db.cursor().execute(query, (base_fen, elo_min, elo_max)).fetchall()

    if results:
        # Build a list of dictionaries for the frontend
        moves_data = [{"move": row[0], "likelihood": round(row[1], 1)} for row in results]

        return jsonify({
            "move": moves_data[0]["move"],  # Auto-play the top move
            "alternatives": moves_data  # Send all 5 back for the UI
        })
    else:
        return jsonify({"error": "Position not found in dataset for this ELO range."}), 404


if __name__ == '__main__':
    app.run(debug=True)
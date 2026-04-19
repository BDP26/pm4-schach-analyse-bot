from flask import Flask, request, jsonify, render_template
import chess
import duckdb
import os
import glob

app = Flask(__name__)

# Connect to DuckDB
db = duckdb.connect()

# Determine which parquet source to query:
# 1. monthly_databases/*.parquet  — populated by lichess_auto_update.py (preferred)
# 2. lichess_moves_final.parquet  — legacy single-file fallback
def _get_parquet_source():
    monthly_files = glob.glob(os.path.join("monthly_databases", "*.parquet"))
    if monthly_files:
        return "monthly_databases/*.parquet"
    return "lichess_moves_final.parquet"


def get_base_fen(board):
    """Formats the FEN to match typical database groupings (strips move counters)"""
    return " ".join(board.fen().split(" ")[:4])


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/setup_board', methods=['POST'])
def setup_board():
    """Takes a space-separated sequence of moves and returns the resulting FEN."""
    data = request.json
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
    data = request.json
    fen = data.get('fen')
    elo_min = data.get('elo_min', 0)
    elo_max = data.get('elo_max', 3000)

    board = chess.Board(fen)
    base_fen = get_base_fen(board)

    parquet_source = _get_parquet_source()

    # Use a CTE to sum the plays across the Elo buckets, then calculate percentages
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

    results = db.execute(query, (base_fen, elo_min, elo_max)).fetchall()

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
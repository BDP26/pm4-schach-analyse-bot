from flask import Flask, request, jsonify, render_template
import chess
import duckdb

app = Flask(__name__)

# Connect to DuckDB (it will query the parquet file directly)
db = duckdb.connect()


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
    """Queries DuckDB for the most played move in the current position."""
    data = request.json
    fen = data.get('fen')
    elo_bucket = data.get('elo', 1500)  # Default to 1500

    board = chess.Board(fen)
    base_fen = get_base_fen(board)

    # Query DuckDB directly from the parquet file
    query = """
            SELECT MoveSAN
            FROM 'lichess_moves_final.parquet'
            WHERE BaseFEN = ? AND PlayerEloBucket = ?
            ORDER BY TimesPlayed DESC
                LIMIT 1 \
            """

    result = db.execute(query, (base_fen, elo_bucket)).fetchone()

    if result:
        return jsonify({"move": result[0]})
    else:
        return jsonify({"error": "Position not found in dataset for this ELO."}), 404


if __name__ == '__main__':
    app.run(debug=True)
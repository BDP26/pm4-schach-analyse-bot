import zstandard as zstd
import io
import pandas as pd
import chess.pgn

def process_lichess_stream(file_path):
    print(f"Öffne Stream für {file_path}...")

    # Unsere temporäre Sammelliste für die einzelnen Züge (Zeilen für Parquet)
    positions_batch = []

    # Variablen, um ein einzelnes Spiel zwischenzuspeichern
    current_game_text = []
    games_processed = 0

    # 1. Zstandard-Dekompressor einrichten
    with open(file_path, 'rb') as compressed_file:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(compressed_file) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')

            # 2. Zeile für Zeile durch die Datei iterieren
            for line in text_stream:
                # Ein neues Spiel beginnt in Lichess immer mit [Event ...
                if line.startswith("[Event ") and current_game_text:

                    # === FILTER-PHASE ===
                    game_string = "".join(current_game_text)

                    # Wir prüfen manuell im Text (sehr schnell), ob das Spiel unseren Kriterien entspricht
                    if 'Rated Rapid game' in game_string or 'Rated Classical game' in game_string:
                        # Extrahiere Elo (rudimentär, aber schnell)
                        try:
                            # Sucht die Zeile mit WhiteElo und schneidet die Zahl raus
                            w_elo_line = [l for l in current_game_text if l.startswith('[WhiteElo ')][0]
                            w_elo = int(w_elo_line.split('"')[1])

                            # Nur Spiele analysieren, wo Weiß zwischen 1000 und 1500 Elo hat
                            if 1000 <= w_elo <= 1500:

                                # === UMWANDLUNGS-PHASE (Zug für Zug) ===
                                # Jetzt nutzen wir python-chess, um die Züge zu verstehen
                                pgn_io = io.StringIO(game_string)
                                game = chess.pgn.read_game(pgn_io)

                                board = game.board()

                                # Gehe durch die Züge der Partie
                                for i, move in enumerate(game.mainline_moves()):
                                    # Wir brechen nach 30 Halbzügen (15 volle Züge) ab!
                                    if i >= 30:
                                        break

                                    board.push(move)

                                    # Speichere die Position in unserer Liste
                                    positions_batch.append({
                                        "white_elo": w_elo,
                                        "move_number": i + 1,           # 1 = Weiß Zug 1, 2 = Schwarz Zug 1
                                        "move_san": board.san(move),    # z.B. "Nf3"
                                        "fen": board.fen()              # Die exakte Brettposition
                                    })

                                games_processed += 1
                                if games_processed % 500 == 0:
                                    print(f"{games_processed} passende Spiele verarbeitet... (Gesammelte Züge/FENs: {len(positions_batch)})")
                        except Exception as e:
                            # Falls ein Spiel kaputte Metadaten hat, überspringen wir es einfach
                            pass

                    # Leere den Textspeicher für das nächste Spiel
                    current_game_text = [line]

                    # === SPEICHER-PHASE (Der Abbruch für deinen Test) ===
                    # Wenn wir 50.000 Züge/FENs gesammelt haben, speichern wir ab und stoppen!
                    if len(positions_batch) >= 50000:
                        print("\nLimit erreicht! Speichere Parquet-Datei...")
                        df = pd.DataFrame(positions_batch)

                        output_file = "test_chunk_001.parquet"
                        df.to_parquet(output_file, index=False)

                        print(f"Fertig! Datei gespeichert als: {output_file}")
                        print(f"Die Tabelle hat {len(df)} Zeilen und {len(df.columns)} Spalten.")
                        return # Bricht die komplette Funktion ab!

                else:
                    current_game_text.append(line)

# HIER DEN DATEINAMEN DEINER HERUNTERGELADENEN DATEI EINTRAGEN
stream_lichess_data("lichess_db_standard_rated_2015-01.pgn.zst")
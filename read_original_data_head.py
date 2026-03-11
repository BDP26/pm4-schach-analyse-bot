import zstandard as zstd
import io
import pandas as pd

def show_lichess_head(file_path, num_games=20):
    print(f"Lese die ersten {num_games} Spiele aus {file_path}...\n")

    games = []
    current_game = {}

    with open(file_path, 'rb') as compressed_file:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(compressed_file) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')

            for line in text_stream:
                line = line.strip()

                # Leere Zeilen überspringen
                if not line:
                    continue

                # Wenn ein neues Spiel beginnt und wir schon eins gesammelt haben -> ab in die Liste!
                if line.startswith("[Event ") and current_game:
                    games.append(current_game)
                    current_game = {} # Reset für das nächste Spiel

                    # Stoppen, wenn wir 20 Spiele haben
                    if len(games) >= num_games:
                        break

                # Metadaten-Zeilen auslesen (z.B. [WhiteElo "1500"])
                if line.startswith("["):
                    # Wir trennen beim ersten Leerzeichen
                    parts = line.split(' ', 1)
                    if len(parts) == 2:
                        # Key ist das Wort nach der Klammer, z.B. "WhiteElo"
                        key = parts[0][1:]
                        # Value ist der Rest ohne die Anführungszeichen und die schließende Klammer
                        val = parts[1].strip('"]')

                        current_game[key] = val

                # Wenn es nicht mit '[' beginnt, sind es die tatsächlichen Spielzüge
                else:
                    # Manchmal gehen die Züge über mehrere Zeilen, wir hängen sie einfach an
                    if "Moves" not in current_game:
                        current_game["Moves"] = line
                    else:
                        current_game["Moves"] += " " + line

    # === DATEN ANZEIGEN ===
    # Wir machen aus unserer Liste von Dictionaries eine schöne Tabelle
    df = pd.DataFrame(games)

    # Pandas Einstellungen anpassen, damit nichts abgeschnitten wird
    pd.set_option('display.max_columns', None)  # Alle Spalten zeigen
    pd.set_option('display.width', 1000)        # Breite der Ausgabe maximieren
    pd.set_option('display.max_colwidth', 50)   # Züge nach 50 Zeichen abschneiden (für die Übersicht)

    print(df.head(num_games))

    print("\nAlle gefundenen Spaltennamen (Tags):")
    print(list(df.columns))

# HIER DEN NAMEN DEINER DATEI EINTRAGEN
show_lichess_head("lichess_db_standard_rated_2015-01.pgn.zst", num_games=20)
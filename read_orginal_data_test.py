import zstandard as zstd
import io
from collections import Counter

def scan_lichess_categories(file_path, sample_size=100000):
    print(f"Starte Schnell-Scan von {sample_size} Spielen in {file_path}...\n")

    # Wir nutzen "Counter" aus Python, das ist wie eine automatische Strichliste
    events = Counter()
    time_controls = Counter()
    terminations = Counter()

    games_counted = 0

    # Gleiches Streaming-Prinzip wie vorher
    with open(file_path, 'rb') as compressed_file:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_reader(compressed_file) as reader:
            text_stream = io.TextIOWrapper(reader, encoding='utf-8')

            for line in text_stream:
                # Wir extrahieren nur die Metadaten-Zeilen, die uns interessieren
                if line.startswith('[Event "'):
                    # Schneidet den Text zwischen den Anführungszeichen heraus
                    event_type = line.split('"')[1]
                    events[event_type] += 1

                    # Da jedes Spiel genau ein [Event ...] hat, zählen wir hier die Spiele
                    games_counted += 1

                    # Wenn wir unsere Stichprobengröße erreicht haben, brechen wir ab
                    if games_counted >= sample_size:
                        break

                elif line.startswith('[TimeControl "'):
                    tc = line.split('"')[1]
                    time_controls[tc] += 1

                elif line.startswith('[Termination "'):
                    term = line.split('"')[1]
                    terminations[term] += 1

    # === AUSWERTUNG AUSGEBEN ===
    print(f"--- ERGEBNISSE NACH {sample_size} SPIELEN ---")

    print("\n🏆 Top 10 Event-Kategorien (Spielarten):")
    # Zeigt die 10 häufigsten Einträge an
    for name, count in events.most_common(10):
        print(f"  - {name}: {count} mal")

    print("\n⏱️ Top 10 TimeControls (Bedenkzeit in Sekunden, z.B. 600+0 = 10 Min ohne Inkrement):")
    for tc, count in time_controls.most_common(10):
        print(f"  - {tc}: {count} mal")

    print("\n🏁 Arten der Beendigung (Termination):")
    for term, count in terminations.most_common():
        print(f"  - {term}: {count} mal")

# HIER WIEDER DEINEN DATEINAMEN EINTRAGEN
scan_lichess_categories("lichess_db_standard_rated_2015-01.pgn.zst")
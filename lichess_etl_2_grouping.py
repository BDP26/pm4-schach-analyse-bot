import duckdb
import os

print("Starte partitionierte DuckDB Aggregation...")

# Erstelle einen Ordner für deine finale Datenbank
output_dir = "lichess_moves_partitioned"
os.makedirs(output_dir, exist_ok=True)

con = duckdb.connect()

# Wir iterieren über alle möglichen Elo-Buckets
for elo in range(600, 4000, 100):
    print(f"-> Verarbeite Elo-Bucket {elo}...")

    query = f"""
        COPY (
            SELECT 
                BaseFEN, 
                PlayerEloBucket, 
                MoveSAN, 
                COUNT(*) as TimesPlayed
            FROM 'lichess_parquet_chunks/*.parquet'
            WHERE PlayerEloBucket = {elo}
            GROUP BY BaseFEN, PlayerEloBucket, MoveSAN
        ) TO '{output_dir}/aggregated_{elo}.parquet' (FORMAT PARQUET);
    """

    try:
        # Führt die Abfrage nur für diesen einen Elo-Wert aus
        con.execute(query)
        print(f"   ✅ Elo {elo} erfolgreich gespeichert!")
    except duckdb.IOException as e:
        # Falls es für ein bestimmtes Elo (z.B. 3000) absolut keine Spiele in deinen Chunks gibt
        print(f"   ℹ️ Keine Daten für Elo {elo} gefunden (übersprungen).")

print("\n🎉 Alle Partitionen erfolgreich erstellt! Dein Backend ist fertig.")
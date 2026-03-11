import pandas as pd

# Lese die Parquet-Datei
df = pd.read_parquet("test_chunk_001.parquet")

# Zeige die ersten 10 Zeilen an
print(df.head(10))
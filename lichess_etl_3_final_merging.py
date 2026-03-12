import duckdb

print("Stitching partitioned files together...")

# DuckDB simply reads all the pre-aggregated files and streams them directly
# into one final Parquet file without loading everything into RAM at once.
query = """
    COPY (
        SELECT *
        FROM 'final_database_partitioned/*.parquet'
    ) TO 'final_yourchessbot_database.parquet' (FORMAT PARQUET);
"""

try:
    duckdb.execute(query)
    print("✅ Merge complete! You now have a single 'final_yourchessbot_database.parquet' file ready for your dashboard.")
except Exception as e:
    print(f"❌ An error occurred: {e}")
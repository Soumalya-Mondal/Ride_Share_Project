import pyarrow.parquet as pq
import pandas as pd
from sqlalchemy import create_engine
from tqdm import tqdm

# ---------------- CONFIGURATION ---------------- #
PARQUET_FILE = "path/to/large_file.parquet"
DB_CONNECTION_STRING = "postgresql+psycopg2://user:password@localhost:5432/your_db"
TABLE_NAME = "your_table"
BATCH_SIZE = 10_000
# ------------------------------------------------ #

def load_parquet_in_batches():
    # Create DB connection
    engine = create_engine(DB_CONNECTION_STRING)
    conn = engine.connect()

    # Open Parquet file
    parquet_file = pq.ParquetFile(PARQUET_FILE)
    total_row_groups = parquet_file.num_row_groups

    print(f"Total row groups: {total_row_groups}")

    # Process each row group separately
    for rg_index in range(total_row_groups):
        print(f"\n▶ Processing row group {rg_index + 1}/{total_row_groups}")

        # Read current row group as Arrow Table (still not all rows in memory)
        row_group = parquet_file.read_row_group(rg_index)
        total_rows = row_group.num_rows

        # Convert to pandas DataFrame in smaller chunks
        start = 0
        with tqdm(total=total_rows, unit="rows") as pbar:
            while start < total_rows:
                end = min(start + BATCH_SIZE, total_rows)

                # Slice the Arrow Table efficiently
                batch_table = row_group.slice(start, end - start)

                # Convert to pandas
                df = batch_table.to_pandas()

                # Write to DB
                df.to_sql(
                    TABLE_NAME,
                    conn,
                    if_exists="append",
                    index=False,
                    method="multi",
                    chunksize=BATCH_SIZE
                )

                start = end
                pbar.update(len(df))

    conn.close()
    print("\n✅ Data load completed successfully!")

if __name__ == "__main__":
    load_parquet_in_batches()

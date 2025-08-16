#!/usr/bin/env python3.13
"""
Stream large Parquet files into PostGIS in chunks.
Automatically creates tables with schema inferred from Parquet,
reprojects to EPSG:5070, and creates a GIST spatial index.
"""

from pathlib import Path
import pyarrow.parquet as pq
import geopandas as gpd
from sqlalchemy import create_engine
import psycopg2

# ------------------------
# Configuration
# ------------------------
USER = "docker"
PASSWORD = "docker"
HOST = "localhost"
PORT = "25432"
DBNAME = "My Test PostGIS"
SCHEMA = "public"
EPSG_TARGET = 5070
CHUNK_SIZE = 25000  # rows per chunk

BASE_DIR = Path("/Volumes/Tooth/FloodProject/02_Overture")
PARQUET_DIR_BUILDINGS = BASE_DIR / "Buildings"

# ------------------------
# Database connection
# ------------------------
conn_str = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
engine = create_engine(conn_str)

# ------------------------
# Push a single Parquet in chunks
# ------------------------
def push_parquet_chunks(parquet_path: Path, table_name: str):
    if not parquet_path.exists():
        print(f"WARNING: Parquet file does not exist: {parquet_path}")
        return

    print(f"Streaming {table_name} from {parquet_path} in chunks of {CHUNK_SIZE} rows...")

    pq_file = pq.ParquetFile(parquet_path)
    first_chunk = True

    for i, batch in enumerate(pq_file.iter_batches(batch_size=CHUNK_SIZE)):
        df = batch.to_pandas()
        gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")  # adjust CRS if needed
        gdf = gdf.to_crs(EPSG_TARGET)

        gdf.to_postgis(
            name=table_name,
            con=engine,
            schema=SCHEMA,
            if_exists="replace" if first_chunk else "append",
            index=False
        )

        if first_chunk:
            # Create spatial index
            geom_col = gdf.geometry.name
            index_name = f"{table_name}_{geom_col}_gist"
            with engine.connect() as conn:
                conn.execute(f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{SCHEMA}"."{table_name}" USING GIST ("{geom_col}");')
            first_chunk = False

        print(f"Chunk {i+1} written.")

# ------------------------
# Main loop
# ------------------------
def main():
    parquet_files = list(PARQUET_DIR_BUILDINGS.glob("building_*.parquet"))
    if not parquet_files:
        print("No building Parquet files found.")
        return

    for parquet_path in parquet_files:
        table_name = parquet_path.stem
        push_parquet_chunks(parquet_path, table_name)

if __name__ == "__main__":
    main()

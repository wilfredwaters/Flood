#!/usr/bin/env python3.13
"""
Stream Overture Maps Parquet files into PostGIS in chunks.
Automatically creates tables with schema inferred from Parquet,
detects CRS, reprojects to EPSG:5070, and creates a GIST spatial index.
"""

from pathlib import Path
import geopandas as gpd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
from tqdm import tqdm

# ------------------------
# PostgreSQL configuration
# ------------------------
USER = "docker"
PASSWORD = "docker"
HOST = "localhost"
PORT = "25432"
DBNAME = "My Test PostGIS"
SCHEMA = "public"
BATCH_SIZE = 25000
EPSG_TARGET = 5070

conn_str = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
engine = create_engine(conn_str)

# ------------------------
# Directories and files
# ------------------------
BASE_DIR = Path("/Volumes/Tooth/FloodProject/02_Overture")
PARQUET_DIR_BUILDINGS = BASE_DIR / "Buildings"
PARQUET_DIR_POIS      = BASE_DIR / "POIs"

# ------------------------
# Regions and bounding boxes (informational)
# ------------------------
regions = {
    "Northeast": [-80.0, 32.0, -66.9, 49.4],
    "Midwest":   [-104.0, 40.0, -80.0, 49.4],
    "South":     [-104.0, 24.0, -80.0, 40.0],
    "West":      [-125.0, 29.0, -104.0, 49.4]
}

# ------------------------
# Utility function: stream parquet in batches
# ------------------------
def stream_parquet_to_postgis(parquet_path: Path, table_name: str, schema: str = SCHEMA, batch_size: int = BATCH_SIZE):
    if not parquet_path.exists():
        print(f"WARNING: Parquet file does not exist: {parquet_path}")
        return

    print(f"Streaming Parquet: {parquet_path}")
    pq_file = pq.ParquetFile(parquet_path)
    table_created = False
    detected_crs = None

    for batch in tqdm(pq_file.iter_batches(batch_size=batch_size), desc=f"Streaming {table_name}"):
        df = batch.to_pandas()
        if df.empty:
            continue
        # Ensure GeoDataFrame
        if "geometry" not in df.columns:
            print(f"ERROR: No 'geometry' column found in {parquet_path}")
            return

        gdf = gpd.GeoDataFrame(df, geometry='geometry')

        # Detect CRS if not already
        if detected_crs is None:
            try:
                detected_crs = gdf.crs
                if detected_crs is None:
                    # fallback
                    detected_crs = "EPSG:4326"
            except Exception:
                detected_crs = "EPSG:4326"
        gdf.set_crs(detected_crs, inplace=True, allow_override=True)

        # Reproject to target CRS
        gdf = gdf.to_crs(EPSG_TARGET)

        # Write to PostGIS
        gdf.to_postgis(
            name=table_name,
            con=engine,
            schema=schema,
            if_exists='replace' if not table_created else 'append',
            index=False
        )
        table_created = True

    # Create spatial index after all batches are inserted
    if table_created:
        geom_col = "geometry"
        index_name = f"{table_name}_{geom_col}_gist"
        with engine.connect() as conn:
            conn.execute(f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{schema}"."{table_name}" USING GIST ("{geom_col}");')
        print(f"Table {schema}.{table_name} created with spatial index.")

# ------------------------
# Main
# ------------------------
def main():

    # Buildings
    for region_name in regions.keys():
        parquet_file = PARQUET_DIR_BUILDINGS / f"building_{region_name.lower()}.parquet"
        table_name = f"building_{region_name.lower()}"
        # Uncomment the next line to process this building region
        stream_parquet_to_postgis(parquet_file, table_name)

if __name__ == "__main__":
    main()
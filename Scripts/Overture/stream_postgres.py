#!/usr/bin/env python3.12
"""
Stream Overture Maps Parquet files into PostGIS.
Automatically creates tables with schema inferred from Parquet,
reprojects to EPSG:5070, and creates a GIST spatial index.
"""

import geopandas as gpd
from sqlalchemy import create_engine
from pathlib import Path
from tqdm import tqdm
import psycopg2

# ------------------------
# PostgreSQL configuration
# ------------------------
USER = "docker"
PASSWORD = "docker"
HOST = "localhost"
PORT = "25432"
DBNAME = "My Test PostGIS"
SCHEMA = "public"

conn_str = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}:{PORT}/{DBNAME}"
engine = create_engine(conn_str)

# ------------------------
# Directories and files
# ------------------------
BASE_DIR = Path("/Volumes/Tooth/FloodProject/02_Overture")
PARQUET_DIR_BUILDINGS = BASE_DIR / "Buildings"
PARQUET_DIR_POIS      = BASE_DIR / "POIs"

# ------------------------
# Corrected CONUS bounding boxes (informational)
# ------------------------
regions = {
    "Northeast": [-80.0, 32.0, -66.9, 49.4],
    "Midwest":   [-104.0, 40.0, -80.0, 49.4],
    "South":     [-104.0, 24.0, -80.0, 40.0],
    "West":      [-125.0, 29.0, -104.0, 49.4]
}

# ------------------------
# Utility function
# ------------------------
def stream_parquet_to_postgis(parquet_path: Path, table_name: str, schema: str = SCHEMA, epsg: int = 5070):
    if not parquet_path.exists():
        print(f"WARNING: Parquet file does not exist: {parquet_path}")
        return

    print(f"Loading Parquet: {parquet_path}")
    gdf = gpd.read_parquet(parquet_path)
    print(f"Original CRS: {gdf.crs}")

    # Reproject
    gdf = gdf.to_crs(epsg=epsg)
    print(f"Reprojected CRS: {gdf.crs}")

    # Write to PostGIS
    print(f"Writing table {schema}.{table_name} ...")
    gdf.to_postgis(name=table_name, con=engine, schema=schema, if_exists="replace", index=False)

    # Create spatial index on geometry
    with engine.connect() as conn:
        geom_col = gdf.geometry.name
        index_name = f"{table_name}_{geom_col}_gist"
        conn.execute(f'CREATE INDEX IF NOT EXISTS "{index_name}" ON "{schema}"."{table_name}" USING GIST ("{geom_col}");')

    print(f"Table {schema}.{table_name} created with spatial index.")

# ------------------------
# Main loop
# ------------------------
def main():
    # Process only Midwest buildings
    region_name = "Midwest"
    parquet_file = PARQUET_DIR_BUILDINGS / f"building_{region_name.lower()}.parquet"
    table_name = f"building_{region_name.lower()}"
    stream_parquet_to_postgis(parquet_file, table_name)

if __name__ == "__main__":
    main()
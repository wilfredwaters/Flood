#!/usr/bin/env python3.12
"""
Convert Overture Maps Parquet files to Geopackage (GPKG) for PostGIS import.
"""

import geopandas as gpd
import os

# Paths
PARQUET_DIR_POIS = "/Volumes/Tooth/FloodProject/02_Overture/POIs"
PARQUET_DIR_BUILDINGS = "/Volumes/Tooth/FloodProject/02_Overture/Buildings"
GPKG_DIR = "/Volumes/Tooth/FloodProject/02_Overture/GPKG"

os.makedirs(GPKG_DIR, exist_ok=True)

# Helper function to convert Parquet to GPKG
def convert_parquet_to_gpkg(parquet_path, output_dir):
    gdf = gpd.read_parquet(parquet_path)
    base_name = os.path.basename(parquet_path).replace(".parquet", ".gpkg")
    output_file = os.path.join(output_dir, base_name)
    gdf.to_file(output_file, driver="GPKG")
    print(f"Converted {parquet_path} → {output_file}")

# Process POIs
for f in os.listdir(PARQUET_DIR_POIS):
    if f.endswith(".parquet"):
        convert_parquet_to_gpkg(os.path.join(PARQUET_DIR_POIS, f), GPKG_DIR)

# Process Buildings (optional, if uncommented)
# for f in os.listdir(PARQUET_DIR_BUILDINGS):
#     if f.endswith(".parquet"):
#         convert_parquet_to_gpkg(os.path.join(PARQUET_DIR_BUILDINGS, f), GPKG_DIR)

print("All Parquet files converted to GPKG.")

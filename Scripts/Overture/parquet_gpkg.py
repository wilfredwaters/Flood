#!/usr/bin/env python3.12
"""
Convert Overture Maps Parquet files to GeoPackage, skipping macOS metadata files.
"""

import os
import geopandas as gpd

# Directories
PARQUET_DIR_BUILDINGS = "/Volumes/Tooth/FloodProject/02_Overture/Buildings"
PARQUET_DIR_POIS      = "/Volumes/Tooth/FloodProject/02_Overture/POIs"
GPKG_DIR              = "/Volumes/Tooth/FloodProject/02_Overture/GPKG"

os.makedirs(GPKG_DIR, exist_ok=True)

def convert_parquet_to_gpkg(parquet_path, output_dir):
    try:
        gdf = gpd.read_parquet(parquet_path)
        filename = os.path.splitext(os.path.basename(parquet_path))[0] + ".gpkg"
        output_path = os.path.join(output_dir, filename)
        gdf.to_file(output_path, driver="GPKG")
        print(f"Converted {parquet_path} → {output_path}")
    except Exception as e:
        print(f"Error converting {parquet_path}: {e}")

# Convert all Parquet files in a directory, skipping macOS metadata files
def convert_directory(parquet_dir, output_dir):
    for f in os.listdir(parquet_dir):
        if f.startswith("._") or f.startswith(".DS_Store"):
            continue  # skip macOS metadata files
        full_path = os.path.join(parquet_dir, f)
        if os.path.isfile(full_path) and full_path.endswith(".parquet"):
            convert_parquet_to_gpkg(full_path, output_dir)

# Uncomment if you want to convert buildings
# convert_directory(PARQUET_DIR_BUILDINGS, GPKG_DIR)

# Convert POIs
convert_directory(PARQUET_DIR_POIS, GPKG_DIR)

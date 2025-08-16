#!/usr/bin/env python3.12
"""
Convert Overture Maps Parquet files to GeoPackage with EPSG:5070,
skipping macOS metadata files, with a progress bar.
"""

import os
import geopandas as gpd
from pathlib import Path
from tqdm import tqdm

# Directories
PARQUET_DIR_BUILDINGS = "/Volumes/Tooth/FloodProject/02_Overture/Buildings"
PARQUET_DIR_POIS      = "/Volumes/Tooth/FloodProject/02_Overture/POIs"
GPKG_DIR              = "/Volumes/Tooth/FloodProject/02_Overture/GPKG"

os.makedirs(GPKG_DIR, exist_ok=True)

def convert_parquet_to_gpkg(parquet_path, output_dir, epsg=5070):
    """Convert a single Parquet file to GPKG with EPSG:5070"""
    try:
        gdf = gpd.read_parquet(parquet_path)

        # Reproject to EPSG:5070
        gdf = gdf.to_crs(epsg=epsg)

        filename = Path(parquet_path).stem + ".gpkg"
        output_path = Path(output_dir) / filename
        gdf.to_file(output_path, driver="GPKG")
        print(f"Converted {parquet_path} → {output_path} (EPSG:{epsg})")
    except Exception as e:
        print(f"Error converting {parquet_path}: {e}")

def convert_directory(parquet_dir, output_dir, epsg=5070):
    """Convert all Parquet files in a directory with progress bar"""
    parquet_files = [
        os.path.join(parquet_dir, f)
        for f in os.listdir(parquet_dir)
        if os.path.isfile(os.path.join(parquet_dir, f))
        and f.endswith(".parquet")
        and not f.startswith("._")
        and not f.startswith(".DS_Store")
    ]

    for parquet_path in tqdm(parquet_files, desc=f"Converting {Path(parquet_dir).name}", unit="file"):
        convert_parquet_to_gpkg(parquet_path, output_dir, epsg=epsg)

# Uncomment if you want to convert buildings
# convert_directory(PARQUET_DIR_BUILDINGS, GPKG_DIR, epsg=5070)

# Convert POIs
convert_directory(PARQUET_DIR_POIS, GPKG_DIR, epsg=5070)

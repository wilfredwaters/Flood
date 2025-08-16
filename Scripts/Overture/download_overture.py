#!/usr/bin/env python3.12
"""
Download Overture Maps building footprints and POIs for CONUS regions,
and convert them to GPKG with EPSG:5070.
"""

import os
import subprocess
from pathlib import Path
import geopandas as gpd
from tqdm import tqdm

# Paths
POIS_DIR = "/Volumes/Tooth/FloodProject/02_Overture/POIs"
BUILDINGS_DIR = "/Volumes/Tooth/FloodProject/02_Overture/Buildings"
GPKG_DIR = "/Volumes/Tooth/FloodProject/02_Overture/GPKG"
os.makedirs(POIS_DIR, exist_ok=True)
os.makedirs(BUILDINGS_DIR, exist_ok=True)
os.makedirs(GPKG_DIR, exist_ok=True)

# Corrected CONUS regions bounding boxes
regions = {
    "Northeast": [-80.0, 32.0, -66.9, 49.4],
    "Midwest":   [-104.0, 40.0, -80.0, 49.4],
    "South":     [-104.0, 24.0, -80.0, 40.0],
    "West":      [-125.0, 29.0, -104.0, 49.4]
}

def download_overture(dataset_type, region_name, bbox, output_file):
    """Download using overturemaps CLI and stream output"""
    print(f"Downloading {dataset_type} for {region_name} → {output_file}")
    cmd = [
        "overturemaps", "download",
        "--type", dataset_type,
        "--bbox", ",".join(map(str, bbox)),
        "-f", "geoparquet",
        "-o", output_file
    ]
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    for line in process.stdout:
        print(line, end="")
    process.wait()
    if process.returncode != 0:
        raise RuntimeError(f"Download failed for {region_name} ({dataset_type})")
    print(f"Completed {dataset_type} for {region_name}")

def convert_parquet_to_gpkg(parquet_path, gpkg_dir, epsg=5070):
    """Convert a Parquet file to GPKG with tqdm progress bar"""
    parquet_file = Path(parquet_path)
    if not parquet_file.exists():
        print(f"WARNING: Parquet file does not exist: {parquet_file}")
        return

    gdf = gpd.read_parquet(parquet_file)
    gdf = gdf.to_crs(epsg=epsg)
    output_file = Path(gpkg_dir) / (parquet_file.stem + ".gpkg")

    for _ in tqdm(range(1), desc=f"Writing {output_file.name}", unit="file"):
        gdf.to_file(output_file, driver="GPKG")
    print(f"Converted {parquet_file} → {output_file}")

def main():
    for region_name, bbox in regions.items():
        # POIs (commented out since we only want buildings in Midwest)
        # poi_file = Path(POIS_DIR) / f"place_{region_name.lower()}.parquet"
        # download_overture("place", region_name, bbox, str(poi_file))
        # convert_parquet_to_gpkg(str(poi_file), GPKG_DIR, epsg=5070)

        # Buildings (only Midwest uncommented)
        if region_name == "Midwest":
            building_file = Path(BUILDINGS_DIR) / f"building_{region_name.lower()}.parquet"
            download_overture("building", region_name, bbox, str(building_file))
            convert_parquet_to_gpkg(str(building_file), GPKG_DIR, epsg=5070)

        # Other buildings (commented out)
        # building_file = Path(BUILDINGS_DIR) / f"building_{region_name.lower()}.parquet"
        # download_overture("building", region_name, bbox, str(building_file))
        # convert_parquet_to_gpkg(str(building_file), GPKG_DIR, epsg=5070)

if __name__ == "__main__":
    main()

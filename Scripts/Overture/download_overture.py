#!/usr/bin/env python3.12
"""
Download Overture Maps building footprints and POIs for CONUS regions,
saving each region separately as Geoparquet files on the thumbdrive.
"""

import overturemaps as om
import os

# Thumbdrive paths
#BUILDINGS_DIR = "/Volumes/Tooth/FloodProject/02_Overture/Buildings"
POIS_DIR = "/Volumes/Tooth/FloodProject/02_Overture/POIs"

# Ensure output directories exist
#os.makedirs(BUILDINGS_DIR, exist_ok=True)
os.makedirs(POIS_DIR, exist_ok=True)

# CONUS regions
regions = {
    "Northeast": (-80.0, 37.0, -66.9, 49.4),
    "Midwest":   (-104.0, 36.5, -80.0, 49.4),
    "South":     (-104.0, 24.0, -75.0, 37.0),
    "West":      (-125.0, 32.0, -104.0, 49.4)
}

# Function to download a dataset for a region
def download_region(dataset: str, region_name: str, bbox: tuple, output_dir: str):
    output_file = os.path.join(output_dir, f"{dataset}_{region_name.lower()}.parquet")
    print(f"Downloading {dataset} for {region_name} → {output_file} ...")
    om.fetch(
        dataset=dataset,
        bbox=bbox,
        format="geoparquet",
        output_path=output_file
    )
    print(f"Completed {dataset} for {region_name}")

# Main download loop
for region_name, bbox in regions.items():
 #   download_region("building", region_name, bbox, BUILDINGS_DIR)
    download_region("place", region_name, bbox, POIS_DIR)

print("All downloads completed!")

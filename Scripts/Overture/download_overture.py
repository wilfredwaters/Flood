import overturemaps as om
import os
from concurrent.futures import ThreadPoolExecutor

# Output base folder
output_base = "/Volumes/Tooth/FloodProject/02_Overture"

# CONUS regions
regions = {
    "Northeast": (-80.0, 37.0, -66.9, 49.4),
    "Midwest": (-104.0, 36.5, -80.0, 49.4),
    "South": (-104.0, 24.0, -75.0, 37.0),
    "West": (-125.0, 32.0, -104.0, 49.4)
}

# Ensure folders exist
os.makedirs(f"{output_base}/Buildings", exist_ok=True)
os.makedirs(f"{output_base}/POIs", exist_ok=True)

def download_region(region_name, bbox):
    # Download buildings
  '''
    print(f"Downloading buildings for {region_name}...")
    om.download(
        data_type="building",
        bbox=bbox,
        fmt="geoparquet",
        output=f"{output_base}/Buildings/buildings_{region_name}.parquet"
    )
   ''' 
    # Download POIs
    print(f"Downloading POIs for {region_name}...")
    om.download(
        data_type="place",
        bbox=bbox,
        fmt="geoparquet",
        output=f"{output_base}/POIs/pois_{region_name}.parquet"
    )
    print(f"{region_name} download complete!")

# Use ThreadPoolExecutor to download all regions in parallel
with ThreadPoolExecutor(max_workers=4) as executor:
    futures = [executor.submit(download_region, name, bbox) for name, bbox in regions.items()]
    
    # Wait for all to finish
    for future in futures:
        future.result()

print("All regions downloaded!")

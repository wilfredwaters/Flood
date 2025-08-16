import os
import zipfile
import tempfile
from pathlib import Path
import geopandas as gpd
import pyogrio
from tqdm import tqdm
import concurrent.futures
import logging
import datetime

input_dir = Path("/Users/wilfredwaters/Documents/FloodData")
output_base_dir = Path("/Users/wilfredwaters/Documents/FloodData_Parquet")

VALID_FLD_ZONES = {"A", "AE", "AH", "AO", "AR", "A99", "V", "VE"}

checkpoint_file = Path("processed_gpkgs.txt")

# Setup logging
logging.basicConfig(
    filename='conversion.log',
    filemode='a',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger()

def load_processed_gpkgs():
    """Load list of already processed GPKG files from checkpoint."""
    if checkpoint_file.exists():
        return set(line.strip() for line in checkpoint_file.read_text().splitlines())
    return set()

def save_processed_gpkg(gpkg_path):
    """Save processed GPKG file path to checkpoint."""
    with open(checkpoint_file, "a") as f:
        f.write(gpkg_path + "\n")

def extract_nested_zips(zip_path, temp_dir):
    """Recursively extract nested zips and return list of GPKG file paths."""
    gpkg_files = []
    files_to_process = [(zip_path, temp_dir)]

    while files_to_process:
        current_zip_path, current_extract_dir = files_to_process.pop(0)
        try:
            with zipfile.ZipFile(current_zip_path, 'r') as current_zip:
                current_zip.extractall(current_extract_dir)
        except zipfile.BadZipFile:
            logger.warning(f"Skipping corrupted zip file: {current_zip_path}")
            continue

        for root, _, files in os.walk(current_extract_dir):
            # Filter out __MACOSX directories and files
            if os.path.basename(root) == '__MACOSX':
                continue
            files = [f for f in files if not f.startswith('._') and '__MACOSX' not in f]

            for file in files:
                file_path = os.path.join(root, file)
                if file.endswith(".zip"):
                    # Create a new temporary directory for nested zip to avoid conflicts
                    new_temp_dir = os.path.join(root, Path(file).stem + "_extracted")
                    os.makedirs(new_temp_dir, exist_ok=True)
                    files_to_process.append((file_path, new_temp_dir))
                elif file.endswith(".gpkg"):
                    gpkg_files.append(file_path)
    return gpkg_files

def get_fips_from_filename(gpkg_path):
    """Extract the 2-digit state FIPS code from filename like _NFHL_48_20250717_S_FLD_HAZ_AR-002.gpkg"""
    stem = Path(gpkg_path).stem
    parts = stem.split("_")
    # Attempt to find FIPS code after "NFHL" or similar patterns
    for i, part in enumerate(parts):
        if part.upper() == "NFHL" and i + 1 < len(parts):
            fips_candidate = parts[i + 1]
            if fips_candidate.isdigit() and len(fips_candidate) == 2:
                return fips_candidate
            # Handle cases like '48-extra_text'
            elif fips_candidate.startswith("48") and len(fips_candidate) > 2 and fips_candidate[2] == '-':
                return fips_candidate[:2]
    # Fallback for patterns like 'nfhl_19_...'
    if len(parts) > 1 and parts[0].lower() == "nfhl" and parts[1].isdigit() and len(parts[1]) == 2:
        return parts[1]
    return None

def convert_gpkg_to_geoparquet(gpkg_path, output_base_dir):
    """Convert GPKG to GeoParquet format using geopandas."""
    processed_gpkgs = load_processed_gpkgs()
    if gpkg_path in processed_gpkgs:
        logger.info(f"Skipping already processed GPKG: {gpkg_path}")
        return

    try:
        fips_code = get_fips_from_filename(gpkg_path)
        if not fips_code:
            msg = f"Could not detect FIPS code from filename: {gpkg_path}"
            logger.warning(msg)
            return

        # List all layers in the GPKG file
        layers = pyogrio.list_layers(gpkg_path)
        
        for layer_name, _ in layers:
            try:
                # Read the layer using geopandas (which handles geometry properly)
                gdf = gpd.read_file(gpkg_path, layer=layer_name)
                
                # Skip layers without FLD_ZONE column
                if "FLD_ZONE" not in gdf.columns:
                    logger.info(f"Skipping layer {layer_name} - no FLD_ZONE column")
                    continue
                
                # Filter for valid flood zones
                gdf = gdf[gdf["FLD_ZONE"].isin(VALID_FLD_ZONES)]
                
                if gdf.empty:
                    logger.info(f"Skipping layer {layer_name} - no valid flood zones")
                    continue

                # Ensure output CRS is WGS84 (EPSG:4326)
                if gdf.crs is None:
                    logger.warning(f"CRS not defined for {gpkg_path} layer {layer_name}. Assuming EPSG:4326.")
                    gdf.set_crs("EPSG:4326", allow_override=True, inplace=True)
                elif gdf.crs != "EPSG:4326":
                    gdf = gdf.to_crs("EPSG:4326")

                # Create partition directory
                partition_dir = output_base_dir / f"STATEFP={fips_code}"
                partition_dir.mkdir(parents=True, exist_ok=True)

                # Define output file path
                out_file = partition_dir / f"{Path(gpkg_path).stem}__{layer_name}.parquet"
                
                # Write to GeoParquet using geopandas
                # This preserves the geometry column in the proper GeoParquet format
                # Note: QGIS 3.44 (and older) may require GDAL 3.5+ for full GeoParquet compatibility.
                # If QGIS does not recognize the file, consider updating QGIS or its GDAL libraries.
                gdf.to_parquet(
                    out_file,
                    compression='snappy',  # Good compression for geospatial data
                    index=False  # Don\'t include pandas index
                )

                logger.info(f"Successfully converted: {out_file} ({len(gdf)} features)")
                
            except Exception as layer_error:
                logger.error(f"Error processing layer {layer_name} in {gpkg_path}: {layer_error}")
                continue
        save_processed_gpkg(gpkg_path)
                
    except Exception as e:
        logger.error(f"Error converting {gpkg_path}: {e}")

def process_single_zip(zip_file):
    """Process a single zip file."""
    try:
        with tempfile.TemporaryDirectory() as tmpdir:
            gpkg_files = extract_nested_zips(zip_file, tmpdir)
            
            if not gpkg_files:
                logger.warning(f"No GPKG files found in {zip_file.name}")
                return
                
            for gpkg in gpkg_files:
                convert_gpkg_to_geoparquet(gpkg, output_base_dir)

        logger.info(f"Finished processing zip: {zip_file.name}")
        
    except Exception as e:
        logger.error(f"Error processing zip {zip_file.name}: {e}")

def main():
    """Main function to process all zip files."""
    # Add a timestamp to differentiate between runs
    logger.info("\n" + "="*50)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(f"Starting new conversion run at {timestamp}")
    logger.info("="*50 + "\n")
    # Ensure output directory exists
    output_base_dir.mkdir(parents=True, exist_ok=True)
    
    # Get all zip files
    all_zip_files = list(input_dir.glob("*.zip"))
    
    if not all_zip_files:
        print(f"No zip files found in {input_dir}")
        return
    
    print(f"Found {len(all_zip_files)} zip files to process")
    
    # Process files with thread pool
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        list(tqdm(
            executor.map(process_single_zip, all_zip_files), 
            total=len(all_zip_files), 
            desc="Processing zip files"
        ))

    print("All done. Check conversion.log for details.")

if __name__ == "__main__":
    main()

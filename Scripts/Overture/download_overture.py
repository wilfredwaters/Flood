#!/usr/bin/env python3.12
"""
Stream Overture Maps Buildings (and optionally POIs) from S3 into PostGIS.
Supports:
- Full ETL
- Single row-group test toggle
No local Parquet files.
"""

import time
import json
import pyarrow.parquet as pq
import geopandas as gpd
from shapely import wkb
from sqlalchemy import create_engine, text
from multiprocessing import Pool
from tqdm import tqdm
from functools import partial
import s3fs
from concurrent.futures import ThreadPoolExecutor

# ----------------------- CONFIGURATION -----------------------
POSTGIS_URL = "postgresql+psycopg2://docker:docker@localhost:25432/gis"
MAIN_TABLE_BUILDINGS = "building_usa"
LOG_PATH = "etl_progress_log.json"

NUM_WORKERS = 4            # Global row group pool
BATCH_WORKERS = 4          # Batch-level parallelism per row group
BATCH_SIZE = 100_000
TARGET_CRS = "EPSG:5070"
RETRIES = 3
RETRY_DELAY = 5  # seconds
USA_BBOX = [-125.0, 24.0, -66.9, 49.4]

# S3 Parquet URL patterns
BUILDINGS_S3_PATTERN = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=buildings/type=building/*"
# POIS_S3_PATTERN = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=places/type=place/*"  # Uncomment if needed

# Toggle for testing: only process the first row group of the first file
TEST_FIRST_ROW_GROUP_ONLY = False

# ----------------------- POSTGIS ENGINE -----------------------
engine = create_engine(POSTGIS_URL)

# ----------------------- LOG UTILITIES -----------------------
def load_log():
    try:
        with open(LOG_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def update_log(log):
    with open(LOG_PATH, "w") as f:
        json.dump(log, f, indent=2)

def row_group_already_done(log, parquet_path, row_group_idx):
    return log.get(parquet_path, {}).get(str(row_group_idx), False)

# ----------------------- INDEX CREATION -----------------------
def create_indexes(main_table):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {main_table}_geom_idx ON {main_table} USING GIST (geometry);"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {main_table}_subtype_idx ON {main_table} (subtype);"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {main_table}_class_idx ON {main_table} (class);"))
    print(f"Indexes created on {main_table}.")

# ----------------------- BATCH PROCESSING -----------------------
def process_batch(batch, staging_table):
    df = batch.to_pandas()
    if df.empty:
        return
    df['geometry'] = df['geometry'].apply(wkb.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
    minx, miny, maxx, maxy = USA_BBOX
    gdf = gdf.cx[minx:maxx, miny:maxy]
    if gdf.empty:
        return
    gdf = gdf.to_crs(TARGET_CRS)
    gdf.to_postgis(staging_table, engine, if_exists='append', index=False)

def process_row_group_batches(parquet_path, row_group_idx, staging_table):
    """Process all batches in a row group using threads to avoid daemonic issues."""
    filesystem, path = fs.FileSystem.from_uri(parquet_path)
    pf = pq.ParquetFile(path, filesystem=filesystem)
    batches = list(pf.iter_batches(batch_size=BATCH_SIZE, row_groups=[row_group_idx]))
    with ThreadPoolExecutor(max_workers=BATCH_WORKERS) as executor:
        executor.map(lambda batch: process_batch(batch, staging_table), batches)


# ----------------------- ROW GROUP PROCESSING -----------------------
def process_row_group_task(task):
    parquet_url, row_group_idx, main_table = task
    staging_table = f"{main_table}_staging_{row_group_idx}"
    attempts = 0
    while attempts < RETRIES:
        try:
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table};"))
            process_row_group_batches(parquet_url, row_group_idx, staging_table)
            with engine.begin() as conn:
                conn.execute(text(f"INSERT INTO {main_table} SELECT * FROM {staging_table};"))
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table};"))
            return (parquet_url, row_group_idx, True)
        except Exception as e:
            print(f"Error processing row group {row_group_idx} from {parquet_url}: {e}")
            attempts += 1
            time.sleep(RETRY_DELAY)
    return (parquet_url, row_group_idx, False)

# ----------------------- MAIN STREAMING FUNCTION -----------------------
def collect_row_group_tasks(parquet_patterns, log):
    s3 = s3fs.S3FileSystem(anon=True)
    tasks = []
    for pattern, main_table in parquet_patterns:
        files = s3.glob(pattern)
        if not files:
            print(f"No files found for pattern {pattern}")
            continue
        for file in files:
            parquet_url = f"s3://{file}" if not file.startswith("s3://") else file
            pf = pq.ParquetFile(parquet_url, filesystem=s3)
            for rg_idx in range(pf.num_row_groups):
                if not row_group_already_done(log, parquet_url, rg_idx):
                    tasks.append((parquet_url, rg_idx, main_table))
                    if TEST_FIRST_ROW_GROUP_ONLY:
                        return tasks  # only first row group
    return tasks

def main():
    overall_start = time.time()
    log = load_log()

    jobs = [
        (BUILDINGS_S3_PATTERN, MAIN_TABLE_BUILDINGS),
        # (POIS_S3_PATTERN, MAIN_TABLE_POIS),  # Uncomment if needed
    ]

    tasks = collect_row_group_tasks(jobs, log)
    print(f"Total row-group tasks to process: {len(tasks)}")

    if not tasks:
        print("No tasks found. Exiting.")
        return

    with Pool(NUM_WORKERS) as pool:
        for parquet_url, rg_idx, success in tqdm(pool.imap_unordered(process_row_group_task, tasks),
                                                 total=len(tasks),
                                                 desc="All RowGroups"):
            log.setdefault(parquet_url, {})[str(rg_idx)] = success
            update_log(log)
            if not success:
                print(f"Row group {rg_idx} from {parquet_url} failed after retries.")

    # Create indexes
    for _, main_table in jobs:
        create_indexes(main_table)

    overall_end = time.time()
    print(f"Total ETL runtime: {overall_end - overall_start:.1f} sec")

if __name__ == "__main__":
    main()

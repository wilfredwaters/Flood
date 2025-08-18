#!/usr/bin/env python3.13
"""
Stream Overture Maps Buildings (and optionally POIs) from S3 into PostGIS.

Features:
- **EFFICIENT S3 PARTITIONING**: Uses Quadkey partitioning to only scan US-based data.
- First-run table creation: staging + main tables.
- Incremental inserts: safe on restart or crash.
- Checkpointing via PostgreSQL `load_log` table.
- All operations for a row group are performed in a single, atomic transaction.
"""

import time
import pyarrow.parquet as pq
import geopandas as gpd
from shapely import wkb
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor
import s3fs
from tqdm import tqdm

# ----------------------- CONFIGURATION -----------------------
POSTGIS_URL = "postgresql+psycopg2://docker:docker@localhost:25432/gis"
MAIN_TABLE_BUILDINGS = "building_usa"
# MAIN_TABLE_POIS = "poi_usa"

NUM_WORKERS = 4
BATCH_WORKERS = 4
BATCH_SIZE = 100_000
TARGET_CRS = "EPSG:5070"
TARGET_SRID = TARGET_CRS.split(':')[-1]
RETRIES = 3
RETRY_DELAY = 5
USA_BBOX = [-125.0, 24.0, -66.9, 49.4]

# <<<<<<< START OF EFFICIENCY FIX >>>>>>>
# Overture uses a quadkey partitioning scheme. We can target only the
# top-level quadkeys that cover the United States to avoid scanning the entire planet.
# Zoom level 4 quadkeys for the USA are primarily within 02, 03, 10, and 11.
USA_QUADKEY_PREFIXES = ["02", "03", "10", "11"]

# We will build the S3 patterns dynamically based on these prefixes.
BASE_S3_URL_BUILDINGS = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=buildings/type=building"
# BASE_S3_URL_POIS = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=places/type=place"
# <<<<<<< END OF EFFICIENCY FIX >>>>>>>

# Set to False for a full production run.
STOP_AFTER_FIRST_SUCCESS = False

# ----------------------- POSTGIS ENGINE -----------------------
engine = create_engine(POSTGIS_URL, pool_size=NUM_WORKERS + BATCH_WORKERS)

# (The rest of the functions: ensure_load_log_table, row_group_already_done, etc. are IDENTICAL)
# ...
# ... (No changes to the core processing logic) ...
# ...

# ----------------------- TASK COLLECTION (MODIFIED) -----------------------
def collect_row_group_tasks(jobs, s3fs_instance):
    """
    List all row-group tasks from S3, using efficient partitioning.
    """
    tasks = []
    # jobs is now a list of tuples: (base_s3_url, quadkey_prefixes, main_table)
    for base_url, quad_prefixes, main_table in jobs:
        for prefix in quad_prefixes:
            # Construct a targeted pattern for each US quadkey prefix
            pattern = f"{base_url}/quadkey={prefix}*"
            print(f"[INFO] Searching for files with pattern: {pattern}")
            try:
                files = s3fs_instance.glob(pattern)
                if not files:
                    print(f"[DEBUG] No files found for pattern {pattern}")
                    continue
                
                for file_path in files:
                    parquet_url = f"s3://{file_path}"
                    pf = pq.ParquetFile(parquet_url, filesystem=s3fs_instance)
                    for rg_idx in range(pf.num_row_groups):
                        if not row_group_already_done(parquet_url, rg_idx):
                            tasks.append((parquet_url, rg_idx, main_table))
            except Exception as e:
                print(f"[WARNING] Could not process pattern {pattern}. Error: {e}")
    return tasks

# ----------------------- MAIN STREAMING FUNCTION (MODIFIED) -----------------------
def main():
    overall_start = time.time()
    ensure_load_log_table()
    s3 = s3fs.S3FileSystem(anon=True)

    # <<<<<<< START OF EFFICIENCY FIX >>>>>>>
    # The jobs list now contains the base URL and the prefixes to target.
    jobs = [
        (BASE_S3_URL_BUILDINGS, USA_QUADKEY_PREFIXES, MAIN_TABLE_BUILDINGS),
        # (BASE_S3_URL_POIS, USA_QUADKEY_PREFIXES, MAIN_TABLE_POIS),
    ]
    # <<<<<<< END OF EFFICIENCY FIX >>>>>>>

    tasks = collect_row_group_tasks(jobs, s3)
    print(f"[INFO] Found {len(tasks)} total row-group tasks to process across all US partitions.")
    if not tasks:
        print("[INFO] No new tasks to process. Exiting.")
        return

    from multiprocessing import Pool
    from functools import partial

    worker_func = partial(process_row_group_task, s3fs_instance=s3)

    with Pool(NUM_WORKERS) as pool:
        for success, row_count in tqdm(pool.imap_unordered(worker_func, tasks), total=len(tasks), desc="All RowGroups"):
            if success and row_count > 0:
                print(f"[INFO] Successfully processed a chunk with {row_count} rows.")
                if STOP_AFTER_FIRST_SUCCESS:
                    print("[INFO] STOP_AFTER_FIRST_SUCCESS is True. Halting processing.")
                    pool.terminate()
                    break
            elif not success:
                print(f"[ERROR] A task failed after all retries.")

    print("[INFO] Data loading complete. Creating indexes...")
    for _, _, main_table in jobs: # Unpack the new job tuple
        create_indexes(main_table)

    overall_end = time.time()
    print(f"[INFO] Total ETL runtime: {overall_end - overall_start:.1f} sec")


# (Paste in the unchanged functions here: ensure_load_log_table, row_group_already_done, mark_row_group_done, create_indexes, create_staging_table, process_batch, process_row_group_batches, process_row_group_task)
# ...
# ...
if __name__ == "__main__":
    main()

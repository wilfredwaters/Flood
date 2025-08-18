#!/usr/bin/env python3.12
"""
Stream Overture Maps Buildings (and optionally POIs) from S3 into PostGIS.

Features:
- First-run table creation: staging + main tables.
- Main table schema inherits from first staging table.
- Incremental inserts: safe on restart or crash.
- Checkpointing via a PostgreSQL `load_log` table (no JSON log).
- Row-group processing with threads to avoid daemonic issues.
- Works for both buildings and POIs (POI lines commented out).
- Indexes/constraints added after full load for efficiency.
"""

import time
import pyarrow.parquet as pq
import geopandas as gpd
from shapely import wkb
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor
import s3fs
import pyarrow.fs as fs

# ----------------------- CONFIGURATION -----------------------
POSTGIS_URL = "postgresql+psycopg2://docker:docker@localhost:25432/gis"
MAIN_TABLE_BUILDINGS = "building_usa"
# MAIN_TABLE_POIS = "poi_usa"  # Uncomment if processing POIs

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

TEST_FIRST_ROW_GROUP_ONLY = True

# ----------------------- POSTGIS ENGINE -----------------------
engine = create_engine(POSTGIS_URL)

# ----------------------- CHECKPOINTING -----------------------
def ensure_load_log_table():
    """Create the load_log table if it doesn't exist."""
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS load_log (
            parquet_url TEXT NOT NULL,
            row_group_idx INT NOT NULL,
            processed_at TIMESTAMP DEFAULT now(),
            PRIMARY KEY(parquet_url, row_group_idx)
        );
        """))

def row_group_already_done(parquet_url, row_group_idx):
    """Check if the row group has been processed already."""
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM load_log WHERE parquet_url = :url AND row_group_idx = :rg"),
            {"url": parquet_url, "rg": row_group_idx}
        ).first()
        return result is not None

def mark_row_group_done(parquet_url, row_group_idx):
    """Mark the row group as processed in load_log."""
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO load_log(parquet_url, row_group_idx) VALUES(:url, :rg) ON CONFLICT DO NOTHING;"),
            {"url": parquet_url, "rg": row_group_idx}
        )

# ----------------------- INDEX CREATION -----------------------
def create_indexes(main_table):
    """Create indexes for faster spatial and attribute queries."""
    with engine.begin() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {main_table}_geom_idx ON {main_table} USING GIST (geometry);"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {main_table}_subtype_idx ON {main_table} (subtype);"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {main_table}_class_idx ON {main_table} (class);"))
    print(f"Indexes created on {main_table}.")

# ----------------------- BATCH PROCESSING -----------------------
def process_batch(batch, staging_table):
    """Process a single Arrow batch into PostGIS staging table."""
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
    """Process a single row group: create staging, insert into main, checkpoint."""
    parquet_url, row_group_idx, main_table = task
    staging_table = f"{main_table}_staging_{row_group_idx}"

    if row_group_already_done(parquet_url, row_group_idx):
        return (parquet_url, row_group_idx, True)

    attempts = 0
    while attempts < RETRIES:
        try:
            # Drop staging table if exists
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table};"))

            # Create staging table from Parquet
            process_row_group_batches(parquet_url, row_group_idx, staging_table)

            # Create main table from first staging table if it does not exist
            with engine.begin() as conn:
                if not conn.execute(
                    text(f"SELECT to_regclass('{main_table}');")
                ).scalar():
                    conn.execute(text(f"CREATE TABLE {main_table} AS TABLE {staging_table} WITH NO DATA;"))

            # Insert into main table
            with engine.begin() as conn:
                conn.execute(text(f"INSERT INTO {main_table} SELECT * FROM {staging_table};"))

            mark_row_group_done(parquet_url, row_group_idx)
            return (parquet_url, row_group_idx, True)
        except Exception as e:
            print(f"Error processing row group {row_group_idx} from {parquet_url}: {e}")
            attempts += 1
            time.sleep(RETRY_DELAY)

    return (parquet_url, row_group_idx, False)

# ----------------------- TASK COLLECTION -----------------------
def collect_row_group_tasks(parquet_patterns):
    """List all row-group tasks from S3, skipping already-processed ones."""
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
                if not row_group_already_done(parquet_url, rg_idx):
                    tasks.append((parquet_url, rg_idx, main_table))
                    if TEST_FIRST_ROW_GROUP_ONLY:
                        return tasks  # only first row group for testing
    return tasks

# ----------------------- MAIN STREAMING FUNCTION -----------------------
def main():
    overall_start = time.time()
    ensure_load_log_table()

    jobs = [
        (BUILDINGS_S3_PATTERN, MAIN_TABLE_BUILDINGS),
        # (POIS_S3_PATTERN, MAIN_TABLE_POIS),  # Uncomment if needed
    ]

    tasks = collect_row_group_tasks(jobs)
    print(f"Total row-group tasks to process: {len(tasks)}")

    if not tasks:
        print("No tasks found. Exiting.")
        return

    from multiprocessing import Pool
    from tqdm import tqdm

    with Pool(NUM_WORKERS) as pool:
        for parquet_url, rg_idx, success in tqdm(
                pool.imap_unordered(process_row_group_task, tasks),
                total=len(tasks),
                desc="All RowGroups"
        ):
            if not success:
                print(f"Row group {rg_idx} from {parquet_url} failed after retries.")

    # Create indexes after full load
    for _, main_table in jobs:
        create_indexes(main_table)

    overall_end = time.time()
    print(f"Total ETL runtime: {overall_end - overall_start:.1f} sec")


if __name__ == "__main__":
    main()

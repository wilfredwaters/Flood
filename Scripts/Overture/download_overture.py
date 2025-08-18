#!/usr/bin/env python3.12
"""
Stream Overture Maps Buildings (and optionally POIs) from S3 into PostGIS.

Features:
- Full ETL for buildings (POIs optional, currently commented out)
- First-run table creation (staging + main) based on Parquet schema
- Restart-safe: resumes from unprocessed row groups
- Local JSON log (etl_progress_log.json) and Postgres load_log table
- Batch-level threading to avoid daemonic process issues
- Indexes & constraints added after full load for efficiency
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
import pyarrow.fs as fs

# ----------------------- CONFIGURATION -----------------------
POSTGIS_URL = "postgresql+psycopg2://docker:docker@localhost:25432/gis"

MAIN_TABLE_BUILDINGS = "building_usa"
# MAIN_TABLE_POIS = "place_usa"  # Uncomment if processing POIs

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
TEST_FIRST_ROW_GROUP_ONLY = True

# ----------------------- POSTGIS ENGINE -----------------------
engine = create_engine(POSTGIS_URL)

# ----------------------- LOG UTILITIES -----------------------
def load_log():
    """Load local JSON progress log."""
    try:
        with open(LOG_PATH, "r") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}

def update_log(log):
    """Write local JSON progress log."""
    with open(LOG_PATH, "w") as f:
        json.dump(log, f, indent=2)

def row_group_already_done(log, parquet_path, row_group_idx):
    """Check if row group has been successfully processed."""
    return log.get(parquet_path, {}).get(str(row_group_idx), False)

# ----------------------- LOAD LOG TABLE -----------------------
def ensure_load_log_table():
    """Ensure Postgres table to track processed row groups exists."""
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS load_log (
            parquet_path TEXT NOT NULL,
            row_group_idx INT NOT NULL,
            main_table TEXT NOT NULL,
            PRIMARY KEY(parquet_path, row_group_idx, main_table)
        );
        """))

def mark_row_group_done(parquet_path, row_group_idx, main_table):
    """Mark a row group as done in Postgres load_log table."""
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO load_log(parquet_path, row_group_idx, main_table)
            VALUES (:parquet_path, :row_group_idx, :main_table)
            ON CONFLICT DO NOTHING;
        """), {"parquet_path": parquet_path, "row_group_idx": row_group_idx, "main_table": main_table})

def is_row_group_done(parquet_path, row_group_idx, main_table):
    """Check if row group is done in Postgres load_log table."""
    with engine.begin() as conn:
        res = conn.execute(text("""
            SELECT 1 FROM load_log 
            WHERE parquet_path=:parquet_path AND row_group_idx=:row_group_idx AND main_table=:main_table
        """), {"parquet_path": parquet_path, "row_group_idx": row_group_idx, "main_table": main_table})
        return res.first() is not None

# ----------------------- TABLE CREATION -----------------------
def create_staging_table_from_parquet(parquet_path, staging_table):
    """Create a staging table based on the Parquet file schema."""
    filesystem, path = fs.FileSystem.from_uri(parquet_path)
    pf = pq.ParquetFile(path, filesystem=filesystem)
    schema = pf.schema_arrow
    cols = []
    for i, name in enumerate(schema.names):
        pa_type = schema.types[i]
        if pa_type == "string":
            pg_type = "TEXT"
        elif pa_type == "int64":
            pg_type = "BIGINT"
        elif pa_type == "double":
            pg_type = "DOUBLE PRECISION"
        elif pa_type == "bool":
            pg_type = "BOOLEAN"
        elif pa_type == "binary":
            pg_type = "BYTEA"
        else:
            pg_type = "TEXT"
        cols.append(f"{name} {pg_type}")
    # Ensure geometry column exists
    if "geometry" not in schema.names:
        cols.append("geometry GEOMETRY")
    cols_sql = ", ".join(cols)
    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {staging_table};"))
        conn.execute(text(f"CREATE TABLE {staging_table} ({cols_sql});"))

def create_main_table_from_staging(staging_table, main_table):
    """Create the main table based on the first staging table schema."""
    with engine.begin() as conn:
        res = conn.execute(text(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name='{staging_table}';
        """))
        cols_sql = ", ".join([f"{row['column_name']} {row['data_type']}" for row in res])
        conn.execute(text(f"CREATE TABLE IF NOT EXISTS {main_table} ({cols_sql});"))

# ----------------------- INDEX CREATION -----------------------
def create_indexes(main_table):
    """Create indexes on the main table after full load."""
    with engine.begin() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {main_table}_geom_idx ON {main_table} USING GIST (geometry);"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {main_table}_subtype_idx ON {main_table} (subtype);"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {main_table}_class_idx ON {main_table} (class);"))
    print(f"Indexes created on {main_table}.")

# ----------------------- BATCH PROCESSING -----------------------
def process_batch(batch, staging_table):
    """Process a batch of rows into a staging table."""
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
    """Process a single row group: create staging table, load data, then insert into main table."""
    parquet_url, row_group_idx, main_table = task
    staging_table = f"{main_table}_staging_{row_group_idx}"
    
    if is_row_group_done(parquet_url, row_group_idx, main_table):
        return (parquet_url, row_group_idx, True)

    attempts = 0
    while attempts < RETRIES:
        try:
            # First-run table creation
            if row_group_idx == 0:
                create_staging_table_from_parquet(parquet_url, staging_table)
                create_main_table_from_staging(staging_table, main_table)
            else:
                create_staging_table_from_parquet(parquet_url, staging_table)

            process_row_group_batches(parquet_url, row_group_idx, staging_table)

            with engine.begin() as conn:
                conn.execute(text(f"INSERT INTO {main_table} SELECT * FROM {staging_table};"))
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table};"))

            mark_row_group_done(parquet_url, row_group_idx, main_table)
            return (parquet_url, row_group_idx, True)
        except Exception as e:
            print(f"Error processing row group {row_group_idx} from {parquet_url}: {e}")
            attempts += 1
            time.sleep(RETRY_DELAY)
    return (parquet_url, row_group_idx, False)

# ----------------------- MAIN STREAMING FUNCTION -----------------------
def collect_row_group_tasks(parquet_patterns, log):
    """List row group tasks from S3 based on patterns, skipping completed ones."""
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
                        return tasks
    return tasks

def main():
    """Main ETL orchestration: collect tasks, process row groups, update logs, create indexes."""
    overall_start = time.time()
    log = load_log()
    ensure_load_log_table()

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

    # Create indexes after full load
    for _, main_table in jobs:
        create_indexes(main_table)

    overall_end = time.time()
    print(f"Total ETL runtime: {overall_end - overall_start:.1f} sec")

if __name__ == "__main__":
    main()

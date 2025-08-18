#!/usr/bin/env python3.13
"""
Stream Overture Maps Buildings (and optionally POIs) from S3 into PostGIS.

Features:
- First-run table creation: staging + main tables.
- Main table schema inherits from first staging table.
- Incremental inserts: safe on restart or crash.
- Checkpointing via PostgreSQL `load_log` table (no JSON log).
- Row-group processing with threads to avoid daemonic issues.
- All operations for a row group are performed in a single, atomic transaction.
- Correct on-the-fly CRS transformation using PostGIS.
- Efficient, centralized S3 filesystem handling.
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
# MAIN_TABLE_POIS = "poi_usa"  # Uncomment if processing POIs

NUM_WORKERS = 4            # Global row group pool
BATCH_WORKERS = 4          # Batch-level parallelism per row group
BATCH_SIZE = 100_000
RETRIES = 3
RETRY_DELAY = 5  # seconds
USA_BBOX = [-125.0, 24.0, -66.9, 49.4]

# CRS and SRID configuration
TARGET_CRS = "EPSG:5070"
# <<<<<<< START OF FIX >>>>>>>
# PostGIS expects an integer SRID, not the full "EPSG:xxxx" string
TARGET_SRID = TARGET_CRS.split(':')[-1]
# <<<<<<< END OF FIX >>>>>>>

# S3 Parquet URL patterns
BUILDINGS_S3_PATTERN = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=buildings/type=building/*"
# POIS_S3_PATTERN = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=places/type=place/*"  # Uncomment if needed

TEST_FIRST_ROW_GROUP_ONLY = True

# ----------------------- POSTGIS ENGINE -----------------------
engine = create_engine(POSTGIS_URL, pool_size=NUM_WORKERS + BATCH_WORKERS)

# ----------------------- CHECKPOINTING -----------------------
def ensure_load_log_table():
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
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM load_log WHERE parquet_url = :url AND row_group_idx = :rg"),
            {"url": parquet_url, "rg": row_group_idx}
        ).first()
        return result is not None

def mark_row_group_done(parquet_url, row_group_idx):
    with engine.begin() as conn:
        conn.execute(
            text("INSERT INTO load_log(parquet_url, row_group_idx) VALUES(:url, :rg) ON CONFLICT DO NOTHING;"),
            {"url": parquet_url, "rg": row_group_idx}
        )

# ----------------------- INDEX CREATION -----------------------
def create_indexes(main_table):
    with engine.begin() as conn:
        exists = conn.execute(text(f"SELECT to_regclass('{main_table}');")).scalar()
        if not exists:
            print(f"[WARNING] Main table {main_table} does not exist. Skipping index creation.")
            return
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {main_table}_geom_idx ON {main_table} USING GIST (geometry);"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {main_table}_subtype_idx ON {main_table} (subtype);"))
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {main_table}_class_idx ON {main_table} (class);"))
    print(f"[INFO] Indexes created on {main_table}.")

# ----------------------- BATCH PROCESSING -----------------------
def create_staging_table(parquet_file, staging_table, conn):
    schema = parquet_file.schema.to_arrow_schema()
    columns = []
    for field in schema:
        col_name = field.name
        col_type_str = str(field.type).lower()
        
        pg_type = "TEXT"
        if 'binary' in col_type_str: pg_type = "BYTEA"
        elif 'int64' in col_type_str: pg_type = "BIGINT"
        elif 'int32' in col_type_str: pg_type = "INTEGER"
        elif 'float64' in col_type_str: pg_type = "DOUBLE PRECISION"
        elif 'bool' in col_type_str: pg_type = "BOOLEAN"
        
        if col_name == 'geometry':
            pg_type = "GEOMETRY(Geometry, 4326)"

        columns.append(f"{col_name} {pg_type}")

    create_sql = f"CREATE TABLE {staging_table} ({', '.join(columns)});"
    conn.execute(text(create_sql))
    print(f"[DEBUG] Created staging table {staging_table}")

def process_batch(batch, staging_table, conn):
    df = batch.to_pandas()
    if df.empty: return

    df['geometry'] = df['geometry'].apply(wkb.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
    
    minx, miny, maxx, maxy = USA_BBOX
    gdf = gdf.cx[minx:maxx, miny:maxy]
    if gdf.empty: return

    gdf.to_postgis(staging_table, conn, if_exists='append', index=False)

def process_row_group_batches(parquet_file, row_group_idx, staging_table, conn):
    batches = list(parquet_file.iter_batches(batch_size=BATCH_SIZE, row_groups=[row_group_idx]))
    print(f"[DEBUG] Processing {len(batches)} batches for row group {row_group_idx}.")
    with ThreadPoolExecutor(max_workers=BATCH_WORKERS) as executor:
        executor.map(lambda batch: process_batch(batch, staging_table, conn), batches)

# ----------------------- ROW GROUP PROCESSING -----------------------
def process_row_group_task(task, s3fs_instance):
    parquet_url, row_group_idx, main_table = task
    staging_table = f"{main_table}_staging_{row_group_idx}"

    if not TEST_FIRST_ROW_GROUP_ONLY and row_group_already_done(parquet_url, row_group_idx):
        print(f"[DEBUG] Row group {row_group_idx} already processed, skipping.")
        return True

    attempts = 0
    while attempts < RETRIES:
        try:
            parquet_file = pq.ParquetFile(parquet_url, filesystem=s3fs_instance)
            
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table};"))
                create_staging_table(parquet_file, staging_table, conn)
                process_row_group_batches(parquet_file, row_group_idx, staging_table, conn)

                exists = conn.execute(text(f"SELECT to_regclass('{main_table}');")).scalar()
                if not exists:
                    print(f"[DEBUG] Main table {main_table} does not exist, creating from {staging_table} ...")
                    conn.execute(text(f"CREATE TABLE {main_table} AS TABLE {staging_table} WITH NO DATA;"))
                    # <<<<<<< START OF FIX >>>>>>>
                    # Use the integer SRID for the ALTER TABLE command
                    conn.execute(text(f"ALTER TABLE {main_table} ALTER COLUMN geometry TYPE geometry(Geometry, {TARGET_SRID});"))
                    # <<<<<<< END OF FIX >>>>>>>

                staging_cols_res = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{staging_table}' ORDER BY ordinal_position;"))
                staging_cols = [row[0] for row in staging_cols_res]
                
                select_expressions = []
                for col in staging_cols:
                    if col == 'geometry':
                        # <<<<<<< START OF FIX >>>>>>>
                        # Use the integer SRID for the ST_Transform function
                        select_expressions.append(f"ST_Transform(geometry, {TARGET_SRID})")
                        # <<<<<<< END OF FIX >>>>>>>
                    else:
                        select_expressions.append(col)
                
                insert_sql = f"INSERT INTO {main_table} ({', '.join(staging_cols)}) SELECT {', '.join(select_expressions)} FROM {staging_table};"
                conn.execute(text(insert_sql))
                print(f"[DEBUG] Row group {row_group_idx} inserted into main table {main_table}.")

            mark_row_group_done(parquet_url, row_group_idx)
            print(f"[DEBUG] Row group {row_group_idx} marked as done.")
            return True
        except Exception as e:
            print(f"[ERROR] Error processing row group {row_group_idx} from {parquet_url}: {e}")
            attempts += 1
            time.sleep(RETRY_DELAY)
    return False

# ----------------------- TASK COLLECTION -----------------------
def collect_row_group_tasks(parquet_patterns, s3fs_instance):
    tasks = []
    for pattern, main_table in parquet_patterns:
        files = s3fs_instance.glob(pattern)
        if not files:
            print(f"[DEBUG] No files found for pattern {pattern}")
            continue
        
        for file_path in files:
            parquet_url = f"s3://{file_path}"
            try:
                pf = pq.ParquetFile(parquet_url, filesystem=s3fs_instance)
                for rg_idx in range(pf.num_row_groups):
                    if TEST_FIRST_ROW_GROUP_ONLY:
                        print("[INFO] TEST_FIRST_ROW_GROUP_ONLY is True. Processing only one task.")
                        return [(parquet_url, rg_idx, main_table)]
                    
                    if not row_group_already_done(parquet_url, rg_idx):
                        tasks.append((parquet_url, rg_idx, main_table))
            except Exception as e:
                print(f"[WARNING] Could not read metadata from {parquet_url}. Skipping. Error: {e}")
    return tasks

# ----------------------- MAIN STREAMING FUNCTION -----------------------
def main():
    overall_start = time.time()
    ensure_load_log_table()

    s3 = s3fs.S3FileSystem(anon=True)

    jobs = [
        (BUILDINGS_S3_PATTERN, MAIN_TABLE_BUILDINGS),
        # (POIS_S3_PATTERN, MAIN_TABLE_POIS),
    ]

    tasks = collect_row_group_tasks(jobs, s3)
    print(f"[INFO] Total row-group tasks to process: {len(tasks)}")

    if not tasks:
        print("[INFO] No new tasks to process. Exiting.")
        return

    from multiprocessing import Pool
    from functools import partial

    worker_func = partial(process_row_group_task, s3fs_instance=s3)

    with Pool(NUM_WORKERS) as pool:
        for _ in tqdm(
                pool.imap_unordered(worker_func, tasks),
                total=len(tasks),
                desc="All RowGroups"
        ):
            pass

    print("[INFO] Data loading complete. Creating indexes...")
    for _, main_table in jobs:
        create_indexes(main_table)

    overall_end = time.time()
    print(f"[INFO] Total ETL runtime: {overall_end - overall_start:.1f} sec")

if __name__ == "__main__":
    main()

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
- Optional toggle to stop after the first successful data insert.
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

NUM_WORKERS = 4
BATCH_WORKERS = 4
BATCH_SIZE = 100_000
TARGET_CRS = "EPSG:5070"
TARGET_SRID = TARGET_CRS.split(':')[-1]
RETRIES = 3
RETRY_DELAY = 5
USA_BBOX = [-125.0, 24.0, -66.9, 49.4]

# S3 Parquet URL patterns
BUILDINGS_S3_PATTERN = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=buildings/type=building/*"
# POIS_S3_PATTERN = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=places/type=place/*"

# <<<<<<< START OF NEW FEATURE >>>>>>>
# Set to True to run the script only until the first non-empty row group is successfully processed.
STOP_AFTER_FIRST_SUCCESS = True
# Set to False for a full production run.
# <<<<<<< END OF NEW FEATURE >>>>>>>

# ----------------------- POSTGIS ENGINE -----------------------
engine = create_engine(POSTGIS_URL, pool_size=NUM_WORKERS + BATCH_WORKERS)

# ----------------------- CHECKPOINTING & LOGGING -----------------------
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
        if col_name == 'geometry': pg_type = f"GEOMETRY(Geometry, 4326)"
        columns.append(f"{col_name} {pg_type}")
    create_sql = f"CREATE TABLE {staging_table} ({', '.join(columns)});"
    conn.execute(text(create_sql))

def process_batch(batch, conn):
    df = batch.to_pandas()
    if df.empty: return None
    df['geometry'] = df['geometry'].apply(wkb.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs="EPSG:4326")
    minx, miny, maxx, maxy = USA_BBOX
    gdf = gdf.cx[minx:maxx, miny:maxy]
    return gdf if not gdf.empty else None

def process_row_group_batches(parquet_file, row_group_idx, staging_table, conn):
    batches = list(parquet_file.iter_batches(batch_size=BATCH_SIZE, row_groups=[row_group_idx]))
    total_rows = 0
    with ThreadPoolExecutor(max_workers=BATCH_WORKERS) as executor:
        results = executor.map(process_batch, batches, [conn] * len(batches))
        for gdf in results:
            if gdf is not None:
                total_rows += len(gdf)
                gdf.to_postgis(staging_table, conn, if_exists='append', index=False)
    return total_rows

# ----------------------- ROW GROUP PROCESSING -----------------------
def process_row_group_task(task, s3fs_instance):
    parquet_url, row_group_idx, main_table = task
    staging_table = f"{main_table}_staging_{row_group_idx}"

    if row_group_already_done(parquet_url, row_group_idx):
        return (True, 0) # Success, but 0 rows processed by this run

    attempts = 0
    while attempts < RETRIES:
        try:
            parquet_file = pq.ParquetFile(parquet_url, filesystem=s3fs_instance)
            row_count = 0
            with engine.begin() as conn:
                conn.execute(text(f"DROP TABLE IF EXISTS {staging_table};"))
                create_staging_table(parquet_file, staging_table, conn)
                row_count = process_row_group_batches(parquet_file, row_group_idx, staging_table, conn)

                if row_count > 0:
                    exists = conn.execute(text(f"SELECT to_regclass('{main_table}');")).scalar()
                    if not exists:
                        conn.execute(text(f"CREATE TABLE {main_table} AS TABLE {staging_table} WITH NO DATA;"))
                        conn.execute(text(f"ALTER TABLE {main_table} ALTER COLUMN geometry TYPE geometry(Geometry, {TARGET_SRID});"))
                    
                    staging_cols_res = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{staging_table}' ORDER BY ordinal_position;"))
                    staging_cols = [row[0] for row in staging_cols_res]
                    select_expressions = [f"ST_Transform(geometry, {TARGET_SRID})" if col == 'geometry' else col for col in staging_cols]
                    insert_sql = f"INSERT INTO {main_table} ({', '.join(staging_cols)}) SELECT {', '.join(select_expressions)} FROM {staging_table};"
                    conn.execute(text(insert_sql))
            
            mark_row_group_done(parquet_url, row_group_idx)
            return (True, row_count)
        except Exception as e:
            print(f"[ERROR] Error processing row group {row_group_idx} from {parquet_url}: {e}")
            attempts += 1
            time.sleep(RETRY_DELAY)
    return (False, 0)

# ----------------------- TASK COLLECTION -----------------------
def collect_row_group_tasks(parquet_patterns, s3fs_instance):
    tasks = []
    for pattern, main_table in parquet_patterns:
        try:
            files = s3fs_instance.glob(pattern)
            if not files: continue
            for file_path in files:
                parquet_url = f"s3://{file_path}"
                pf = pq.ParquetFile(parquet_url, filesystem=s3fs_instance)
                for rg_idx in range(pf.num_row_groups):
                    if not row_group_already_done(parquet_url, rg_idx):
                        tasks.append((parquet_url, rg_idx, main_table))
        except Exception as e:
            print(f"[WARNING] Could not list or read files for pattern {pattern}. Error: {e}")
    return tasks

# ----------------------- MAIN STREAMING FUNCTION -----------------------
def main():
    overall_start = time.time()
    ensure_load_log_table()
    s3 = s3fs.S3FileSystem(anon=True)

    jobs = [(BUILDINGS_S3_PATTERN, MAIN_TABLE_BUILDINGS)]
    # jobs.append((POIS_S3_PATTERN, MAIN_TABLE_POIS))

    tasks = collect_row_group_tasks(jobs, s3)
    print(f"[INFO] Total row-group tasks to process: {len(tasks)}")
    if not tasks: return

    from multiprocessing import Pool
    from functools import partial

    worker_func = partial(process_row_group_task, s3fs_instance=s3)

    with Pool(NUM_WORKERS) as pool:
        for success, row_count in tqdm(pool.imap_unordered(worker_func, tasks), total=len(tasks), desc="All RowGroups"):
            if success and row_count > 0:
                print(f"[INFO] Successfully processed {row_count} rows.")
                if STOP_AFTER_FIRST_SUCCESS:
                    print("[INFO] STOP_AFTER_FIRST_SUCCESS is True. Halting processing.")
                    pool.terminate() # Stop all worker processes immediately
                    break
            elif not success:
                print(f"[ERROR] A task failed after all retries.")

    print("[INFO] Data loading complete. Creating indexes...")
    for _, main_table in jobs:
        create_indexes(main_table)

    overall_end = time.time()
    print(f"[INFO] Total ETL runtime: {overall_end - overall_start:.1f} sec")

if __name__ == "__main__":
    main()

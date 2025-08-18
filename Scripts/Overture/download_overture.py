#!/usr/bin/env python3.13
"""
Stream Overture Maps Buildings from S3 into PostGIS using the official spatial index.

Features:
- HIGHLY EFFICIENT: Reads the spatial index first to find exactly which Parquet files
  intersect the target BBOX, avoiding scanning thousands of irrelevant files.
- Uses authenticated S3 access.
- First-run table creation: staging + main tables.
- Incremental inserts: safe on restart or crash.
- Checkpointing via PostgreSQL `load_log` table.
- All operations for a row group are performed in a single, atomic transaction.
"""

import time
import pyarrow.parquet as pq
import geopandas as gpd
from shapely import wkb
from shapely.geometry import box
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor
import s3fs
from tqdm import tqdm
from multiprocessing import Pool
from functools import partial

# ----------------------- CONFIGURATION -----------------------
POSTGIS_URL = "postgresql+psycopg2://docker:docker@localhost:25432/gis"
MAIN_TABLE_BUILDINGS = "building_usa"

NUM_WORKERS = 4
BATCH_WORKERS = 4
BATCH_SIZE = 100_000
TARGET_CRS = "EPSG:5070"
TARGET_SRID = TARGET_CRS.split(':')[-1]
RETRIES = 3
RETRY_DELAY = 5
USA_BBOX = [-125.0, 24.0, -66.9, 49.4]

# URL to the official spatial index for the buildings theme.
# This file contains the bounding box for every Parquet file in the dataset.
SPATIAL_INDEX_URL = "s3://overturemaps-us-west-2/release/2025-07-23.0/theme=buildings/spatial-index.geojson.gz"

# Set to False for a full production run.
STOP_AFTER_FIRST_SUCCESS = False

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
        if col_name == 'geometry': pg_type = "GEOMETRY(Geometry, 4326)"
        columns.append(f"{col_name} {pg_type}")
    create_sql = f"CREATE TABLE {staging_table} ({', '.join(columns)});"
    conn.execute(text(create_sql))

def process_batch(batch):
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
        results = executor.map(process_batch, batches)
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
        return (True, 0)

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

# ----------------------- TASK COLLECTION (SPATIAL INDEX STRATEGY) -----------------------
def get_intersecting_files(s3fs_instance):
    """Reads the spatial index to find Parquet files that intersect the USA BBOX."""
    print(f"[INFO] Reading spatial index to find relevant files: {SPATIAL_INDEX_URL}")
    try:
        with s3fs_instance.open(SPATIAL_INDEX_URL, 'rb') as f:
            index_gdf = gpd.read_file(f)
        
        usa_polygon = box(*USA_BBOX)
        
        intersecting_idx = index_gdf.sindex.query(usa_polygon, predicate='intersects')
        intersecting_files_gdf = index_gdf.iloc[intersecting_idx]
        
        base_s3_path = SPATIAL_INDEX_URL.rsplit('/', 1)[0]
        file_list = [f"{base_s3_path}/{row['file']}" for _, row in intersecting_files_gdf.iterrows()]
        
        print(f"[INFO] Found {len(file_list)} Parquet files intersecting the USA BBOX.")
        return file_list
    except Exception as e:
        print(f"[ERROR] Could not read or process the spatial index file. Error: {e}")
        return []

def collect_row_group_tasks(file_list, main_table, s3fs_instance):
    """Generates row group tasks from a pre-filtered list of Parquet files."""
    tasks = []
    for parquet_url in file_list:
        try:
            pf = pq.ParquetFile(parquet_url, filesystem=s3fs_instance)
            for rg_idx in range(pf.num_row_groups):
                if not row_group_already_done(parquet_url, rg_idx):
                    tasks.append((parquet_url, rg_idx, main_table))
        except Exception as e:
            print(f"[WARNING] Could not read metadata from {parquet_url}. Skipping. Error: {e}")
    return tasks

# ----------------------- MAIN FUNCTION (SPATIAL INDEX STRATEGY) -----------------------
def main():
    overall_start = time.time()
    ensure_load_log_table()
    
    # Use authenticated access for S3
    s3 = s3fs.S3FileSystem()

    # 1. Get the precise list of files to process from the spatial index
    intersecting_files = get_intersecting_files(s3)
    if not intersecting_files:
        print("[ERROR] No intersecting files found. Exiting.")
        return

    # 2. Generate tasks only for the relevant files
    tasks = collect_row_group_tasks(intersecting_files, MAIN_TABLE_BUILDINGS, s3)
    print(f"[INFO] Found {len(tasks)} total row-group tasks to process.")
    if not tasks:
        print("[INFO] No new tasks to process. All relevant files are already logged as done.")
        return

    # 3. Execute the tasks
    worker_func = partial(process_row_group_task, s3fs_instance=s3)
    with Pool(NUM_WORKERS) as pool:
        for success, row_count in tqdm(pool.imap_unordered(worker_func, tasks), total=len(tasks), desc="All RowGroups"):
            if success and row_count > 0:
                print(f"\n[INFO] Successfully processed a chunk with {row_count} rows.")
                if STOP_AFTER_FIRST_SUCCESS:
                    print("[INFO] STOP_AFTER_FIRST_SUCCESS is True. Halting processing.")
                    pool.terminate()
                    break
            elif not success:
                print(f"\n[ERROR] A task failed after all retries.")

    # 4. Create indexes
    print("[INFO] Data loading complete. Creating indexes...")
    create_indexes(MAIN_TABLE_BUILDINGS)

    overall_end = time.time()
    print(f"[INFO] Total ETL runtime: {overall_end - overall_start:.1f} sec")

if __name__ == "__main__":
    main()

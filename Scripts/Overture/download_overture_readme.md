# Overture Maps US ETL to PostGIS

## Overview

This Python script streams Overture Maps building footprints and points of interest (POIs) directly from the Overture S3 data lake into a PostGIS database, without saving local Parquet files.

It is designed for large-scale, production-grade ETL and implements advanced features such as parallel processing, atomic row group insertion, resume support, and detailed logging.

## Features

### 1. Single USA Bounding Box
- Supports ingesting all US data using a single bounding box.
- Pushdown filter leverages the Overture `bbox` column for efficient S3 reads.
- No fallback to geometry is needed; only row groups overlapping the bounding box are processed.

### 2. Row-Group Level Processing
- Reads each Parquet row group individually.
- Converts WKB geometries to Shapely and reprojects to EPSG:5070.
- Each row group is written to a staging table, then atomically moved to the main table on success.
- Ensures the main table is never partially populated by failed row groups.

### 3. Parallel Processing
- **Global Pool:** Processes multiple row groups concurrently across all files.
- **Nested Pool:** Processes batches within each row group concurrently, optimizing memory usage and CPU utilization.
- Highly efficient for very large datasets.

### 4. Resume Support
- JSON log tracks completed row groups by file and index.
- If interrupted (internet loss or laptop shutdown), the script resumes from the last successfully processed row group.

### 5. Retry Logic
- Transient S3 or network errors are automatically retried with a configurable delay.
- Ensures robustness in unreliable network conditions.

### 6. Index Creation
- **Spatial Index:** GIST on `geometry`.
- **Attribute Indexes:** BTREE on `subtype` and `class`.
- Built after all row groups are successfully loaded.

### 7. Verbose Progress
- Uses `tqdm` progress bars for visual feedback.
- Detailed per-row-group logging to the JSON log with timestamps and error messages.

### 8. Timers
- Reports runtime per row group and total ETL runtime for monitoring and benchmarking.

### 9. POIs Support (Optional)
- Optional ingestion of Overture Places (POIs) is included.
- Enabled by uncommenting the relevant line in the `jobs` list within the `main()` function.
- Currently commented out if only buildings are needed.

## Dependencies

- Python ≥3.12
- pyarrow
- pandas
- geopandas
- sqlalchemy
- psycopg2-binary
- tqdm
- fsspec (for S3 streaming)

Install via:

```bash
pip install -r requirements.txt
```

## Configuration

**PostGIS Connection:**
Defined via SQLAlchemy engine, e.g.:

```python 
POSTGIS_URL = "postgresql+psycopg2://docker:docker@localhost:25432/gis" 
```

### Main Table Names:

Buildings: building_usa

POIs: place_usa (optional)

### S3 Paths:

Buildings: s3://overturemaps-us-west-2/release/<version>/theme=buildings/type=building/*

Places: s3://overturemaps-us-west-2/release/<version>/theme=places/type=place/*

## Usage
python stream_overture_to_postgis.py


By default, streams buildings only.

Optional POIs can be enabled by uncommenting the corresponding line in the jobs list.

Resume is automatic if interrupted; JSON log tracks progress.

## Logging & Resume

**Log File:** etl_progress.json
**Format:** Nested JSON

```json 
{
  "s3://overturemaps-us-west-2/.../building.parquet": {
    "0": true,
    "1": true,
    "2": false
  }
} 
```



true indicates successful processing of a row group.

Failed or incomplete row groups will be retried on next run.

## Performance Notes

Efficient streaming avoids local Parquet storage.

Pushdown filtering on bbox reduces S3 I/O.

Parallelization maximizes CPU utilization.

Staging tables ensure atomic insertion and data integrity.

## License

All data used are public from Overture Maps Foundation.

This script is open-source and free to use.
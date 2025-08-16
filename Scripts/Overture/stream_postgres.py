import pyarrow.parquet as pq
import pandas as pd
import geopandas as gpd
from shapely import wkb
from sqlalchemy import create_engine
from sqlalchemy import text

# PostgreSQL connection (Docker PostGIS)
engine = create_engine("postgresql+psycopg2://docker:docker@localhost:25432/gis")

CHUNK_SIZE = 25000
TARGET_CRS = "EPSG:5070"

def push_parquet_chunks(parquet_path, table_name):
    parquet_file = pq.ParquetFile(parquet_path)
    total_rows = parquet_file.metadata.num_rows
    num_row_groups = parquet_file.num_row_groups
    print(f"Parquet has {total_rows} rows in {num_row_groups} row groups")

    for i in range(num_row_groups):
        # Read one row group at a time
        table = parquet_file.read_row_group(i)
        df_chunk = table.to_pandas()

        # Convert WKB to Shapely
        df_chunk['geometry'] = df_chunk['geometry'].apply(wkb.loads)

        # Create GeoDataFrame and reproject
        gdf = gpd.GeoDataFrame(df_chunk, geometry='geometry', crs="EPSG:4326")
        gdf = gdf.to_crs(TARGET_CRS)

        # Push to Postgres
        gdf.to_postgis(table_name, engine, if_exists='append', index=False)
        print(f"Pushed row group {i+1}/{num_row_groups} ({len(gdf)} rows)")

    # Create spatial index
    with engine.connect() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {table_name}_geom_idx ON {table_name} USING GIST (geometry);"))
        print(f"Spatial index created on {table_name}.geometry")

def main():
    parquet_path = "/Volumes/Tooth/FloodProject/02_Overture/Buildings/building_midwest.parquet"
    table_name = "building_midwest"

    print(f"Streaming {table_name} from {parquet_path}...")
    push_parquet_chunks(parquet_path, table_name)
    print("Done!")

if __name__ == "__main__":
    main()

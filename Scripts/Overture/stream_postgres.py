import pandas as pd
import geopandas as gpd
from shapely import wkb
from sqlalchemy import create_engine
from sqlalchemy import text

# PostgreSQL connection — adjust your user, password, host, db
engine = create_engine("postgresql+psycopg2://user:password@localhost:5432/mydb")

CHUNK_SIZE = 25000
TARGET_CRS = "EPSG:5070"  # USA Contiguous Albers

def push_parquet_chunks(parquet_path, table_name):
    parquet_iter = pd.read_parquet(parquet_path, chunksize=CHUNK_SIZE)

    for i, df_chunk in enumerate(parquet_iter, 1):
        # Convert WKB to Shapely geometries
        df_chunk['geometry'] = df_chunk['geometry'].apply(wkb.loads)

        # Create GeoDataFrame with correct CRS
        gdf = gpd.GeoDataFrame(df_chunk, geometry='geometry', crs="EPSG:4326")

        # Reproject to EPSG:5070
        gdf = gdf.to_crs(TARGET_CRS)

        # Push to Postgres (append chunks)
        gdf.to_postgis(table_name, engine, if_exists='append', index=False)
        print(f"Pushed chunk {i} ({len(gdf)} rows) to {table_name}")

    # Create spatial index after all chunks loaded
    with engine.connect() as conn:
        conn.execute(text(f"CREATE INDEX IF NOT EXISTS {table_name}_geom_idx ON {table_name} USING GIST (geometry);"))
        print(f"Spatial index created on {table_name}.geometry")

def main():
    parquet_path = "/Volumes/Tooth/FloodProject/02_Overture/Buildings/building_midwest.parquet"
    table_name = "building_midwest"

    print(f"Streaming {table_name} from {parquet_path} in chunks of {CHUNK_SIZE} rows...")
    push_parquet_chunks(parquet_path, table_name)
    print("Done!")

if __name__ == "__main__":
    main()

# Overture Maps CONUS Downloader & Converter

This Python script automates downloading Overture Maps data for the contiguous United States (CONUS) and converting it to GeoPackage (GPKG) files in EPSG:5070 projection.

The script downloads **building footprints** and **points of interest (POIs)** for four predefined regions and converts each download from Parquet to GPKG with a progress bar.

---

## Features

- Downloads Overture Maps **POIs** (and optionally **buildings**) for CONUS regions:
  - Northeast
  - Midwest
  - South
  - West
- Corrected bounding boxes to avoid missing parts of the US.
- Converts Parquet files to GPKG in **EPSG:5070**.
- Skips macOS metadata files automatically.
- Displays a **progress bar** during conversion.

---

## Requirements

- Python 3.12
- [OvertureMaps CLI](https://pypi.org/project/overturemaps/)
- [GeoPandas](https://geopandas.org/)
- [PyArrow](https://arrow.apache.org/)
- [Tqdm](https://github.com/tqdm/tqdm)

Install dependencies with:

```bash
python3.12 -m pip install overturemaps geopandas pyarrow tqdm

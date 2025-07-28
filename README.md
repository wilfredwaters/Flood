# 🌾 FEMA Flood GPKG to GeoParquet Converter

This Python script automates the extraction and conversion of FEMA National Flood Hazard Layer (NFHL) data from nested ZIP files containing GeoPackages (`.gpkg`) into partitioned GeoParquet files suitable for large-scale geospatial analysis and cloud-native workflows.

It is optimized for high-performance processing with checkpointing, multiprocessing, logging, CRS standardization, and filtering by valid FEMA flood zone codes.

---

## 📦 Features

* ✅ Extracts recursively nested ZIP archives
* ✅ Converts `.gpkg` layers to GeoParquet using `geopandas`
* ✅ Filters data to valid flood zones only: `A`, `AE`, `AH`, `AO`, `AR`, `A99`, `V`, `VE`
* ✅ Auto-detects and assigns state FIPS codes for partitioned output
* ✅ Logs errors and progress in `conversion.log`
* ✅ Checkpoints processed GPKGs to avoid redundant work
* ✅ Handles missing or undefined CRS gracefully
* ✅ Parallel processing support (currently 1 thread by default, adjustable)

---

## 🗂 Directory Structure

* **Input Directory**: `/home/ubuntu/data/FloodData/` — Place your FEMA ZIP files here
* **Output Directory**: `/home/ubuntu/output/FloodData_Parquet/` — Converted GeoParquet files are saved here, partitioned by `STATEFP`
* **Checkpoint File**: `processed_gpkgs.txt` — Tracks completed GPKG files to allow resumable runs
* **Logs**: `conversion.log` — Timestamped processing details and errors

---

## 🚀 Quickstart

### 1. Clone the Repo

```bash
git clone https://github.com/yourusername/flood-gpkg-to-parquet.git
cd flood-gpkg-to-parquet
```

### 2. Install Dependencies

We recommend using a virtual environment:

```bash
pip install geopandas pyogrio tqdm
```

**Note**: You may also need system-level libraries for GDAL depending on your OS.

### 3. Add Your FEMA Data

Download and place your FEMA NFHL `.zip` files into:

```
/home/ubuntu/data/FloodData/
```

### 4. Run the Script

```bash
python convert_flood_data.py
```

Converted files will be saved in:

```
/home/ubuntu/output/FloodData_Parquet/STATEFP=XX/
```

---

## 🧠 How It Works

1. **ZIP Extraction**: Recursively extracts ZIPs, including nested ZIPs and filters out metadata files like `__MACOSX`.
2. **GPKG Processing**:

   * Reads all layers from each `.gpkg` file
   * Skips layers without a `FLD_ZONE` column
   * Filters only valid flood zones
   * Converts to EPSG:4326 (WGS84) if needed
3. **GeoParquet Output**:

   * Saved in a partitioned structure using the state FIPS code inferred from the filename
   * Uses Snappy compression for fast read/write

---

## 📋 Sample Output Path

```
output/
└── FloodData_Parquet/
    ├── STATEFP=48/
    │   ├── _NFHL_48_20250717_S_FLD_HAZ_AR-002__S_FLD_HAZ_AR.parquet
    │   └── ...
```

---

## 🛠 Configuration

You can change key paths and parameters by modifying the variables at the top of the script:

```python
input_dir = Path("/home/ubuntu/data/FloodData")
output_base_dir = Path("/home/ubuntu/output/FloodData_Parquet")
checkpoint_file = Path("processed_gpkgs.txt")
```

---

## 📃 Notes

* Output files use [GeoParquet](https://parquet.apache.org/) with geometry support for modern cloud-native geospatial workflows.
* Compatible with DuckDB, Wherobots, AWS Athena (with spatial plugins), and other modern stack tools.
* QGIS compatibility: use QGIS 3.28+ with GDAL 3.5+ for proper GeoParquet support.

---

## 🧪 Test Run

To verify a small number of files, move just one or two `.zip` files into your input directory and run:

```bash
python convert_flood_data.py
```

Check `conversion.log` and output folders for results.

---

## 🧼 Cleaning Up

To re-run everything from scratch:

```bash
rm processed_gpkgs.txt conversion.log
rm -r /home/ubuntu/output/FloodData_Parquet/*
```

---

## 📖 License

MIT License

---

## 🤝 Contributions

Pull requests, bug reports, and suggestions are welcome!

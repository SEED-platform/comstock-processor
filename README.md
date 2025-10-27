# ComStock Processor

A Python class to help download ComStock data locally for analysis. The `ComStockProcessor` class provides an easy interface to download metadata and time series data from the ComStock dataset hosted on AWS S3.

## Installation

Install and run poetry:

```bash
pip install poetry
poetry install
```

## ComStockProcessor Class

The `ComStockProcessor` class is located in `lib/comstock_processor.py` and provides methods to download and process ComStock building data.

### Initialization

```python
from pathlib import Path
from lib.comstock_processor import ComStockProcessor

# Initialize the processor
processor = ComStockProcessor(
    state="CA",           # 2-letter state abbreviation
    county_name="All",    # County name or "All"
    building_type="All",  # Building type or "All"
    upgrade="0",          # Upgrade identifier (0 = baseline)
    base_dir=Path("./datasets/comstock")  # Local directory to save data
)
```

### Methods

#### `process_metadata(save_dir: Path) -> pd.DataFrame`
Downloads and processes ComStock metadata with filtering based on the class constraints.

- Downloads the baseline metadata parquet file if not already present
- Filters by state, county, and building type as specified during initialization
- Saves filtered results as a CSV file
- Returns a pandas DataFrame with the filtered metadata

#### `process_building_time_series(data_frame, save_dir: Path) -> tuple`
Downloads time series data for buildings specified in the input DataFrame using parallel execution.

- Uses multi-threading to download building time series files efficiently
- Skips downloading files that already exist locally
- Downloads from the ComStock AWS S3 bucket
- Returns paths and building IDs of downloaded files

### Usage Example

```python
from pathlib import Path
from lib.comstock_processor import ComStockProcessor

# Set up directories
base_dir = Path("./datasets/comstock")
timeseries_dir = base_dir / "timeseries"
for d in [base_dir, timeseries_dir]:
    d.mkdir(parents=True, exist_ok=True)

# Initialize processor for California data
processor = ComStockProcessor(
    state="CA",
    county_name="All", 
    building_type="All",
    upgrade="0",
    base_dir=base_dir
)

# Download and filter metadata
metadata_df = processor.process_metadata(save_dir=base_dir)

# Download time series data for buildings in metadata
paths, building_ids = processor.process_building_time_series(
    metadata_df, 
    save_dir=timeseries_dir
)
```

### Data Source

The processor downloads data from the ComStock dataset hosted on AWS S3:
- **Base URL**: `https://oedi-data-lake.s3.amazonaws.com/nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/2024/comstock_amy2018_release_1/`
- **Data Explorer**: [OpenEI Data Lake Explorer](https://data.openei.org/s3_viewer?bucket=oedi-data-lake&prefix=nrel-pds-building-stock%2Fend-use-load-profiles-for-us-building-stock%2F2024%2Fcomstock_amy2018_release_1%2F)

### Performance Features

- **Parallel Downloads**: Uses ThreadPoolExecutor for concurrent file downloads
- **Smart Caching**: Skips downloading files that already exist locally
- **Progress Tracking**: Shows download progress with tqdm progress bars
- **Efficient Filtering**: Uses pandas parquet filtering for large datasets

## Development

### Committing

Before pushing changes to GitHub, run `pre-commit` to format the code consistently:

```bash
pre-commit run --all-files
```

If this doesn't work, try:

```bash
poetry update
poetry run pre-commit run --all-files
```

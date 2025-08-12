from nrcan_etl_toolbox.etl_toolbox.reader.source_readers import ExcelReader

# NRCAN ETL Toolbox


[![codecov](https://codecov.io/github/xmalet-nrcan/etl-toolbox/graph/badge.svg?token=L1B4RHVN2E)](https://codecov.io/github/xmalet-nrcan/etl-toolbox)
[![CI](https://github.com/xmalet-nrcan/etl-toolbox/actions/workflows/ci-release.yml/badge.svg)](https://github.com/xmalet-nrcan/etl-toolbox/actions/workflows/ci-release.yml)

Pour la version française de ce document, consultez [README-fr.md](README-fr.md).


`etl-toolbox` is a Python toolkit designed to simplify Extract, Transform, and Load (ETL) data processes. This modular toolkit offers several specialized components for different aspects of ETL workflows.

## Components

### etl_logging
Specialized logging module for ETL processes, allowing simple configuration and efficient log analysis.

### etl_toolbox
Collection of tools for reading data from various sources. It includes readers for different file formats and databases, facilitating data integration in ETL processes:
- **Data Readers**: CSV, Excel, GeoPackage, JSON, PostGIS, Shapefile


### database
Interfaces and ORM for interacting with different database systems:
- **Database Interfaces**: Abstract object handlers for database interactions
- **ORM**: Object-relational mappings to simplify data access

## Installation

Install the package via Poetry:

```bash
poetry install
```

Or by creating a distribution:

```bash
poetry build
pip install dist/nrcan_etl_toolbox-*.whl
```

## Usage

### Logging Module (etl_logging)

```python
from nrcan_etl_toolbox.etl_logging import CustomLogger

logger = CustomLogger(name="Test Logger", level='INFO'
                      ,logger_type='verbose',
                      logger_file_name='test_logger.log')

# Logging messages
logger.info("Starting ETL process")
logger.debug("Technical details", extra={"data": {"items": 100}})
logger.error("Processing error", exc_info=True)
```

### Data Readers (etl_toolbox)

```python
from nrcan_etl_toolbox.etl_toolbox.reader import ReaderFactory
from nrcan_etl_toolbox.etl_toolbox.reader.source_readers import ExcelReader

# Creating a CSV reader
csv_reader = ReaderFactory(input_source="data.csv")
data = csv_reader.data

# Creating a Shapefile reader
shp_reader = ReaderFactory(input_source="data.shp")
geo_data = shp_reader.data

# Creating a PostGIS reader
postgis_reader = ReaderFactory(input_source="postgresql://user:password@host:port/database", # Use the connection string for your database
                               table_name="table_name",
                               schema="schema_name")
geo_data = postgis_reader.data

# Creating an Excel reader
reader = ReaderFactory(input_source="data.xlsx")
# Get the Reader object
excel_reader : ExcelReader = reader.reader
# If excel file contains multiple sheets, 
# data will be a dictionary with sheet names as keys and dataframes as values
data = excel_reader.dataframe 
# data = {'Sheet1': df1, 'Sheet2': df2}
# To read a specific sheet, use the sheet_name parameter
data = excel_reader.read_sheet('Sheet1')
# data = df1
```

### Database Interface

```python
# TODO: Complete documentation.
from nrcan_etl_toolbox.database.interface import AbstractDatabaseHandler
# Usage example to be documented
```

## Development

To contribute to the project, install development dependencies:

```bash
poetry install --with dev
```

Run tests with:

```bash
pytest
```

## Project Structure

```
nrcan_etl_toolbox/
├── database/               # Database interactions
│   ├── interface/          # Abstract interfaces for databases
│   └── orm/                # Object-relational mappings
├── etl_logging/            # ETL logging module
└── etl_toolbox/            # Main ETL tools
    └── reader/             # Data source readers
        └── source_readers/ # Specific reader implementations
```

[//]: # (## License)

[//]: # ()
[//]: # (This project is distributed under the MIT license. See the [LICENSE]&#40;LICENSE&#41; file for more information.)

## Authors

- NRCAN (Natural Resources Canada)
- [Xavier Malet](mailto:xavier.malet@nrcan-rncan.gc.ca)

For questions or suggestions, please use the project's GitHub issues.

# StockForcasting

## Getting started

On this projects, i build a data pipeline for ingest data from Vnstock api, load it to Minio object storage, and transforming data into training-ready data for SARIMA models.
I am planing on create an Streamlit app for graphical interface for user (for visualization the output of the models)

## Project structure. \
├── .... \
├── docker-compose.yaml        # <------------------------Containerize MinIO and PostgreSQL (for later use) \
│ \
├── data/                      # <------------------------Store data and models for training \
│ \
├── src/ \
│   └── StockForcasting/ \
│       ├── __init__.py \
│       ├── definitions.py     # <------------------------Collects asset & resource definitions by scanning submodules \
│       └── defs/ \
│           ├── __init__.py \
│           ├── assets/        # <------------------------Asset definitions \
│           │   ├── cleaning.py \
│           │   ├── ingestion.py \
│           │   ├── modeling.py \
│           │   ├── transformation.py \
│           │   └── __init__.py    # <------------------------Loads assets above to create asset definitions \
│           ├── partition_defs/ \
│           │   └── partition_def.py #<------------------------ Monthly partitions for ingestion \
│           ├── resources/ \
│           │   ├── minio_io_manager.py  # <---------------------------------- I/O manager between assets and object storage (MinIO) \
│           │   └── psql_io_manager.py # <---------------------------------- Planned for DBT-based transformations (BI use cases) \
│           └── tickers/ \
│               └── tickers.csv # <--------------------------- Columns: ticker symbol | source (default: VCI) \
└── tests/ \
    └── __init__.py \

### Installing dependencies

**Option 1: uv**

Ensure [`uv`](https://docs.astral.sh/uv/) is installed following their [official documentation](https://docs.astral.sh/uv/getting-started/installation/).

Create a virtual environment, and install the required dependencies using _sync_:

```bash
uv sync
```

Then, activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

**Option 2: pip**

Install the python dependencies with [pip](https://pypi.org/project/pip/):

```bash
python3 -m venv .venv
```

Then activate the virtual environment:

| OS | Command |
| --- | --- |
| MacOS | ```source .venv/bin/activate``` |
| Windows | ```.venv\Scripts\activate``` |

Install the required dependencies:

```bash
pip install -e ".[dev]"
```

### Running Dagster

Start the Dagster UI web server:

```bash
dg dev
```

Open http://localhost:3000 in your browser to see the project.

## license


## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)

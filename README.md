# StockForcasting

## Getting started

On this projects, i build a data pipeline for ingest data from Vnstock api, load it to Minio object storage, and transforming data into training-ready data for SARIMA models.
I am planing on create an Streamlit app for graphical interface for user (for visualization the output of the models)

## Project structure
ª   .env
ª   .gitignore
ª   docker-compose.yaml <------------------------------- containerize the minio and postgres (for later use)
ª   pyproject.toml
ª   uv.lock
ª   
ª                   
+---data  #<------------------- for storing data and model for traning 
ª   +---minio
ª   +---model
ª   +---postgresql
+---src
ª   +---StockForcasting
ª       ª   definitions.py  #<---------------------------- it collect asset definitions, resources definitions by scanning all the sub files.
ª       ª   __init__.py
ª       ª   
ª       +---defs
ª           ª   __init__.py
ª           ª   
ª           +---assets # <------------------ definitions forall the assets.
ª           ª       cleaning.py
ª           ª       ingestion.py
ª           ª       modeling.py
ª           ª       transformation.py
ª           ª       __init__.py    #<---------------- I load these asset from above (cleaning.py, ingestion.py, modeling.py and transformation.py) to create asset definitions from them.
ª           ª       
ª           +---partition_defs
ª           ª       partition_def.py # <--------------------------- include monthly partitions use for ingestions
ª           ª       
ª           +---resources
ª           ª       minio_io_manager.py # <--------------------------- mainly use as input/ouput transmiter between asset and object storage.
ª           ª       psql_io_manager.py  # <--------------------------- for later expansion (i am planing to use dbt for transforming data for bussiness intelligence, rather than pandas.)
ª           ª       
ª           +---tickers
ª                   tickers.csv  # <---------------------------------- csv file, include 2 columns : first one contain the ticker's symbol, and the second one is the sources (it should be VCI for default)
ª                   
+---tests
        __init__.py
        


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

## Learn more

To learn more about this template and Dagster in general:

- [Dagster Documentation](https://docs.dagster.io/)
- [Dagster University](https://courses.dagster.io/)
- [Dagster Slack Community](https://dagster.io/slack)

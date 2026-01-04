from dagster import IOManager, OutputContext, InputContext, Field, io_manager, Definitions
import os
from dotenv import load_dotenv
import pandas as pd
from contextlib import contextmanager
from typing import Mapping, Any

from sqlalchemy import create_engine, text, Engine


@contextmanager
def connect_psql(config: Mapping[str, Any]):
    """Context Manager để tạo và quản lý kết nối SQLAlchemy Engine."""
    conn_info = (
            f"postgresql+psycopg2://{config['username']}:{config['password']}"
            + f"@{config['host']}:{config['port']}"
            + f"/{config['db_name']}"
    )

    db_conn = create_engine(conn_info)
    try:
        yield db_conn
    except Exception:
        print("\n this is happen \n")
        raise

    finally:
        db_conn.dispose()

class PostgresIOManager(IOManager):
    """
    PostgreSQL IOManager using Pandas DataFrame.
    Uses a persistent staging table (no TEMP tables).
    """

    def __init__(self, config: Mapping[str, Any]):
        self._config = config

    # ---------- Public methods ----------

    def handle_output(self, context: OutputContext, obj: pd.DataFrame):
        """Write DataFrame to PostgreSQL using staging + UPSERT."""
        if not isinstance(obj, pd.DataFrame):
            raise ValueError(f"Expected Pandas DataFrame, got {type(obj)}")

        schema, table = self._parse_asset_key(context.asset_key)
        table = table.lower()
        staging_table = f"{table}__staging"

        primary_keys = (context.metadata or {}).get("primary_keys", ["time"])

        with connect_psql(self._config) as db_conn:
            with db_conn.begin() as connection:

                # Ensure schema, target table, and staging table exist
                self._ensure_schema(connection, schema)
                self._ensure_target_table(connection, schema, table)
                self._ensure_staging_table(connection, schema, staging_table)

                # Clean staging table
                connection.execute(
                    text(f"TRUNCATE TABLE {schema}.{staging_table}")
                )

                # Load DataFrame into staging table
                obj.to_sql(
                    name=staging_table,
                    con=connection,
                    schema=schema,
                    if_exists="append",
                    index=False,
                )

                # Optional sanity check (safe)
                count = connection.execute(
                    text(f"SELECT COUNT(*) FROM {schema}.{staging_table}")
                ).scalar()

                context.log.info(
                    f"Loaded {count} rows into staging table {schema}.{staging_table}"
                )

                # Merge staging into target
                if primary_keys:
                    self._perform_upsert(
                        connection,
                        schema,
                        table,
                        staging_table,
                        primary_keys,
                    )
                else:
                    self._perform_insert(
                        connection,
                        schema,
                        table,
                        staging_table,
                    )

    def load_input(self, context: InputContext) -> pd.DataFrame:
        """Read full table from PostgreSQL."""
        schema, table = self._parse_asset_key(context.asset_key)

        with connect_psql(self._config) as db_conn:
            return pd.read_sql_table(
                table,
                con=db_conn,
                schema=schema,
            )

    # ---------- Helper methods ----------

    def _parse_asset_key(self, asset_key):
        """
        Parse schema and table from asset_key.
        Example: silver_raw_VNM → schema=silver, table=VNM
        """
        name = asset_key.path[-1]
        parts = name.split("_")

        return parts[0], parts[-1]

    def _ensure_schema(self, connection, schema: str):
        """Create schema if not exists."""
        connection.execute(
            text(f"CREATE SCHEMA IF NOT EXISTS {schema}")
        )

    def _ensure_target_table(self, connection, schema: str, table: str):
        """Create target table if not exists."""
        connection.execute(
            text(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{table} (
                    time TIMESTAMP PRIMARY KEY,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume BIGINT,
                    log_close DOUBLE PRECISION,
                    diff_log_close DOUBLE PRECISION
                )
            """)
        )

    def _ensure_staging_table(self, connection, schema: str, staging_table: str):
        """Create staging table if not exists."""
        connection.execute(
            text(f"""
                CREATE TABLE IF NOT EXISTS {schema}.{staging_table} (
                    time TIMESTAMP,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume BIGINT,
                    log_close DOUBLE PRECISION,
                    diff_log_close DOUBLE PRECISION
                )
            """)
        )

    def _get_table_columns(self, connection, schema: str, table: str):
        """Fetch column names from a table."""
        result = connection.execute(
            text(f"SELECT * FROM {schema}.{table} LIMIT 0")
        )
        return list(result.keys())

    def _perform_upsert(
        self,
        connection,
        schema: str,
        table: str,
        staging_table: str,
        primary_keys: list,
    ):
        """Perform UPSERT from staging table."""
        all_cols = self._get_table_columns(connection, schema, table)

        col_list = ", ".join(f'"{c}"' for c in all_cols)
        pk_list = ", ".join(primary_keys)

        update_clause = ", ".join(
            f'"{c}" = EXCLUDED."{c}"'
            for c in all_cols
            if c not in primary_keys
        )

        sql = f"""
            INSERT INTO {schema}.{table} ({col_list})
            SELECT {col_list}
            FROM {schema}.{staging_table}
            ON CONFLICT ({pk_list})
            DO UPDATE SET {update_clause}
        """

        connection.execute(text(sql))

    def _perform_insert(
        self,
        connection,
        schema: str,
        table: str,
        staging_table: str,
    ):
        """Insert from staging table without conflict handling."""
        connection.execute(
            text(f"""
                INSERT INTO {schema}.{table}
                SELECT *
                FROM {schema}.{staging_table}
            """)
        )




@io_manager(
    description=" PostgreSQL",
    config_schema={
        "user": Field(str, is_required=True, description="Postgres Username"),
        "password": Field(str, is_required=True, description="Postgres Password"),
        "host": Field(str, is_required=True, description="Postgres Host (e.g., localhost)"),
        "port": Field(int, is_required=False, default_value=5432),
        "database": Field(str, is_required=True, description="Database Name"),
    }
)
def postgres_io_manager(context):
    return PostgresIOManager(config = {
        "username":context.resource_config["user"],
        "password":context.resource_config["password"],
        "host":context.resource_config["host"],
        "port":context.resource_config["port"],
        "db_name":context.resource_config["database"],
    })


# --- 3. Load biến môi trường ---
load_dotenv()

PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", 5432))
PG_DB = os.getenv("POSTGRES_DB", "dagster_db")

# Kiểm tra biến quan trọng (Tùy chọn)
if not all([PG_USER, PG_PASSWORD, PG_HOST, PG_DB]):
    raise EnvironmentError("Thiếu biến môi trường cấu hình PostgreSQL.")

# --- 4. Definitions ---
defs = Definitions(
    # ... assets, jobs
    resources={
        # Đặt tên resource là 'io_manager' nếu muốn nó áp dụng mặc định cho toàn bộ assets
        "psql_io_manager": postgres_io_manager.configured({
            "user": PG_USER,
            "password": PG_PASSWORD,
            "host": PG_HOST,
            "port": PG_PORT,
            "database": PG_DB,
        })
    }
)
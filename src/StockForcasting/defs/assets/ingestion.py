import dagster as dg
import pandas as pd
from pathlib import Path

from fontTools.misc.plistlib import end_date
from vnstock import Vnstock  # Ensure vnstock is installed and compatible
from typing import Dict, List
from datetime import datetime, timedelta

from ..partition_defs.partition_def import monthly_partition_def

# --- Factory Function ---
def make_ingestion_asset(connect_config: Dict, asset_config: Dict) -> dg.AssetsDefinition:
    """
    Creates a Dagster asset definition from configuration.
    Returns an AssetsDefinition
    params:
        **connect_config** is used for api connections via vnstock api, it should meet the following requirements:
            {
                "symbol": <ticker's symbol> you can search for these in vnstock api documentation
                "source": you should leave it as "VCI" as default.
            }
        **asset_config** is used for configure the assets, it should meet the following requirements:
            {
            "asset_name": <asset_name>
            "key_prefix": <key_prefix>
            "kind": <kind>
            }
        these asset, as default, is put in minio's object storage.
    """

    @dg.asset(
        name=asset_config["asset_name"],
        key_prefix=asset_config["key_prefix"],
        # kinds needs to be a set of strings or simple string in newer versions
        kinds={asset_config["kind"]},
        partitions_def=monthly_partition_def,
        io_manager_key="minio_io_manager",
        group_name="Ingestion"  # Grouping for UI organization
    )
    def bronze_raw_ticker(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
        """
        Monthly partitioned stock data in the form of csv file (save in minio object storage).
        """
        stock = Vnstock().stock(**connect_config)

        # YYYY-MM-DD
        partition_date_str = context.partition_key

        start_date_obj = datetime.strptime(partition_date_str, "%Y-%m-%d").date()
        end_date_obj = start_date_obj + timedelta(days=30)

        start_date_str = start_date_obj.strftime("%Y-%m-%d")
        end_date_str = end_date_obj.strftime("%Y-%m-%d")

        # df = stock.quote.history(start=start_date_str, end=end_date_str, interval='1D')
        df = stock.quote.history(start=partition_date_str,end=end_date_str ,interval='1D')

        context.log.info(f"Loaded {len(df)} rows for {asset_config['asset_name']}")

        return dg.MaterializeResult(
            value=df,
            metadata={
                "start_date": start_date_str,
                "end_date": end_date_str,
                "row_count": len(df),
                "source": connect_config.get("source", "unknown")
            }
        )

    return bronze_raw_ticker

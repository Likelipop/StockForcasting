import dagster as dg
import pandas as pd

from typing import Dict
from ..partition_defs.partition_def import monthly_partition_def


def make_train_val_split_asset(
        connect_config: Dict,
        train_ratio: float = 0.8
) -> dg.AssetsDefinition:
    """
    Train test split using log return columns
    """

    symbol = connect_config["symbol"]

    upstream_key = dg.AssetKey(
        ["transformation", "price", f"transformation_price_{symbol}"]
    )

    @dg.multi_asset(
        name=f"train_val_split_{symbol}",
        group_name="modeling",
        ins={
            "price_asset": dg.AssetIn(key = upstream_key)
        },
        outs={
            "train_data": dg.AssetOut(
                io_manager_key="minio_io_manager",
                key=dg.AssetKey(["model", "trainData", f"model_trainData_{symbol}"]),
                description="Training dataset (full-history time-series split)",
                kinds={"minio", "pandas"}
            ),
            "val_data": dg.AssetOut(
                io_manager_key="minio_io_manager",
                key=dg.AssetKey(["model", "valData", f"model_valData_{symbol}"]),
                description="Validation dataset (full-history time-series split)",
                kinds={"minio", "pandas"}
            ),
        },

        required_resource_keys={"minio_io_manager"},
    )
    def train_val_split_asset(
            context: dg.AssetExecutionContext,
            price_asset: pd.DataFrame

    ):
        context.log.info(f" starting train/val spliting for [{symbol}] ")


        # --- 4. Time-series split ---
        n_total = len(price_asset)
        split_idx = int(n_total * train_ratio)

        train_df = price_asset.iloc[:split_idx].copy()
        val_df = price_asset.iloc[split_idx:].copy()

        context.log.info(
            f"[{symbol}] Total={n_total} | Train={len(train_df)} | Val={len(val_df)}"
        )

        return (train_df, val_df)

    return train_val_split_asset

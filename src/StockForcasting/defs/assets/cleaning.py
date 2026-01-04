from pathlib import Path
from ..partition_defs.partition_def import monthly_partition_def

import pandas as pd
import numpy as np
import dagster as dg
from typing import Dict, List

import io
import pyarrow.parquet as pq

from dagster import (
    multi_asset,
    AssetOut,
    AssetKey,
    Output,
    Failure,
)

def get_dominant_period(prices):
    """
    Trả về chu kỳ (số ngày) mạnh nhất trong chuỗi dữ liệu dựa trên FFT.
    Ví dụ: Trả về 5.0 nghĩa là chu kỳ lặp lại xu hướng là 5 ngày (Weekly pattern).
    """
    if len(prices) < 4: return 0

    # Detrend (loại bỏ xu hướng tăng giảm để chỉ còn dao động)
    detrended = prices - np.mean(prices)

    # FFT
    fft_vals = np.fft.fft(detrended)
    fft_freq = np.fft.fftfreq(len(detrended))

    # Lấy phần dương
    pos_mask = fft_freq > 0
    fft_vals = fft_vals[pos_mask]
    fft_freq = fft_freq[pos_mask]

    if len(fft_vals) == 0: return 0

    # Tìm tần số có năng lượng cao nhất
    spectral_density = np.abs(fft_vals) ** 2
    peak_idx = np.argmax(spectral_density)
    dominant_freq = fft_freq[peak_idx]

    # Chu kỳ = 1 / Tần số
    return 1.0 / dominant_freq if dominant_freq != 0 else 0

def make_clean_data_asset(connect_config: Dict, asset_config: Dict) -> dg.AssetsDefinition:
    """
    Factory function for agregating all data from START_DATE to END_DATE, cleaning it. (no partition here)
    :param
        **connect_config** is used for api connections via vnstock api, it should meet the following requirements:
        {
            "symbol": <ticker's symbol> you can search for these in vnstock api documentation
            "source": you should leave it as "VCI" as default.
        }

        **asset_config** is used for configure the assets, it should meet the following requirements:
        {
        "asset_name": <asset_name>
        "key_prefix": <key_prefix>
        "group_name": str
        }
    these asset, as default, is put in minio's object storage.
    Input: ingestion/raw/ingestion_raw_<symbol>
    Output: transformation/cleaned/transformation_cleaned_<symbol>
    """

    # Extract configuration variables
    symbol = connect_config.get("symbol")
    asset_name = asset_config.get("asset_name", f"transformation_cleaned_{symbol}")
    key_prefix = asset_config.get("key_prefix", ["transformation", "cleaned"])
    group_name = asset_config.get("group_name", "transformation")

    # Define the upstream dependency key
    upstream_key = dg.AssetKey(["ingestion", "raw", f"ingestion_raw_{symbol}"])

    @dg.asset(
        name=asset_name,
        key_prefix=key_prefix,
        group_name=group_name,
        ins={"raw_df": dg.AssetIn(key=upstream_key)},
        description="Clean it (validate value, forward fill, date-time conversion)",
        io_manager_key="minio_io_manager",
        required_resource_keys={"minio_io_manager"},
        kinds={"minio", "pandas"},
        partitions_def= monthly_partition_def
    )
    def clean_data_ticker(
            context: dg.AssetExecutionContext,
            raw_df: pd.DataFrame
    ) -> dg.MaterializeResult:




        context.log.info(f"[{symbol}] Cleaning all data (partitions: {context.partition_key}).")

        df = raw_df.copy()
        df.columns = [c.lower().strip() for c in df.columns]
        # 1. Date-Time  Conversion
        if 'time' in df.columns:
            df['time'] = pd.to_datetime(df['time'])
            df.set_index('time', inplace=True)
        elif 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)

        # 2. Sorting and Deduplication
        df.sort_index(inplace=True)
        df = df[~df.index.duplicated(keep='last')]  # Keep the latest update if dupe exists

        # 3. Numeric Validation & Conversion
        # Force these columns to numeric, coercing errors to NaNs to be handled next
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # 4. Handle Missing Data (Forward Fill as requested)
        # ffill propagates last valid observation forward
        df.ffill(inplace=True)
        # bfill handles any initial NaNs at the very start of the dataframe
        df.bfill(inplace=True)

        return dg.MaterializeResult(
            value=df,
            metadata={
                "row_count": len(df),
                "columns": str(list(df.columns)),
                "start_date": str(df.index.min()),
                "end_date": str(df.index.max())
            }
        )

    return clean_data_ticker


def make_parquet_data_asset(connect_config: Dict) -> dg.AssetsDefinition:
    """
    Multi-asset factory to split data to difference columns.
    Load each partitioned asset and split it to different columns => parquet data

    """

    symbol = connect_config.get("symbol")
    upstream_key = dg.AssetKey(["transformation", "cleaned", f"transformation_cleaned_{symbol}"])

    cols = ["open", "high", "low", "close", "volume"]

    asset_outs = {}
    for col in cols:
        asset_outs[col] = dg.AssetOut(
            key=dg.AssetKey(["transformation", col, f"transformation_{col}_{symbol}"]),
            description=f"Cleaned {col} series for {symbol}",
            group_name="transformation",
            io_manager_key="minio_io_manager"
        )

    @dg.multi_asset(
        name=f"split_ohlcv_asset_{symbol}",
        outs=asset_outs,  # <--- Thay đổi ở đây: Dùng outs thay vì specs
        ins={"cleaned_data": dg.AssetIn(key=upstream_key)},
        required_resource_keys={"minio_io_manager"}
    )
    def split_ohlcv_asset(context: dg.AssetExecutionContext,cleaned_data : pd.DataFrame):

        minio_connector = context.resources.minio_io_manager
        data_buckets = {col: [] for col in cols}
        partition_keys = context.asset_partition_keys_for_input("cleaned_data")
        partition_keys.sort()


        context.log.info(f"[{symbol}] Found {len(partition_keys)} partitions to process.")



        upstream_path_prefix = ["transformation", "cleaned", f"transformation_cleaned_{symbol}"]

        for p in partition_keys:
            object_name = f"{upstream_path_prefix}/{p}.parquet"
            try:
                df_p = minio_connector.extract_obj_to_df(where_to_fetch= ["transformation",
                                                                          "cleaned",
                                                                          f"transformation_cleaned_{symbol}/{p}"],
                                                         file_extension="parquet",)
            except Exception as e:
                raise Exception(f"Failed to load partition {p} at {object_name}")


            if len(df_p) == 0:
                raise ValueError(f"Partition {p} (Asset: {symbol}) has 0 rows! Data integrity failed.")

            df_p.columns = [c.lower().strip() for c in df_p.columns]

            for col in cols:
                if col in df_p.columns:
                    data_buckets[col].append(df_p[col].copy())
                else:
                    context.log.warning(f"Column {col} missing in partition {p}")

            del df_p

        context.log.info(f"[{symbol}] Finished iterating partitions. Concatenating...")

        for col in cols:
            if data_buckets[col]:
                final_series = pd.concat(data_buckets[col])
                final_series.sort_index(inplace=True)
                final_series = final_series[~final_series.index.duplicated(keep='last')]

                output_df = final_series.to_frame(name=col)

                yield dg.MaterializeResult(
                    asset_key=dg.AssetKey(["transformation", col, f"transformation_{col}_{symbol}"]),
                    value=output_df,
                    metadata={
                        "row_count": len(output_df),
                        "start_date": str(output_df.index.min()),
                        "end_date": str(output_df.index.max())
                    }
                )
            else:
                context.log.error(f"No data found for column {col}")

    return split_ohlcv_asset



def make_feature_asset(connect_config: Dict) -> List[dg.AssetsDefinition]:
    """
    Factory function for create an assets consist of engineered featured for models/ analytics
    These features include (Rolling mean, Lag features, Log return,...)
    parameter:

        **connect_config** is used for api connections via vnstock api, it should meet the following requirements:
        {
            "symbol": <ticker's symbol> you can search for these in vnstock api documentation
            "source": you should leave it as "VCI" as default.
        }

    """
    symbol = connect_config.get("symbol")

    def get_col_key(col_name):
        return dg.AssetKey(["transformation", col_name, f"transformation_{col_name}_{symbol}"])

    # ------------------------------------------------------------------
    # ASSET 1: Price Transformations
    # Dependencies: Cần Close, Open, High để tính toán các tỷ lệ.
    # ------------------------------------------------------------------
    @dg.asset(
        name=f"transformation_price_{symbol}",
        key_prefix=["transformation", "price"],
        group_name="transformation",
        ins={
            "close_df": dg.AssetIn(key=get_col_key("close")),
            "open_df": dg.AssetIn(key=get_col_key("open")),
            "high_df": dg.AssetIn(key=get_col_key("high")),
        },
        description="Price Transformations (Log returns, Close-to-Open, High-to-Close)",
        io_manager_key="minio_io_manager",
        required_resource_keys={"minio_io_manager"},
        kinds={"minio", "pandas"}
    )
    def price_asset(
            context: dg.AssetExecutionContext,
            close_df: pd.DataFrame,
            open_df: pd.DataFrame,
            high_df: pd.DataFrame
    ) -> dg.MaterializeResult:
        """
        Input: 3 DataFrames riêng biệt (mỗi cái 1 cột).
        Logic: Join lại theo Index (Date) rồi tính toán.
        """
        context.log.info(f"[{symbol}] Loading Close, Open, High for Price Features...")

        df_calc = pd.concat([close_df, open_df, high_df], axis=1)

        df_calc.columns = [c.lower() for c in df_calc.columns]

        if df_calc.isnull().any().any():
            context.log.warning(f"Found NaNs after merging columns. Forward filling...")
            df_calc.ffill(inplace=True)

        features = pd.DataFrame(index=df_calc.index)

        features['log_return'] = np.log(df_calc['close'] / df_calc['close'].shift(1))

        features['close_to_open'] = (df_calc['close'] - df_calc['open']) / df_calc['open']

        features['high_to_close'] = (df_calc['high'] - df_calc['close']) / df_calc['close']

        features['close_to_close'] = df_calc['close'].pct_change()

        features.dropna(inplace=True)

        return dg.MaterializeResult(
            value=features,
            metadata={
                "row_count": len(features),
                "columns": str(list(features.columns)),
                "preview": str(features.head(2).to_dict())
            }
        )

    # ------------------------------------------------------------------
    # ASSET 2: Technical Indicators
    # Dependencies: Chỉ cần Close (cho RSI, SMA, EMA).
    # ------------------------------------------------------------------
    @dg.asset(
        name=f"transformation_techindi_{symbol}",
        key_prefix=["transformation", "indicators"],
        group_name="transformation",
        ins={
            "close_df": dg.AssetIn(key=get_col_key("close"))
        },
        description="Technical indicators (RSI, SMA, EMA) based on Close price",
        io_manager_key="minio_io_manager",
        required_resource_keys={"minio_io_manager"},
        kinds={"minio", "pandas"}
    )
    def technical_indicator_asset(
            context: dg.AssetExecutionContext,
            close_df: pd.DataFrame
    ) -> dg.MaterializeResult:
        """
        Compute techinical indicators for analysis (sma, ema, rsi)
        """
        context.log.info(f"[{symbol}] Computing Technical Indicators from Close price...")

        close = close_df["close"]

        df = pd.DataFrame(index=close_df.index)

        # 1. SMA (Simple Moving Average) - Window 20
        df['SMA'] = close.rolling(window=20).mean()

        # 2. EMA (Exponential Moving Average) - Span 20
        df['EMA'] = close.ewm(span=20, adjust=False).mean()

        # 3. RSI (Relative Strength Index) - Window 14
        delta = close.diff()
        gain = delta.clip(lower=0)
        loss = -1 * delta.clip(upper=0)

        ema_gain = gain.ewm(com=13, adjust=False).mean()
        ema_loss = loss.ewm(com=13, adjust=False).mean()

        rs = ema_gain / ema_loss
        df['RSI'] = 100 - (100 / (1 + rs))

        df.dropna(inplace=True)

        return dg.MaterializeResult(
            value=df,
            metadata={
                "row_count": len(df),
                "columns": str(list(df.columns))
            }
        )

    return [price_asset, technical_indicator_asset]


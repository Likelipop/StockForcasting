from dagster import Config
import dagster as dg
import pandas as pd
from pathlib import Path
from sklearn.metrics import mean_absolute_error, mean_squared_error
import numpy as np
from typing import Dict
from statsmodels.tsa.statespace.sarimax import SARIMAX
import joblib

class SarimaConfig(Config):
    # Non-seasonal
    p: int = 1
    d: int = 0
    q: int = 1

    # Seasonal
    P: int = 0
    D: int = 0
    Q: int = 0
    m: int = 0  # season length (e.g. 5, 7, 12)

    # Train / validation split
    train_ratio: float = 0.8

    # Optimization
    enforce_stationarity: bool = False
    enforce_invertibility: bool = False

def make_sarima_training_asset(connect_config: Dict) -> dg.AssetsDefinition:
    """
    Asset train SARIMA model từ log_return.
    Config được inject trực tiếp qua function signature.
    """

    symbol = connect_config["symbol"]

    upstream_key = dg.AssetKey(["model", "trainData", f"model_trainData_{symbol}"])

    @dg.asset(
        name=f"model_SARIMA_{symbol}",
        key_prefix=["model", "SARIMA"],
        group_name="modeling",
        ins={"train_df": dg.AssetIn(key=upstream_key)},
        io_manager_key="minio_io_manager",
        kinds={"statsmodels", "pickle"},
    )
    def train_sarima_asset(
        context: dg.AssetExecutionContext,
        config: SarimaConfig,
        train_df: pd.DataFrame,
    ):

        context.log.info(
            f"[{symbol}] Training SARIMA "
            f"order=({config.p},{config.d},{config.q}) "
            f"seasonal=({config.P},{config.D},{config.Q},{config.m})"
        )

        # --- Build & fit model ---
        model = SARIMAX(
            train_df["log_return"].to_numpy(),
            order=(config.p, config.d, config.q),
            seasonal_order=(config.P, config.D, config.Q, config.m),
            enforce_stationarity=config.enforce_stationarity,
            enforce_invertibility=config.enforce_invertibility,
        )

        fitted_model = model.fit(disp=False)


        current_path_files = Path(__file__)

        model_path = (current_path_files / ".." / ".." / ".." / ".." / ".." / "data" / "SARIMA").resolve()

        print(model_path)

        joblib.dump(fitted_model, model_path)

        return dg.Output(
            fitted_model,
            metadata={
                "symbol": symbol,
                "model_path": model_path
            },
        )

    return train_sarima_asset


def make_sarima_validation_asset(connect_config: Dict) -> dg.AssetsDefinition:
    """
    Asset validate SARIMA model.
    Output: Metadata metrics only (NO dataframe).
    """
    symbol = connect_config["symbol"]

    model_key = dg.AssetKey(["model", "SARIMA", f"model_SARIMA_{symbol}"])
    data_key = dg.AssetKey(["model", "valData", f"model_valData_{symbol}"])

    @dg.asset(
        name=f"SARIMA_validation_{symbol}",
        group_name="modeling",
        ins={
            "sarima_model": dg.AssetIn(key=model_key),
            "val_data": dg.AssetIn(key=data_key),
        },
        required_resource_keys=set(),
        kinds={"statsmodels"}
    )
    def validate_sarima_asset(
        context: dg.AssetExecutionContext,
        sarima_model,
        val_data: pd.DataFrame
    ):
        context.log.info(f"[{symbol}] Validating SARIMA model...")

        n_forecast = len(val_data["log_return"])

        context.log.info(f"Train size (model): {sarima_model.nobs}")
        context.log.info(f"Validation size (input): {n_forecast}")

        if n_forecast == 0:
            raise ValueError("Validation data is empty!")

        forecast = sarima_model.forecast(steps=n_forecast)

        mae = mean_absolute_error(
            val_data["log_return"].to_numpy(),
            forecast)

        rmse = np.sqrt(mean_squared_error(
            val_data["log_return"].to_numpy(),
            forecast))

        mpe = np.mean(forecast - val_data["log_return"].to_numpy())

        context.log.info(
            f"[{symbol}] Validation done | MAE={mae:.6f} | RMSE={rmse:.6f}"
        )

        return dg.Output(
            None,
            metadata={
                "symbol": symbol,
                "val_size": n_forecast,
                "MAE": mae,
                "RMSE": float(rmse),
                "MeanPredictionError": float(mpe),
            }
        )

    return validate_sarima_asset


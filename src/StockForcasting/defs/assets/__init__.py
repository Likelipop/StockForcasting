from .ingestion import *
from .cleaning import *
from .modeling import *
from .transformation import *

def load_assets_from_csv() -> List[dg.AssetsDefinition]:
    """
    Reads CSV and generates a list of asset definitions.
    """
    # Defensive coding for path resolution
    current_dir = Path(__file__).parent
    tickers_path = current_dir.parent / "tickers" / "tickers.csv"

    if not tickers_path.exists():
        raise FileNotFoundError(f"Tickers CSV not found at: {tickers_path}")

    tickers_df = pd.read_csv(tickers_path, header=0)

    asset_list = []
    for symbol, source in zip(tickers_df.symbol, tickers_df.source):
        # make ingestion
        connect_config = {
            "symbol": symbol,
            "source": source
        }
        ingestion_asset = make_ingestion_asset(
            connect_config=connect_config,
            asset_config={
                "asset_name": f"ingestion_raw_{symbol}",
                "key_prefix": ["ingestion", "raw"],
                "kind": "minio"
            }
        )
        cleaned_asset = make_clean_data_asset(
            connect_config=connect_config,
            asset_config={
                "asset_name": f"transformation_cleaned_{symbol}",
                "key_prefix": ["transformation", "cleaned"],
                "group_name": "transformation"
            }
        )

        engineered_feature_asset = make_feature_asset(
            connect_config=connect_config
        )
        split_ohlcv_asset = make_parquet_data_asset(
            connect_config = connect_config
        )

        train_val_asset = make_train_val_split_asset(
            connect_config = connect_config
        )
        sarima_trained_asset = make_sarima_training_asset(
            connect_config=connect_config,
        )


        #make SARIMA model (validate)
        sarima_validated_asset = make_sarima_validation_asset(
            connect_config=connect_config,
        )

        asset_list.append(ingestion_asset)
        asset_list.append(cleaned_asset)
        asset_list.append(split_ohlcv_asset)
        asset_list.extend(engineered_feature_asset)
        asset_list.append(train_val_asset)
        asset_list.append(sarima_trained_asset)
        asset_list.append(sarima_validated_asset)

    return asset_list


generated_transforming_assets = load_assets_from_csv()
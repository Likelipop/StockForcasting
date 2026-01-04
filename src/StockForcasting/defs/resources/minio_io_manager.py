import pandas as pd
import pickle
import torch
import io
import os
from typing import Any, Union, List, Tuple, Optional
from datetime import datetime
from dotenv import load_dotenv

from dagster import (
    IOManager,
    io_manager,
    Field,
    MetadataValue,
    OutputContext,
    InputContext,
    Definitions
)
from minio import Minio
from minio.error import S3Error


# 1. Định nghĩa MinioIOManager
class MinioIOManager(IOManager):
    """
    Minio IOManager đa năng:
    - Hỗ trợ Pandas DataFrame (lưu .csv)
    - Hỗ trợ PyTorch Model/State Dict (lưu .pt)
    - Hỗ trợ Python Objects chung như SARIMA, Scikit-learn (lưu .pkl)
    """

    def __init__(self, endpoint: str, access_key: str, secret_key: str, secure: bool, bucket_name: str):
        self.bucket_name = bucket_name
        self.client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=secure)

        try:
            if not self.client.bucket_exists(self.bucket_name):
                self.client.make_bucket(self.bucket_name)
        except S3Error as err:
            raise Exception(f"Lỗi khi kiểm tra/tạo bucket MinIO: {err}")

    def _read_object_to_df(self, object_name: str, file_extension: str) -> pd.DataFrame:
        """Helper to read a specific object from MinIO into DataFrame"""
        response = None
        try:
            response = self.client.get_object(self.bucket_name, object_name)
            data = io.BytesIO(response.read())

            if file_extension == '.csv':
                return pd.read_csv(data)
            elif file_extension == '.parquet':
                return pd.read_parquet(data)
        except Exception as e:
            print(f"Error reading {object_name}: {e}")
            return pd.DataFrame()
        finally:
            if response:
                response.close()
                response.release_conn()
        return pd.DataFrame()

    # 2. Helper function mới: Trả về Folder path và Filename (chưa có đuôi)
    def _get_path_info(self, context: Union[InputContext, OutputContext]) -> Tuple[str, Optional[str]]:
        """
        Phân tích context để lấy đường dẫn thư mục và tên file cơ sở.

        Quy tắc Key: [<layer>, "<nhiệm vụ>", "<layer>_<nhiệm vụ>_<tên_symbol>"]
        Đường dẫn Folder: layer/nhiệm vụ/tên_symbol

        Returns:
            (directory_path, file_name_base)
            - file_name_base sẽ là partition_key hoặc timestamp (nếu là output) hoặc None (nếu là input không partition)
        """

        # 1. Xác định asset parts
        if context.has_asset_key:
            keys = context.asset_key.path
        else:
            keys = [context.name]

        # 2. Xử lý logic đường dẫn thư mục: layer/task/symbol
        # Key mong đợi: ["ingestion", "raw", "ingestion_raw_AAPL"]
        if len(keys) >= 3:
            layer = keys[0]  # ingestion
            task = keys[1]  # raw
            full_name = keys[-1]  # ingestion_raw_AAPL

            # Lấy symbol: phần cuối cùng sau dấu gạch dưới
            # ingestion_raw_AAPL -> AAPL
            symbol = full_name.split('_')[-1]

            directory_path = f"{layer}/{task}/{symbol}"
        else:
            # Fallback nếu key không đúng chuẩn 3 phần
            directory_path = "/".join(keys[:-1]) if len(keys) > 1 else "unclassified"
            if len(keys) == 1: directory_path += f"/{keys[0]}"

        # 3. Xác định tên file (chưa có extension)
        file_name_base = None

        if context.has_partition_key:
            file_name_base = context.partition_key
        elif isinstance(context, OutputContext):
            # Nếu là Output và không có partition -> Dùng thời gian thực
            file_name_base = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Lưu ý: Nếu là InputContext không có partition, file_name_base sẽ là None
        # (để logic load tự tìm file mới nhất)

        return directory_path, file_name_base

    # Helper function để nhận diện object của torch
    def _is_torch_obj(self, obj):
        if isinstance(obj, torch.nn.Module):
            return True
        if isinstance(obj, dict):
            # Kiểm tra heuristic: nếu dict chứa tensor thì coi là state_dict
            for val in obj.values():
                if isinstance(val, torch.Tensor):
                    return True
        return False

    # 3. Triển khai handle_output
    def handle_output(self, context: OutputContext, obj: Any):
        directory_path, file_name_base = self._get_path_info(context)

        buffer = io.BytesIO()
        extension = ""
        content_type = ""

        # --- Logic Phân loại dữ liệu ---
        # 1. Pandas DataFrame -> Parquet (Thay vì CSV)
        if isinstance(obj, pd.DataFrame):
            # Parquet giữ nguyên Index (quan trọng cho Time Series) và nén tốt hơn
            obj.to_parquet(buffer)
            extension = ".parquet"
            content_type = "application/octet-stream"

        # 2. PyTorch Object -> .pt
        elif isinstance(obj, (dict, torch.nn.Module)) and self._is_torch_obj(obj):
            torch.save(obj, buffer)
            extension = ".pt"
            content_type = "application/octet-stream"

        # 3. Các object khác -> Pickle (.pkl)
        else:
            pickle.dump(obj, buffer)
            extension = ".pkl"
            content_type = "application/octet-stream"

        # Tạo key hoàn chỉnh
        minio_key = f"{directory_path}/{file_name_base}{extension}"

        buffer.seek(0)
        data_bytes = buffer.getvalue()

        context.log.info(f"Đang upload ({extension}) tới MinIO: s3://{self.bucket_name}/{minio_key}")

        try:
            self.client.put_object(
                bucket_name=self.bucket_name,
                object_name=minio_key,
                data=io.BytesIO(data_bytes),
                length=len(data_bytes),
                content_type=content_type
            )

            # Tạo metadata
            metadata = {
                "path": MetadataValue.url(f"s3://{self.bucket_name}/{minio_key}"),
                "type": extension,
                "size_bytes": len(data_bytes),
                "saved_at": datetime.now().isoformat()
            }

            # Thêm thông tin row count nếu là DataFrame (tiện debug)
            if isinstance(obj, pd.DataFrame):
                metadata["row_count"] = len(obj)

            if context.metadata:
                metadata.update(context.metadata)

            context.add_output_metadata(metadata)

        except S3Error as err:
            raise Exception(f"Lỗi khi upload lên MinIO: {err}")

    def load_input(self, context: InputContext) -> Any:
        directory_path, file_name_base = self._get_path_info(context)

        # Ưu tiên tìm .parquet trước vì nó tối ưu hơn, sau đó mới fallback về csv/pt/pkl
        target_extensions = ['.parquet', '.csv', '.pt', '.pkl']
        found_obj = None

        # --- Logic Tìm File ---
        target_key = None

        if file_name_base:
            # Trường hợp 1: Có Partition Key -> Tìm chính xác file
            for ext in target_extensions:
                check_key = f"{directory_path}/{file_name_base}{ext}"
                try:
                    # stat_object nhanh hơn list_objects vì chỉ check metadata
                    self.client.stat_object(self.bucket_name, check_key)
                    target_key = check_key
                    break  # Tìm thấy file ưu tiên cao nhất thì dừng
                except S3Error:
                    continue
        else:
            # Trường hợp 2: Không có Partition Key -> Tìm file mới nhất
            context.log.info(f"Không có partition key. Đang tìm file mới nhất trong: {directory_path}")

            objects = list(self.client.list_objects(self.bucket_name, prefix=f"{directory_path}/", recursive=False))

            # Lọc các file có đuôi hợp lệ
            valid_objects = [
                obj for obj in objects
                if any(obj.object_name.endswith(ext) for ext in target_extensions)
            ]

            if valid_objects:
                # Sắp xếp theo tên (timestamp) -> Lấy file cuối cùng
                latest_obj = sorted(valid_objects, key=lambda x: x.object_name)[-1]
                target_key = latest_obj.object_name
                context.log.info(f"Đã tìm thấy file mới nhất: {target_key}")

        # --- Logic Load File ---
        if not target_key:
            raise Exception(
                f"Không tìm thấy file hợp lệ nào (.parquet/.csv/.pt/.pkl) tại: s3://{self.bucket_name}/{directory_path}/ "
                f"(Yêu cầu bởi asset {context.asset_key})"
            )

        try:
            response = self.client.get_object(self.bucket_name, target_key)
            content = response.read()
            response.close()
            response.release_conn()

            buffer = io.BytesIO(content)

            if target_key.endswith('.parquet'):
                found_obj = pd.read_parquet(buffer)
            elif target_key.endswith('.csv'):
                found_obj = pd.read_csv(buffer)
            elif target_key.endswith('.pt'):
                found_obj = torch.load(buffer, map_location=torch.device('cpu'))
            elif target_key.endswith('.pkl'):
                found_obj = pickle.load(buffer)

            return found_obj

        except Exception as err:
            raise Exception(f"Lỗi khi tải từ MinIO ({target_key}): {err}")

    def extract_to_df(self,
                      where_to_fetch: List[str],
                      start_date: str,
                      end_date: str,
                      file_extension: str = 'csv') -> pd.DataFrame:
        """
        Extracts data from MinIO bucket within a specific date range and file extension,
        returning a consolidated Pandas DataFrame.

        This method constructs the MinIO prefix based on the 'where_to_fetch' structure,
        filters files by the provided date range (assuming filenames are dates), and
        supports reading both CSV and Parquet formats.

        Args:
            where_to_fetch (List[str]): A list defining the path structure.
                Logic: Base path is all elements except the last. The last element is split by '_'
                to extract the ticker/identifier.
                Example: ["ingestion", "raw", "ingestion_raw_VNM"] -> "ingestion/raw/VNM/"
            start_date (str): The start date for filtering files (Format: 'YYYY-MM-DD').
            end_date (str): The end date for filtering files (Format: 'YYYY-MM-DD').
            file_extension (str): The file extension to filter and read.
                Accepts 'csv' or 'parquet' (case-insensitive). Defaults to 'csv'.

        Returns:
            pd.DataFrame: A DataFrame containing the concatenated data from all matching files.
                          Returns an empty DataFrame if no files are found or errors occur.
        """

        # 1. Normalize extension format (ensure it starts with '.')
        file_extension = file_extension.lower().strip()
        if not file_extension.startswith("."):
            file_extension = f".{file_extension}"

        if file_extension not in ['.csv', '.parquet']:
            print(f"Unsupported file extension: {file_extension}")
            return pd.DataFrame()

        # 2. Parse Date Range
        try:
            start_dt = datetime.strptime(start_date, "%Y-%m-%d")
            end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        except ValueError as e:
            print(f"Date format error: {e}")
            return pd.DataFrame()

        # 3. Construct MinIO Prefix (Logic preserved from original request)
        # Ex: ["ingestion", "raw", "ingestion_raw_VNM"] -> "ingestion/raw/VNM/"
        try:
            base_path = where_to_fetch[:-1]
            last_item = where_to_fetch[-1]
            ticker = last_item.split('_')[-1]  # Extract 'VNM' from 'ingestion_raw_VNM'
            file_prefix = '/'.join(base_path + [ticker]) + '/'
        except IndexError:
            print("Error constructing path from 'where_to_fetch'. Check list structure.")
            return pd.DataFrame()

        print(f"--- Start Extracting to DataFrame ---")
        print(f"Bucket: {self.bucket_name} | Prefix: {file_prefix} | Ext: {file_extension}")

        # 4. List Objects from MinIO
        try:
            objects = self.client.list_objects(bucket_name=self.bucket_name, prefix=file_prefix)
        except Exception as e:
            print(f"Error listing objects: {e}")
            return pd.DataFrame()

        data_frames = []

        # 5. Iterate and Filter Files
        for obj in objects:
            file_name = obj.object_name.split('/')[-1]

            # Filter by extension
            if not file_name.endswith(file_extension):
                continue

            try:
                # Logic: Filename is the date (e.g., "2023-01-01.csv")
                file_date_str = file_name.replace(file_extension, "")

                # Skip timestamped files (YYYYMMDD_HHMMSS) or non-date files, strictly parse YYYY-MM-DD
                try:
                    file_date = datetime.strptime(file_date_str, "%Y-%m-%d")
                except ValueError:
                    # Silently skip files that don't match the date format
                    continue

                # Check if file is within date range
                if start_dt <= file_date <= end_dt:
                    response = None
                    try:
                        response = self.client.get_object(self.bucket_name, obj.object_name)
                        file_content = io.BytesIO(response.read())

                        # Read based on extension
                        if file_extension == '.csv':
                            df = pd.read_csv(file_content)
                        elif file_extension == '.parquet':
                            df = pd.read_parquet(file_content)

                        data_frames.append(df)
                    except Exception as read_err:
                        print(f"Error reading file {file_name}: {read_err}")
                    finally:
                        if response:
                            response.close()
                            response.release_conn()

            except Exception as e:
                print(f"Skipping file {file_name}: {e}")
                continue

        if not data_frames:
            print("No matching files found.")
            return pd.DataFrame()

        # 6. Concatenate and Post-process
        full_df = pd.concat(data_frames, ignore_index=True)

        if 'time' in full_df.columns:
            full_df['time'] = pd.to_datetime(full_df['time'])

        print(f"Successfully extracted {len(full_df)} rows.")
        return full_df

    def extract_obj_to_df(self,
                          where_to_fetch: List[str],
                          file_extension: str = 'csv') -> pd.DataFrame:
        """
        Extracts data from MinIO bucket from a given "where_to_fetch + file_extension" path,
        returning a consolidated Pandas DataFrame.

        This method constructs the MinIO prefix based on the 'where_to_fetch' structure,
        filters files by the provided date range (assuming filenames are dates), and
        supports reading both CSV and Parquet formats.

        Args:
            where_to_fetch (List[str]): A list defining the path structure.
                Logic: Base path is all elements except the last. The last element is split by '_'
                to extract the ticker/identifier.
                Example: ["ingestion", "raw", "ingestion_raw_VNM"] -> "ingestion/raw/VNM/"
            file_extension (str): The file extension to filter and read.
                Accepts 'csv' or 'parquet' (case-insensitive). Defaults to 'csv'.

        Returns:
            pd.DataFrame: A DataFrame containing the concatenated data from all matching files.
                          Returns an empty DataFrame if no files are found or errors occur.
        """
        # 1. Normalize extension
        file_extension = file_extension.lower().strip()
        if not file_extension.startswith("."):
            file_extension = f".{file_extension}"

        if file_extension not in ['.csv', '.parquet']:
            print(f"Unsupported file extension: {file_extension}")
            return pd.DataFrame()

        # 2. Construct Common Base Path Logic
        try:
            # ["ingestion", "raw", "ingestion_raw_VNM"]
            base_path_list = where_to_fetch[:-1]  # ["ingestion", "raw"]
            last_item = where_to_fetch[-1]  # "ingestion_raw_VNM"
            ticker = last_item.split('_')[-1]  # "VNM"
        except IndexError:
            print("Error parsing 'where_to_fetch'. Check list structure.")
            return pd.DataFrame()

        file_path = '/'.join(base_path_list + [ticker]) + file_extension

        print(f"--- Extracting Single File ({file_path}) ---")

        df = self._read_object_to_df(file_path, file_extension)
        return df


# 6. Định nghĩa IOManager resource
@io_manager(
    description="Minio IO Manager (Partitioned & Non-Partitioned)",
    config_schema={
        "endpoint": Field(str, is_required=True),
        "access_key": Field(str, is_required=True),
        "secret_key": Field(str, is_required=True),
        "secure": Field(bool, is_required=False, default_value=False),
        "bucket_name": Field(str, is_required=True),
    }
)
def minio_io_manager(context):
    return MinioIOManager(
        endpoint=context.resource_config["endpoint"],
        access_key=context.resource_config["access_key"],
        secret_key=context.resource_config["secret_key"],
        secure=context.resource_config["secure"],
        bucket_name=context.resource_config["bucket_name"],
    )


# --- Load Environment & Definitions ---
load_dotenv()

REQUIRED_VARS = ["MINIO_ENDPOINT", "MINIO_ACCESS_KEY", "MINIO_SECRET_KEY", "MINIO_BUCKET_NAME"]
missing_vars = [var for var in REQUIRED_VARS if not os.getenv(var)]
if missing_vars:
    raise EnvironmentError(f"Thiếu các biến môi trường: {', '.join(missing_vars)}")

defs = Definitions(
    resources={
        "minio_io_manager": minio_io_manager.configured({
            "endpoint": os.getenv("MINIO_ENDPOINT"),
            "access_key": os.getenv("MINIO_ACCESS_KEY"),
            "secret_key": os.getenv("MINIO_SECRET_KEY"),
            "secure": os.getenv("MINIO_SECURE", "False").lower() in ('true', '1', 't'),
            "bucket_name": os.getenv("MINIO_BUCKET_NAME"),
        })
    }
)
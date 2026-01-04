from dagster import MonthlyPartitionsDefinition
from dotenv import load_dotenv
from pathlib import Path
import os

ENV_PATH = Path(__file__).parent.parent.parent.parent.parent / ".env"
load_dotenv(ENV_PATH)

START_DATE = os.getenv("START_DATE")
END_DATE = os.getenv("END_DATE")

monthly_partition_def = MonthlyPartitionsDefinition(start_date=START_DATE, end_date=END_DATE)

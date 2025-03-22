import sys
import pathlib
import pandas as pd
from logs.logger import logger  # Updated import for logger
from scripts.data_scrubber import DataScrubber  # noqa: E402

# Add the project root to sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Constants
DATA_DIR = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR = DATA_DIR.joinpath("raw")
PREPARED_CUSTOMERS_DIR = DATA_DIR.joinpath("prepared/customers")

def read_raw_data(file_name: str) -> pd.DataFrame:
    file_path = RAW_DATA_DIR.joinpath(file_name)
    logger.info(f"Reading raw data from {file_path}")
    return pd.read_csv(file_path)

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    PREPARED_CUSTOMERS_DIR.mkdir(parents=True, exist_ok=True)
    file_path = PREPARED_CUSTOMERS_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def prepare_customers_data():
    logger.info("Starting CUSTOMERS data preparation...")
    df_customers = read_raw_data("customers_data.csv")

    df_customers.columns = df_customers.columns.str.strip()
    df_customers = df_customers.drop_duplicates()
    df_customers['Name'] = df_customers['Name'].str.strip()
    df_customers = df_customers.dropna(subset=['CustomerID', 'Name'])

    scrubber = DataScrubber(df_customers)
    scrubber.check_data_consistency_before_cleaning()
    scrubber.inspect_data()
    df_customers = scrubber.handle_missing_data(fill_value="N/A")
    df_customers = scrubber.parse_dates_to_add_standard_datetime('JoinDate')
    scrubber.df = df_customers

    scrubber.check_data_consistency_after_cleaning()
    save_prepared_data(scrubber.df, "customers_data_prepared.csv")
    logger.info("CUSTOMERS data preparation complete.")

if __name__ == "__main__":
    prepare_customers_data()

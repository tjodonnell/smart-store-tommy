import pathlib
import pandas as pd
from utils.logger import logger
from scripts.data_scrubber import DataScrubber

# Constants
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR = DATA_DIR.joinpath("raw")
PREPARED_CUSTOMERS_DIR = DATA_DIR.joinpath("prepared/customers")

def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw customer data from CSV."""
    file_path = RAW_DATA_DIR.joinpath(file_name)
    return pd.read_csv(file_path)

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """Save cleaned customer data to CSV."""
    PREPARED_CUSTOMERS_DIR.mkdir(parents=True, exist_ok=True)
    file_path = PREPARED_CUSTOMERS_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def prepare_customers_data():
    """Prepare and clean customer data."""
    logger.info("Starting CUSTOMERS data preparation...")

    df_customers = read_raw_data("customers_data.csv")
    df_customers.columns = df_customers.columns.str.strip()
    df_customers = df_customers.drop_duplicates()
    df_customers['Name'] = df_customers['Name'].str.strip()
    df_customers = df_customers.dropna(subset=['CustomerID', 'Name'])

    scrubber_customers = DataScrubber(df_customers)
    scrubber_customers.check_data_consistency_before_cleaning()
    scrubber_customers.inspect_data()

    df_customers = scrubber_customers.handle_missing_data(fill_value="N/A")
    df_customers = scrubber_customers.parse_dates_to_add_standard_datetime('JoinDate')
    scrubber_customers.check_data_consistency_after_cleaning()

    save_prepared_data(df_customers, "customers_data_prepared.csv")
    logger.info("CUSTOMERS data preparation complete.")

if __name__ == "__main__":
    prepare_customers_data()

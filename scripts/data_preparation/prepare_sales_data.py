import pathlib
import pandas as pd
from utils.logger import logger
from scripts.data_scrubber import DataScrubber

# Constants
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR = DATA_DIR.joinpath("raw")
PREPARED_SALES_DIR = DATA_DIR.joinpath("prepared/sales")

def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw sales data from CSV."""
    file_path = RAW_DATA_DIR.joinpath(file_name)
    return pd.read_csv(file_path)

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """Save cleaned sales data to CSV."""
    PREPARED_SALES_DIR.mkdir(parents=True, exist_ok=True)
    file_path = PREPARED_SALES_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def prepare_sales_data():
    """Prepare and clean sales data."""
    logger.info("Starting SALES data preparation...")

    df_sales = read_raw_data("sales_data.csv")
    df_sales.columns = df_sales.columns.str.strip()
    df_sales = df_sales.drop_duplicates()
    df_sales['SaleDate'] = pd.to_datetime(df_sales['SaleDate'], errors='coerce')
    df_sales = df_sales.dropna(subset=['TransactionID', 'SaleDate'])

    scrubber_sales = DataScrubber(df_sales)
    scrubber_sales.check_data_consistency_before_cleaning()
    scrubber_sales.inspect_data()

    df_sales = scrubber_sales.handle_missing_data(fill_value="Unknown")
    scrubber_sales.check_data_consistency_after_cleaning()

    save_prepared_data(df_sales, "sales_data_prepared.csv")
    logger.info("SALES data preparation complete.")

if __name__ == "__main__":
    prepare_sales_data()

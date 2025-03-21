import pathlib
import pandas as pd
from utils.logger import logger
from scripts.data_scrubber import DataScrubber

# Constants
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent.parent
DATA_DIR = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR = DATA_DIR.joinpath("raw")
PREPARED_PRODUCTS_DIR = DATA_DIR.joinpath("prepared/products")

def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw product data from CSV."""
    file_path = RAW_DATA_DIR.joinpath(file_name)
    return pd.read_csv(file_path)

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """Save cleaned product data to CSV."""
    PREPARED_PRODUCTS_DIR.mkdir(parents=True, exist_ok=True)
    file_path = PREPARED_PRODUCTS_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def prepare_products_data():
    """Prepare and clean product data."""
    logger.info("Starting PRODUCTS data preparation...")

    df_products = read_raw_data("products_data.csv")
    df_products.columns = df_products.columns.str.strip()
    df_products = df_products.drop_duplicates()
    df_products['ProductName'] = df_products['ProductName'].str.strip()

    scrubber_products = DataScrubber(df_products)
    scrubber_products.check_data_consistency_before_cleaning()
    scrubber_products.inspect_data()

    scrubber_products.check_data_consistency_after_cleaning()

    save_prepared_data(df_products, "products_data_prepared.csv")
    logger.info("PRODUCTS data preparation complete.")

if __name__ == "__main__":
    prepare_products_data()

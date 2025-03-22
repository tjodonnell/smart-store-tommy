import sys
import pathlib
import pandas as pd

# Add the project root directory to Python's sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Import local modules
from utils.logger import logger  # noqa: E402
from scripts.data_scrubber import DataScrubber  # noqa: E402

# Constants
DATA_DIR: pathlib.Path = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("raw")
PREPARED_DATA_DIR: pathlib.Path = DATA_DIR.joinpath("prepared")

def read_raw_data(file_name: str) -> pd.DataFrame:
    """Read raw data from CSV."""
    file_path: pathlib.Path = RAW_DATA_DIR.joinpath(file_name)
    logger.info(f"Reading raw data from {file_path}")
    return pd.read_csv(file_path)

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    """Save cleaned data to CSV."""
    file_path: pathlib.Path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def main() -> None:
    """Main function for pre-processing sales data."""
    logger.info("======================")
    logger.info("STARTING prepare_sales_data.py")
    logger.info("======================")

    logger.info("========================")
    logger.info("Starting SALES prep")
    logger.info("========================")

    df_sales = read_raw_data("sales_data.csv")

    # Data cleaning operations
    df_sales.columns = df_sales.columns.str.strip()  # Clean column names
    df_sales = df_sales.drop_duplicates()            # Remove duplicates
    df_sales['SaleDate'] = pd.to_datetime(df_sales['SaleDate'], errors='coerce')  # Convert to datetime
    df_sales = df_sales.dropna(subset=['TransactionID', 'SaleDate'])  # Drop rows missing critical info
    
    # Scrubber operations
    scrubber_sales = DataScrubber(df_sales)
    scrubber_sales.check_data_consistency_before_cleaning()
    scrubber_sales.inspect_data()
    
    # Handle missing data and perform outlier removal
    df_sales = scrubber_sales.handle_missing_data(fill_value="Unknown")
    scrubber_sales.check_data_consistency_after_cleaning()

    # Save the prepared data
    save_prepared_data(df_sales, "sales_data_prepared.csv")
    logger.info("SALES data preparation complete.")

if __name__ == "__main__":
    main()

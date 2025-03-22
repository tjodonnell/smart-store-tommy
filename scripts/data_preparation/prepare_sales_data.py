import pathlib
import sys
import pandas as pd
from utils.logger import logger  # noqa: E402
from scripts.data_scrubber import DataScrubber  # noqa: E402

# Add the project root to sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Constants
DATA_DIR = PROJECT_ROOT.joinpath("data")
RAW_DATA_DIR = DATA_DIR.joinpath("raw")
PREPARED_SALES_DIR = DATA_DIR.joinpath("prepared/sales")

def read_raw_data(file_name: str) -> pd.DataFrame:
    file_path = RAW_DATA_DIR.joinpath(file_name)
    logger.info(f"Reading raw data from {file_path}")
    return pd.read_csv(file_path)

def save_prepared_data(df: pd.DataFrame, file_name: str) -> None:
    PREPARED_SALES_DIR.mkdir(parents=True, exist_ok=True)
    file_path = PREPARED_SALES_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")

def remove_outliers(df: pd.DataFrame, column: str) -> pd.DataFrame:
    if column in df.columns:
        mean_value = df[column].mean()
        threshold = 1.5 * mean_value
        logger.info(f"Removing outliers in column '{column}' greater than {threshold}")
        return df[df[column] <= threshold]
    else:
        logger.warning(f"Column '{column}' not found in the DataFrame. No outliers removed.")
        return df

def prepare_sales_data():
    logger.info("Starting SALES data preparation...")
    df_sales = read_raw_data("sales_data.csv")

    df_sales.columns = df_sales.columns.str.strip()
    df_sales = df_sales.drop_duplicates()
    df_sales['SaleDate'] = pd.to_datetime(df_sales['SaleDate'], errors='coerce')
    df_sales = df_sales.dropna(subset=['TransactionID', 'SaleDate'])

    scrubber = DataScrubber(df_sales)
    scrubber.check_data_consistency_before_cleaning()
    scrubber.inspect_data()
    df_sales = remove_outliers(scrubber.df, "SaleAmount")
    scrubber.df = df_sales

    scrubber.check_data_consistency_after_cleaning()
    save_prepared_data(scrubber.df, "sales_data_prepared.csv")
    logger.info("SALES data preparation complete.")

if __name__ == "__main__":
    prepare_sales_data()

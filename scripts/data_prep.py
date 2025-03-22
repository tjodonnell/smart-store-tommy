import pathlib
import sys
import pandas as pd

# For local imports, temporarily add project root to Python sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Now we can import local modules
from utils.logger import logger  # Correctly importing logger
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
    PREPARED_DATA_DIR.mkdir(parents=True, exist_ok=True)
    file_path: pathlib.Path = PREPARED_DATA_DIR.joinpath(file_name)
    df.to_csv(file_path, index=False)
    logger.info(f"Data saved to {file_path}")


def remove_outliers(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Remove outliers in a specified column where the value is greater than 1.5 times the average.

    Parameters:
        df (pd.DataFrame): DataFrame to clean.
        column (str): Column to evaluate for outliers.

    Returns:
        pd.DataFrame: Updated DataFrame with outliers removed.
    """
    if column in df.columns:
        mean_value = df[column].mean()
        threshold = 1.5 * mean_value
        logger.info(f"Removing outliers in column '{column}' greater than {threshold}")
        return df[df[column] <= threshold]
    else:
        logger.warning(f"Column '{column}' not found in the DataFrame. No outliers removed.")
        return df


def prepare_customers_data():
    """Prepare and clean customers data."""
    logger.info("========================")
    logger.info("Starting CUSTOMERS prep")
    logger.info("========================")

    df_customers = read_raw_data("customers_data.csv")
    df_customers.columns = df_customers.columns.str.strip()  # Clean column names
    df_customers = df_customers.drop_duplicates()  # Remove duplicates
    df_customers['Name'] = df_customers['Name'].str.strip()  # Trim whitespace
    df_customers = df_customers.dropna(subset=['CustomerID', 'Name'])  # Drop rows missing critical info

    scrubber_customers = DataScrubber(df_customers)
    scrubber_customers.check_data_consistency_before_cleaning()
    scrubber_customers.inspect_data()
    df_customers = scrubber_customers.handle_missing_data(fill_value="N/A")
    df_customers = scrubber_customers.parse_dates_to_add_standard_datetime('JoinDate')

    # Remove outliers for specific numeric columns if applicable
    df_customers = remove_outliers(df_customers, "CustomerID")
    scrubber_customers.df = df_customers

    scrubber_customers.check_data_consistency_after_cleaning()
    save_prepared_data(df_customers, "customers_data_prepared.csv")


def prepare_products_data():
    """Prepare and clean products data."""
    logger.info("========================")
    logger.info("Starting PRODUCTS prep")
    logger.info("========================")

    df_products = read_raw_data("products_data.csv")
    df_products.columns = df_products.columns.str.strip()  # Clean column names
    df_products = df_products.drop_duplicates()  # Remove duplicates
    df_products['ProductName'] = df_products['ProductName'].str.strip()  # Trim whitespace

    scrubber_products = DataScrubber(df_products)
    scrubber_products.check_data_consistency_before_cleaning()
    scrubber_products.inspect_data()
    scrubber_products.check_data_consistency_after_cleaning()

    # Remove outliers for specific numeric columns if applicable
    df_products = remove_outliers(df_products, "UnitPrice")
    scrubber_products.df = df_products

    save_prepared_data(df_products, "products_data_prepared.csv")


def prepare_sales_data():
    """Prepare and clean sales data."""
    logger.info("========================")
    logger.info("Starting SALES prep")
    logger.info("========================")

    df_sales = read_raw_data("sales_data.csv")
    df_sales.columns = df_sales.columns.str.strip()  # Clean column names
    df_sales = df_sales.drop_duplicates()  # Remove duplicates
    df_sales['SaleDate'] = pd.to_datetime(df_sales['SaleDate'], errors='coerce')  # Parse dates
    df_sales = df_sales.dropna(subset=['TransactionID', 'SaleDate'])  # Drop rows missing critical info

    scrubber_sales = DataScrubber(df_sales)
    scrubber_sales.check_data_consistency_before_cleaning()
    scrubber_sales.inspect_data()
    df_sales = scrubber_sales.handle_missing_data(fill_value="Unknown")

    # Remove outliers for specific numeric columns if applicable
    df_sales = remove_outliers(df_sales, "SaleAmount")
    scrubber_sales.df = df_sales

    scrubber_sales.check_data_consistency_after_cleaning()
    save_prepared_data(df_sales, "sales_data_prepared.csv")


def main():
    """Main function for processing customer, product, and sales data."""
    logger.info("======================")
    logger.info("STARTING data_prep.py")
    logger.info("======================")

    prepare_customers_data()
    prepare_products_data()
    prepare_sales_data()

    logger.info("======================")
    logger.info("FINISHED data_prep.py")
    logger.info("======================")


if __name__ == "__main__":
    main()

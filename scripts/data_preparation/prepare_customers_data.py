import sys
import pathlib
import pandas as pd  # External library import

# Add the project root directory to Python's sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Now we can import local modules
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

def remove_outliers(df: pd.DataFrame, column: str) -> pd.DataFrame:
    """
    Remove extreme values from a numeric column using the IQR method.

    Parameters:
        df (pd.DataFrame): DataFrame to clean.
        column (str): Column to evaluate for extreme values.

    Returns:
        pd.DataFrame: Updated DataFrame without extreme values.
    """
    if column in df.columns:
        Q1 = df[column].quantile(0.25)  # First quartile (25th percentile)
        Q3 = df[column].quantile(0.75)  # Third quartile (75th percentile)
        IQR = Q3 - Q1  # Interquartile range
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR

        logger.info(f"Removing outliers in column '{column}': Lower bound = {lower_bound}, Upper bound = {upper_bound}")
        df = df[(df[column] >= lower_bound) & (df[column] <= upper_bound)]
    else:
        logger.warning(f"Column '{column}' not found in the DataFrame. No outliers removed.")
    return df

def main() -> None:
    """Main function for pre-processing customers data."""
    logger.info("======================")
    logger.info("STARTING prepare_customers_data.py")
    logger.info("======================")

    logger.info("========================")
    logger.info("Starting CUSTOMERS prep")
    logger.info("========================")

    df_customers = read_raw_data("customers_data.csv")

    # Data cleaning operations
    df_customers.columns = df_customers.columns.str.strip()  # Clean column names
    df_customers = df_customers.drop_duplicates()            # Remove duplicates
    df_customers['Name'] = df_customers['Name'].str.strip()  # Trim whitespace from column values
    df_customers = df_customers.dropna(subset=['CustomerID', 'Name'])  # Drop rows missing critical info
    
    # Remove outliers in a numeric column (example: 'CustomerID')
    df_customers = remove_outliers(df_customers, "CustomerID")
    
    # Scrubber operations
    scrubber_customers = DataScrubber(df_customers)
    scrubber_customers.check_data_consistency_before_cleaning()
    scrubber_customers.inspect_data()
    
    df_customers = scrubber_customers.handle_missing_data(fill_value="N/A")
    df_customers = scrubber_customers.parse_dates_to_add_standard_datetime('JoinDate')
    scrubber_customers.check_data_consistency_after_cleaning()

    # Save the prepared data
    save_prepared_data(df_customers, "customers_data_prepared.csv")
    logger.info("CUSTOMERS data preparation complete.")

if __name__ == "__main__":
    main()
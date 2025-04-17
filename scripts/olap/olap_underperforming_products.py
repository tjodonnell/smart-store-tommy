"""
Module 6: OLAP Goal Script (uses cubed results)
File: scripts/olap_underperforming_products.py

GOAL: Identify underperforming products during slow months.

ACTION: Use this information to target promotions and improve product performance.

PROCESS:
1. Load the OLAP cube to get sales data by Month and ProductID.
2. Filter the data for slow months.
3. Group by Month and ProductID to calculate total sales and transaction count.
4. Sort the results to identify underperforming products.
"""

import pandas as pd
import pathlib
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# Constants
OLAP_OUTPUT_DIR: pathlib.Path = pathlib.Path("data").joinpath("olap_cubing_outputs")
CUBED_FILE: pathlib.Path = OLAP_OUTPUT_DIR.joinpath("multidimensional_olap_cube.csv")
PRODUCTS_FILE: pathlib.Path = pathlib.Path("data").joinpath("prepared").joinpath("products_data_prepared.csv")
RESULTS_OUTPUT_DIR: pathlib.Path = pathlib.Path("data").joinpath("results")

# Create output directory for results if it doesn't exist
RESULTS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def load_olap_cube(file_path: pathlib.Path) -> pd.DataFrame:
    """Load the precomputed OLAP cube data."""
    try:
        cube_df = pd.read_csv(file_path)
        logger.info(f"OLAP cube data successfully loaded from {file_path}.")
        return cube_df
    except Exception as e:
        logger.error(f"Error loading OLAP cube data: {e}")
        raise


def load_products_data(file_path: pathlib.Path) -> pd.DataFrame:
    """Load the product details data."""
    try:
        products_df = pd.read_csv(file_path)
        logger.info(f"Products data successfully loaded from {file_path}.")
        return products_df
    except Exception as e:
        logger.error(f"Error loading products data: {e}")
        raise


def analyze_underperforming_products(cube_df: pd.DataFrame, products_df: pd.DataFrame, slow_months: list) -> pd.DataFrame:
    """Identify underperforming products during slow months."""
    try:
        # Filter the data for slow months
        filtered_data = cube_df[cube_df["Month"].isin(slow_months)]

        # Group by Month and ProductID, calculate total sales and transaction count
        grouped = filtered_data.groupby(["Month", "ProductID"]).agg(
            TotalSales=("SaleAmount_sum", "sum"),
            TransactionCount=("TransactionID_count", "sum")
        ).reset_index()

        # Merge with product details to get ProductName
        merged_data = grouped.merge(products_df[["ProductID", "ProductName"]], on="ProductID", how="left")

        # Sort by TotalSales in ascending order to identify underperforming products
        sorted_data = merged_data.sort_values(by="TotalSales", ascending=True)

        logger.info("Underperforming products analysis completed successfully.")
        return sorted_data
    except Exception as e:
        logger.error(f"Error analyzing underperforming products: {e}")
        raise


def save_results_to_csv(results_df: pd.DataFrame, filename: str) -> None:
    """Save the results to a CSV file."""
    try:
        output_path = RESULTS_OUTPUT_DIR.joinpath(filename)
        results_df.to_csv(output_path, index=False)
        logger.info(f"Results saved to {output_path}.")
    except Exception as e:
        logger.error(f"Error saving results to CSV file: {e}")
        raise


def main():
    """Main function for analyzing underperforming products."""
    logger.info("Starting UNDERPERFORMING_PRODUCTS analysis...")

    # Step 1: Load the precomputed OLAP cube
    cube_df = load_olap_cube(CUBED_FILE)

    # Step 2: Load the product details
    products_df = load_products_data(PRODUCTS_FILE)

    # Step 3: Define slow months (e.g., months with the lowest total sales)
    slow_months = [3, 10]  # Example: march and october

    # Step 4: Analyze underperforming products during slow months
    underperforming_products = analyze_underperforming_products(cube_df, products_df, slow_months)
    print(underperforming_products)

    # Step 5: Save the results to a CSV file
    save_results_to_csv(underperforming_products, "underperforming_products.csv")

    logger.info("Analysis completed successfully.")


if __name__ == "__main__":
    main()
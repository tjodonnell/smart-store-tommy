"""
Module 7: OLAP Goal Script
File: scripts/olap_least_and_best_months_by_region.py

GOAL: Identify the least and best performing months in terms of sales for each region.

ACTION: Use this information to target marketing campaigns during slow months and optimize inventory during peak months.

PROCESS:
1. Load the OLAP cube to get sales data by Month and Region.
2. Merge the OLAP cube with customer data to include Region.
3. Group by Region and Month to calculate total sales.
4. Identify the least and best performing months for each region.
5. Save the results to a CSV file and optionally visualize the data.
"""

import pandas as pd
import pathlib
import logging
import calendar

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# Constants
OLAP_OUTPUT_DIR: pathlib.Path = pathlib.Path("data").joinpath("olap_cubing_outputs")
CUBED_FILE: pathlib.Path = OLAP_OUTPUT_DIR.joinpath("multidimensional_olap_cube.csv")
CUSTOMERS_FILE: pathlib.Path = pathlib.Path("data").joinpath("prepared").joinpath("customers_data_prepared.csv")
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


def load_customers_data(file_path: pathlib.Path) -> pd.DataFrame:
    """Load the customer details data."""
    try:
        customers_df = pd.read_csv(file_path)
        logger.info(f"Customers data successfully loaded from {file_path}.")
        return customers_df
    except Exception as e:
        logger.error(f"Error loading customers data: {e}")
        raise


def analyze_least_and_best_performing_months_by_region(cube_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    """Analyze the least and best performing months by region."""
    try:
        # Merge the OLAP cube with customer data to include Region
        merged_data = cube_df.merge(customers_df[["CustomerID", "Region"]], on="CustomerID", how="left")

        # Group by Region and Month, sum the sales
        grouped = merged_data.groupby(["Region", "Month"])["SaleAmount_sum"].sum().reset_index()

        # Identify the least and best performing months for each region
        least_performing = grouped.sort_values(["Region", "SaleAmount_sum"]).groupby("Region").first().reset_index()
        best_performing = grouped.sort_values(["Region", "SaleAmount_sum"], ascending=False).groupby("Region").first().reset_index()

        # Merge least and best performing data
        result = least_performing.merge(
            best_performing,
            on="Region",
            suffixes=("_Least", "_Best")
        )

        # Rename columns for clarity
        result.rename(
            columns={
                "Month_Least": "LeastPerformingMonth",
                "SaleAmount_sum_Least": "LeastPerformingSales",
                "Month_Best": "BestPerformingMonth",
                "SaleAmount_sum_Best": "BestPerformingSales",
            },
            inplace=True,
        )

        # Convert numeric months to month names
        result["LeastPerformingMonth"] = result["LeastPerformingMonth"].apply(lambda x: calendar.month_name[x])
        result["BestPerformingMonth"] = result["BestPerformingMonth"].apply(lambda x: calendar.month_name[x])

        logger.info("Least and best performing months by region analysis completed successfully.")
        return result
    except Exception as e:
        logger.error(f"Error analyzing least and best performing months by region: {e}")
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
    """Main function for analyzing least and best performing months by region."""
    logger.info("Starting LEAST_AND_BEST_PERFORMING_MONTHS_BY_REGION analysis...")

    # Step 1: Load the precomputed OLAP cube
    cube_df = load_olap_cube(CUBED_FILE)

    # Step 2: Load the customer details
    customers_df = load_customers_data(CUSTOMERS_FILE)

    # Step 3: Analyze least and best performing months by region
    results = analyze_least_and_best_performing_months_by_region(cube_df, customers_df)
    print(results)

    # Step 4: Save the results to a CSV file
    save_results_to_csv(results, "least_and_best_performing_months_by_region.csv")

    logger.info("Analysis completed successfully.")


if __name__ == "__main__":
    main()
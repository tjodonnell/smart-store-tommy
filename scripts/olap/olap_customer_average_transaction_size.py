"""
Module 6: OLAP Goal Script (uses cubed results)
File: scripts/olap_customer_average_transaction_size.py

This script calculates the average transaction size for each customer.

GOAL: Calculate the average transaction size for each customer.

ACTION: Use this information to inform upselling strategies or customer segmentation.

PROCESS:
1. Group transactions by CustomerID.
2. Sum SaleAmount for each customer.
3. Divide the total sales by the number of transactions to calculate the average transaction size per customer.
"""

import pandas as pd
import pathlib
import logging
import matplotlib.pyplot as plt
import seaborn as sns

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# Constants
OLAP_OUTPUT_DIR: pathlib.Path = pathlib.Path("data").joinpath("olap_cubing_outputs")
CUBED_FILE: pathlib.Path = OLAP_OUTPUT_DIR.joinpath("multidimensional_olap_cube.csv")
RESULTS_OUTPUT_DIR: pathlib.Path = pathlib.Path("data").joinpath("results")

# Create output directory for results if it doesn't exist
RESULTS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# Load the results
customer_stats = pd.read_csv("c:/Projects/smart-store-tommy/data/results/customer_average_transaction_size.csv")

# Sort by AverageTransactionSize for better visualization
customer_stats = customer_stats.sort_values(by="AverageTransactionSize", ascending=False)

# Create the bar chart
plt.figure(figsize=(12, 6))
plt.bar(customer_stats["CustomerID"].astype(str), customer_stats["AverageTransactionSize"], color="skyblue")
plt.title("Average Transaction Size by Customer", fontsize=16)
plt.xlabel("Customer ID", fontsize=12)
plt.ylabel("Average Transaction Size (USD)", fontsize=12)
plt.xticks(rotation=45, fontsize=10)
plt.tight_layout()

# Save and show the chart
plt.savefig("c:/Projects/smart-store-tommy/data/results/average_transaction_size_by_customer.png")
plt.show()

def load_olap_cube(file_path: pathlib.Path) -> pd.DataFrame:
    """Load the precomputed OLAP cube data."""
    try:
        cube_df = pd.read_csv(file_path)
        logger.info(f"OLAP cube data successfully loaded from {file_path}.")
        return cube_df
    except Exception as e:
        logger.error(f"Error loading OLAP cube data: {e}")
        raise

def calculate_average_transaction_size(cube_df: pd.DataFrame) -> pd.DataFrame:
    """Calculate the average transaction size for each customer."""
    try:
        # Group by CustomerID and calculate total sales and transaction count
        customer_stats = cube_df.groupby("CustomerID").agg(
            TotalSales=("SaleAmount_sum", "sum"),
            TransactionCount=("TransactionID_count", "sum")
        ).reset_index()

        # Calculate the average transaction size
        customer_stats["AverageTransactionSize"] = customer_stats["TotalSales"] / customer_stats["TransactionCount"]

        logger.info("Average transaction size calculated for each customer.")
        return customer_stats
    except Exception as e:
        logger.error(f"Error calculating average transaction size: {e}")
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
    """Main function for calculating average transaction size."""
    logger.info("Starting CUSTOMER_AVERAGE_TRANSACTION_SIZE analysis...")

    # Step 1: Load the precomputed OLAP cube
    cube_df = load_olap_cube(CUBED_FILE)

    # Step 2: Calculate the average transaction size for each customer
    customer_stats = calculate_average_transaction_size(cube_df)

    # Step 3: Save the results to a CSV file
    save_results_to_csv(customer_stats, "customer_average_transaction_size.csv")

    logger.info("Analysis completed successfully.")


if __name__ == "__main__":
    main()
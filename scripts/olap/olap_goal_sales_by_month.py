"""
Module 6: OLAP Goal Script (uses cubed results)
File: scripts/olap_goal_sales_by_month.py

GOAL: Analyze sales data to determine total revenue by month.

ACTION: Use this information to identify seasonal trends and plan inventory or promotions.

PROCESS:
1. Group transactions by Month.
2. Sum SaleAmount for each month.
3. Visualize total sales by month using a line graph.
"""

import pandas as pd
import matplotlib.pyplot as plt
import pathlib
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger(__name__)

# Constants
OLAP_OUTPUT_DIR: pathlib.Path = pathlib.Path("data").joinpath("olap_cubing_outputs")
CUBED_FILE: pathlib.Path = OLAP_OUTPUT_DIR.joinpath("multidimensional_olap_cube.csv")
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


def analyze_sales_by_month(cube_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate total sales by Month."""
    try:
        # Ensure the Month column exists
        if "Month" not in cube_df.columns:
            logger.error("The OLAP cube does not contain a 'Month' column.")
            raise ValueError("The OLAP cube does not contain a 'Month' column.")

        # Group by Month and sum the sales
        sales_by_month = cube_df.groupby("Month")["SaleAmount_sum"].sum().reset_index()
        sales_by_month.rename(columns={"SaleAmount_sum": "TotalSales"}, inplace=True)
        sales_by_month.sort_values(by="Month", inplace=True)
        logger.info("Sales aggregated by Month successfully.")
        return sales_by_month
    except Exception as e:
        logger.error(f"Error analyzing sales by Month: {e}")
        raise


def visualize_sales_by_month(sales_by_month: pd.DataFrame) -> None:
    """Visualize total sales by month using a line graph."""
    try:
        # Create the line graph
        plt.figure(figsize=(10, 6))
        plt.plot(sales_by_month["Month"], sales_by_month["TotalSales"], marker="o", color="blue", linestyle="-")
        plt.title("Total Sales by Month", fontsize=16)
        plt.xlabel("Month", fontsize=12)
        plt.ylabel("Total Sales (USD)", fontsize=12)
        plt.xticks(sales_by_month["Month"], fontsize=10)
        plt.grid(True)
        plt.tight_layout()

        # Save the visualization
        output_path = RESULTS_OUTPUT_DIR.joinpath("sales_by_month.png")
        plt.savefig(output_path)
        logger.info(f"Line graph saved to {output_path}.")
        plt.show()
    except Exception as e:
        logger.error(f"Error visualizing sales by Month: {e}")
        raise


def main():
    """Main function for analyzing and visualizing sales by month."""
    logger.info("Starting SALES_BY_MONTH analysis...")

    # Step 1: Load the precomputed OLAP cube
    cube_df = load_olap_cube(CUBED_FILE)

    # Step 2: Analyze total sales by Month
    sales_by_month = analyze_sales_by_month(cube_df)
    print(sales_by_month)

    # Step 3: Visualize total sales by Month
    visualize_sales_by_month(sales_by_month)

    logger.info("Analysis and visualization completed successfully.")


if __name__ == "__main__":
    main()
"""
Module 7: OLAP Goal Script
File: scripts/olap_sales_by_month_and_region.py

GOAL: Create a bar graph showing total sales by month with regions as the legend.

ACTION: Use this information to identify regional sales trends over the months.

PROCESS:
1. Load the OLAP cube to get sales data by Month and CustomerID.
2. Load the customer details to get Region.
3. Merge the data on CustomerID.
4. Group by Month and Region to calculate total sales.
5. Create a bar graph with months on the X-axis, total sales on the Y-axis, and regions as the legend.
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


def analyze_sales_by_month_and_region(cube_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    """Analyze total sales by Month and Region."""
    try:
        # Merge the OLAP cube with customer data to get Region
        merged_data = cube_df.merge(customers_df[["CustomerID", "Region"]], on="CustomerID", how="left")

        # Group by Month and Region, sum the sales
        grouped = merged_data.groupby(["Month", "Region"])["SaleAmount_sum"].sum().reset_index()

        logger.info("Sales by month and region analysis completed successfully.")
        return grouped
    except Exception as e:
        logger.error(f"Error analyzing sales by month and region: {e}")
        raise


def visualize_sales_by_month_and_region(grouped_data: pd.DataFrame) -> None:
    """Visualize total sales by month and region using a line graph."""
    try:
        # Pivot the data to organize sales by Month and Region
        sales_pivot = grouped_data.pivot(index="Month", columns="Region", values="SaleAmount_sum").fillna(0)

        # Plot the line graph
        sales_pivot.plot(kind="line", figsize=(12, 8), marker='o', colormap="tab10")

        plt.title("Total Sales by Month and Region", fontsize=16)
        plt.xlabel("Month", fontsize=12)
        plt.ylabel("Total Sales (USD)", fontsize=12)
        plt.xticks(ticks=range(1, 13), labels=["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], rotation=0)
        plt.legend(title="Region", bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.grid(axis="y", linestyle="--", alpha=0.7)
        plt.tight_layout()

        # Save the visualization
        output_path = RESULTS_OUTPUT_DIR.joinpath("sales_by_month_and_region_line_graph.png")
        plt.savefig(output_path)
        logger.info(f"Line graph saved to {output_path}.")
        plt.show()
    except Exception as e:
        logger.error(f"Error visualizing sales by month and region: {e}")
        raise

def main():
    """Main function for analyzing and visualizing sales by month and region."""
    logger.info("Starting SALES_BY_MONTH_AND_REGION analysis...")

    # Step 1: Load the precomputed OLAP cube
    cube_df = load_olap_cube(CUBED_FILE)

    # Step 2: Load the customer details
    customers_df = load_customers_data(CUSTOMERS_FILE)

    # Step 3: Analyze sales by month and region
    grouped_data = analyze_sales_by_month_and_region(cube_df, customers_df)
    print(grouped_data)

    # Step 4: Visualize the results
    visualize_sales_by_month_and_region(grouped_data)

    logger.info("Analysis and visualization completed successfully.")


if __name__ == "__main__":
    main()
"""
Module 7: OLAP Goal Script
File: scripts/olap_product_sales_by_region_line_chart.py

GOAL: Create individual line charts for each region, showing product sales by month.

ACTION: Use this information to identify product trends in each region over time.

PROCESS:
1. Load the OLAP cube to get sales data by Month, Region, and ProductID.
2. Merge the data with product details to include ProductName.
3. Filter the data by region.
4. Create individual line charts for each region with products as the legend.
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


def analyze_product_sales_by_region(cube_df: pd.DataFrame, products_df: pd.DataFrame) -> dict:
    """Analyze product sales for each region by month."""
    try:
        # Merge the OLAP cube with product details to include ProductName
        merged_data = cube_df.merge(products_df[["ProductID", "ProductName"]], on="ProductID", how="left")

        # Group by Region, Month, and ProductName, and calculate total sales
        grouped = merged_data.groupby(["Region", "Month", "ProductName"])["SaleAmount_sum"].sum().reset_index()

        # Organize data by region
        region_data = {}
        for region in grouped["Region"].unique():
            region_data[region] = grouped[grouped["Region"] == region]

        logger.info("Product sales analysis by region completed successfully.")
        return region_data
    except Exception as e:
        logger.error(f"Error analyzing product sales by region: {e}")
        raise


def visualize_product_sales_by_region(region_data: dict) -> None:
    """Visualize product sales for each region using line charts."""
    try:
        for region, data in region_data.items():
            # Pivot the data to organize sales by Month and ProductName
            sales_pivot = data.pivot(index="Month", columns="ProductName", values="SaleAmount_sum").fillna(0)

            # Plot the line chart
            sales_pivot.plot(kind="line", figsize=(12, 8), marker='o', colormap="tab10")

            # Add labels and title
            plt.title(f"Product Sales by Month in {region}", fontsize=16)
            plt.xlabel("Month", fontsize=12)
            plt.ylabel("Total Sales (USD)", fontsize=12)
            plt.xticks(ticks=range(1, 13), labels=["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"], rotation=0)
            plt.legend(title="Product", bbox_to_anchor=(1.05, 1), loc="upper left")
            plt.grid(axis="y", linestyle="--", alpha=0.7)
            plt.tight_layout()

            # Save the chart
            output_path = RESULTS_OUTPUT_DIR.joinpath(f"product_sales_{region.lower()}_line_chart.png")
            plt.savefig(output_path)
            logger.info(f"Line chart saved to {output_path}.")
            plt.show()
    except Exception as e:
        logger.error(f"Error visualizing product sales by region: {e}")
        raise


def main():
    """Main function for analyzing and visualizing product sales by region."""
    logger.info("Starting PRODUCT_SALES_BY_REGION analysis...")

    # Step 1: Load the precomputed OLAP cube
    cube_df = load_olap_cube(CUBED_FILE)

    # Step 2: Load the product details
    products_df = load_products_data(PRODUCTS_FILE)

    # Step 3: Analyze product sales by region
    region_data = analyze_product_sales_by_region(cube_df, products_df)

    # Step 4: Visualize the results
    visualize_product_sales_by_region(region_data)

    logger.info("Analysis and visualization completed successfully.")


if __name__ == "__main__":
    main()
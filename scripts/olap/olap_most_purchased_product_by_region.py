"""
Module 7: OLAP Goal Script
File: scripts/olap_most_purchased_product_by_region.py

GOAL: Create a stacked bar chart showing the most purchased products by region over the course of the year.

ACTION: Use this information to identify regional product preferences and optimize inventory and marketing strategies.

PROCESS:
1. Load the OLAP cube to get sales data by ProductID and Region.
2. Merge the data with product details to get ProductName.
3. Group by Region and ProductID to calculate total sales.
4. Create a stacked bar chart with regions on the X-axis, total sales on the Y-axis, and products as the stacks.
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


def load_products_data(file_path: pathlib.Path) -> pd.DataFrame:
    """Load the product details data."""
    try:
        products_df = pd.read_csv(file_path)
        logger.info(f"Products data successfully loaded from {file_path}.")
        return products_df
    except Exception as e:
        logger.error(f"Error loading products data: {e}")
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


def analyze_most_purchased_products_by_region(cube_df: pd.DataFrame, products_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    """Analyze the most purchased products by region."""
    try:
        # Merge the OLAP cube with customer data to get Region
        merged_data = cube_df.merge(customers_df[["CustomerID", "Region"]], on="CustomerID", how="left")

        # Merge with product details to get ProductName
        merged_data = merged_data.merge(products_df[["ProductID", "ProductName"]], on="ProductID", how="left")

        # Group by Region and ProductName, sum the sales
        grouped = merged_data.groupby(["Region", "ProductName"])["SaleAmount_sum"].sum().reset_index()

        logger.info("Most purchased products by region analysis completed successfully.")
        return grouped
    except Exception as e:
        logger.error(f"Error analyzing most purchased products by region: {e}")
        raise


def visualize_most_purchased_products_by_region(grouped_data: pd.DataFrame) -> None:
    """Visualize the most purchased products by region using a stacked bar chart."""
    try:
        # Pivot the data to organize sales by Region and ProductName
        sales_pivot = grouped_data.pivot(index="Region", columns="ProductName", values="SaleAmount_sum").fillna(0)

        # Plot the stacked bar chart
        sales_pivot.plot(kind="bar", stacked=True, figsize=(12, 8), colormap="tab10")

        plt.title("Most Purchased Products by Region", fontsize=16)
        plt.xlabel("Region", fontsize=12)
        plt.ylabel("Total Sales (USD)", fontsize=12)
        plt.xticks(rotation=45)
        plt.legend(title="Product Name", bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.tight_layout()

        # Save the visualization
        output_path = RESULTS_OUTPUT_DIR.joinpath("most_purchased_products_by_region.png")
        plt.savefig(output_path)
        logger.info(f"Stacked bar chart saved to {output_path}.")
        plt.show()
    except Exception as e:
        logger.error(f"Error visualizing most purchased products by region: {e}")
        raise


def main():
    """Main function for analyzing and visualizing most purchased products by region."""
    logger.info("Starting MOST_PURCHASED_PRODUCTS_BY_REGION analysis...")

    # Step 1: Load the precomputed OLAP cube
    cube_df = load_olap_cube(CUBED_FILE)

    # Step 2: Load the product details
    products_df = load_products_data(PRODUCTS_FILE)

    # Step 3: Load the customer details
    customers_df = load_customers_data(CUSTOMERS_FILE)

    # Step 4: Analyze most purchased products by region
    grouped_data = analyze_most_purchased_products_by_region(cube_df, products_df, customers_df)
    print(grouped_data)

    # Step 5: Visualize the results
    visualize_most_purchased_products_by_region(grouped_data)

    logger.info("Analysis and visualization completed successfully.")


if __name__ == "__main__":
    main()
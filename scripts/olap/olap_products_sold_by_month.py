"""
Module 6: OLAP Goal Script (uses cubed results)
File: scripts/olap_products_sold_by_month.py

GOAL: Visualize total sales for each product by month using a stacked column chart.

ACTION: Use this information to identify product performance trends over time.

PROCESS:
1. Load the OLAP cube to get sales data by Month and ProductID.
2. Load the product details to get ProductName.
3. Merge the data on ProductID.
4. Create a stacked column chart to show total sales by product for each month.
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


def analyze_products_sold_by_month(cube_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    """Analyze total sales by Month and ProductID."""
    try:
        # Group by Month and ProductID, sum the sales
        grouped = cube_df.groupby(["Month", "ProductID"])["SaleAmount_sum"].sum().reset_index()
        grouped.rename(columns={"SaleAmount_sum": "TotalSales"}, inplace=True)

        # Merge with product details to get ProductName
        merged_data = grouped.merge(products_df[["ProductID", "ProductName"]], on="ProductID", how="left")

        logger.info("Sales by month and product analysis completed successfully.")
        return merged_data
    except Exception as e:
        logger.error(f"Error analyzing sales by month and product: {e}")
        raise

def visualize_products_sold_by_month(merged_data: pd.DataFrame) -> None:
    """Visualize total sales by month and product using a line graph."""
    try:
        # Pivot the data to organize sales by Month and ProductName
        sales_pivot = merged_data.pivot_table(
            index="Month",
            columns="ProductName",
            values="TotalSales",
            aggfunc="sum",
            fill_value=0
        )

        # Plot the line graph
        plt.figure(figsize=(12, 8))
        for product in sales_pivot.columns:
            plt.plot(sales_pivot.index, sales_pivot[product], marker="o", label=product)

        plt.title("Total Sales by Month and Product", fontsize=16)
        plt.xlabel("Month", fontsize=12)
        plt.ylabel("Total Sales (USD)", fontsize=12)
        plt.xticks(sales_pivot.index, fontsize=10)
        plt.legend(title="Product Name", bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.grid(True)
        plt.tight_layout()

        # Save the visualization
        output_path = RESULTS_OUTPUT_DIR.joinpath("products_sold_by_month_line_graph.png")
        plt.savefig(output_path)
        logger.info(f"Line graph saved to {output_path}.")
        plt.show()
    except Exception as e:
        logger.error(f"Error visualizing products sold by month: {e}")
        raise

def main():
    """Main function for analyzing and visualizing products sold by month."""
    logger.info("Starting PRODUCTS_SOLD_BY_MONTH analysis...")

    # Step 1: Load the precomputed OLAP cube
    cube_df = load_olap_cube(CUBED_FILE)

    # Step 2: Load the product details
    products_df = load_products_data(PRODUCTS_FILE)

    # Step 3: Analyze sales by Month and ProductID
    merged_data = analyze_products_sold_by_month(cube_df, products_df)
    print(merged_data)

    # Step 4: Visualize the results
    visualize_products_sold_by_month(merged_data)

    logger.info("Analysis and visualization completed successfully.")


if __name__ == "__main__":
    main()
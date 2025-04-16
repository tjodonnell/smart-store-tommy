"""
Module 6: OLAP Goal Script (uses cubed results)
File: scripts/olap_goals_top_product_by_weekday.py

This script uses our precomputed cubed data set to get the information 
we need to answer a specific business goal. 

GOAL: Analyze sales data to determine the product with the highest revenue 
for each day of the week. 

ACTION: This can help inform inventory decisions, optimize promotions, 
and understand purchasing patterns on different days.

PROCESS: 
Group transactions by the day of the week and product.
Sum SaleAmount for each product on each day.
Identify the top product for each day based on total revenue.
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


def analyze_top_product_by_weekday(cube_df: pd.DataFrame, products_df: pd.DataFrame) -> pd.DataFrame:
    """Identify the product with the highest revenue for each day of the week."""
    try:
        # Group by DayOfWeek and ProductID, sum the sales
        grouped = cube_df.groupby(["DayOfWeek", "ProductID"])["SaleAmount_sum"].sum().reset_index()
        grouped.rename(columns={"SaleAmount_sum": "TotalSales"}, inplace=True)

        # Merge with product details to get ProductName
        grouped = grouped.merge(products_df[["ProductID", "ProductName"]], on="ProductID", how="left")

        # Sort within each day to find the top product
        top_products = grouped.sort_values(["DayOfWeek", "TotalSales"], ascending=[True, False]).groupby("DayOfWeek").head(1)
        logger.info("Top products identified for each day of the week.")
        return top_products
    except Exception as e:
        logger.error(f"Error analyzing top product by DayOfWeek: {e}")
        raise


def visualize_sales_by_weekday_and_product(cube_df: pd.DataFrame, products_df: pd.DataFrame) -> None:
    """Visualize total sales by day of the week, broken down by product."""
    try:
        # Merge with product details to get ProductName
        cube_df = cube_df.merge(products_df[["ProductID", "ProductName"]], on="ProductID", how="left")

        # Pivot the data to organize sales by DayOfWeek and ProductName
        sales_pivot = cube_df.pivot_table(
            index="DayOfWeek",
            columns="ProductName",
            values="SaleAmount_sum",
            aggfunc="sum",
            fill_value=0
        )

        # Plot the stacked bar chart
        sales_pivot.plot(
            kind="bar",
            stacked=True,
            figsize=(12, 8),
            colormap="tab10"
        )

        plt.title("Total Sales by Day of the Week and Product", fontsize=16)
        plt.xlabel("Day of the Week", fontsize=12)
        plt.ylabel("Total Sales (USD)", fontsize=12)
        plt.xticks(rotation=45)
        plt.legend(title="Product Name", bbox_to_anchor=(1.05, 1), loc="upper left")
        plt.tight_layout()

        # Save the visualization
        output_path = RESULTS_OUTPUT_DIR.joinpath("sales_by_day_and_product.png")
        plt.savefig(output_path)
        logger.info(f"Stacked bar chart saved to {output_path}.")
        plt.show()
    except Exception as e:
        logger.error(f"Error visualizing sales by day and product: {e}")
        raise
    
def visualize_total_sales_by_category(products_df: pd.DataFrame) -> None:
    """Visualize total sales by product category."""
    try:
        # Aggregate sales by product category
        category_sales = products_df.groupby("Category")["SaleAmount_sum"].sum().reset_index()
        category_sales.rename(columns={"Category": "product_category", "SaleAmount_sum": "sum(sale_amount)"}, inplace=True)

        # Create the bar chart
        import seaborn as sns
        import matplotlib.pyplot as plt

        sns.barplot(data=category_sales, x="product_category", y="sum(sale_amount)", palette="viridis")
        plt.xticks(rotation=45)
        plt.title("Total Sales by Product Category")
        plt.xlabel("Product Category")
        plt.ylabel("Total Sales (USD)")
        plt.tight_layout()

        # Save the visualization
        output_path = RESULTS_OUTPUT_DIR.joinpath("total_sales_by_category.png")
        plt.savefig(output_path)
        logger.info(f"Bar chart saved to {output_path}.")
        plt.show()
    except Exception as e:
        logger.error(f"Error visualizing total sales by product category: {e}")
        raise

def main():
    """Main function for analyzing and visualizing top product sales by day of the week."""
    logger.info("Starting SALES_TOP_PRODUCT_BY_WEEKDAY analysis...")

    # Step 1: Load the precomputed OLAP cube
    cube_df = load_olap_cube(CUBED_FILE)

    # Step 2: Load the product details
    products_df = load_products_data(PRODUCTS_FILE)

    # Step 3: Analyze top products by DayOfWeek
    top_products = analyze_top_product_by_weekday(cube_df, products_df)
    print(top_products)

    # Step 4: Visualize the results
    visualize_sales_by_weekday_and_product(cube_df, products_df)
    logger.info("Analysis and visualization completed successfully.")


if __name__ == "__main__":
    main()
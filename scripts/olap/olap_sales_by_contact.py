"""
Module 6: OLAP Goal Script (uses cubed results)
File: scripts/olap_total_sales_by_contact.py

GOAL: Break out total sales by CustomerID and identify their preferred contact method.

ACTION: Use this information to personalize marketing strategies and improve customer engagement.

PROCESS:
1. Load the OLAP cube to get total sales by CustomerID.
2. Load the customers data to get preferred contact methods.
3. Merge the data on CustomerID.
4. Visualize total sales by CustomerID and their preferred contact method.
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
    """Load the customers data."""
    try:
        customers_df = pd.read_csv(file_path)
        logger.info(f"Customers data successfully loaded from {file_path}.")
        return customers_df
    except Exception as e:
        logger.error(f"Error loading customers data: {e}")
        raise


def analyze_sales_and_contact(cube_df: pd.DataFrame, customers_df: pd.DataFrame) -> pd.DataFrame:
    """Analyze total sales by CustomerID and include preferred contact method."""
    try:
        # Group by CustomerID and calculate total sales
        customer_sales = cube_df.groupby("CustomerID")["SaleAmount_sum"].sum().reset_index()
        customer_sales.rename(columns={"SaleAmount_sum": "TotalSales"}, inplace=True)

        # Merge with customers data to get preferred contact method
        merged_data = customer_sales.merge(customers_df[["CustomerID", "PreferredContactMethod"]], on="CustomerID", how="left")

        logger.info("Sales and contact method analysis completed successfully.")
        return merged_data
    except Exception as e:
        logger.error(f"Error analyzing sales and contact method: {e}")
        raise

def visualize_sales_by_contact_method(merged_data: pd.DataFrame) -> None:
    """Visualize total sales by preferred contact method."""
    try:
        # Group by PreferredContactMethod and calculate total sales
        contact_method_sales = merged_data.groupby("PreferredContactMethod")["TotalSales"].sum().reset_index()

        # Sort data for better visualization
        contact_method_sales = contact_method_sales.sort_values(by="TotalSales", ascending=False)

        # Create the bar chart
        plt.figure(figsize=(10, 6))
        plt.bar(contact_method_sales["PreferredContactMethod"], contact_method_sales["TotalSales"], color="skyblue")
        plt.title("Total Sales by Preferred Contact Method", fontsize=16)
        plt.xlabel("Preferred Contact Method", fontsize=12)
        plt.ylabel("Total Sales (USD)", fontsize=12)
        plt.xticks(rotation=45, fontsize=10)
        plt.tight_layout()

        # Save the visualization
        output_path = RESULTS_OUTPUT_DIR.joinpath("sales_by_contact_method.png")
        plt.savefig(output_path)
        logger.info(f"Bar chart saved to {output_path}.")
        plt.show()
    except Exception as e:
        logger.error(f"Error visualizing sales by contact method: {e}")
        raise

def main():
    """Main function for analyzing sales and contact methods."""
    logger.info("Starting CUSTOMER_SALES_AND_CONTACT analysis...")

    # Step 1: Load the precomputed OLAP cube
    cube_df = load_olap_cube(CUBED_FILE)

    # Step 2: Load the customers data
    customers_df = load_customers_data(CUSTOMERS_FILE)

    # Step 3: Analyze sales and contact method
    merged_data = analyze_sales_and_contact(cube_df, customers_df)
    print(merged_data)

    # Step 4: Visualize total sales by contact method
    visualize_sales_by_contact_method(merged_data)

    logger.info("Analysis and visualization completed successfully.")


if __name__ == "__main__":
    main()
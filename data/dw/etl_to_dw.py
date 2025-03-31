import pandas as pd
import sqlite3
import pathlib
import sys

# For local imports, temporarily add project root to sys.path
PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

# Constants
DW_DIR = pathlib.Path("data").joinpath("dw")
DB_PATH = DW_DIR.joinpath("smart_sales.db")
PREPARED_DATA_DIR = pathlib.Path("data").joinpath("prepared")

def drop_unwanted_tables(cursor: sqlite3.Cursor) -> None:
    """Drop all tables except 'customer', 'product', and 'sales'."""
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = cursor.fetchall()
    for table in tables:
        table_name = table[0]
        if table_name not in ['customer', 'product', 'sales']:
            print(f"Dropping table: {table_name}")
            cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

def create_schema(cursor: sqlite3.Cursor) -> None:
    """Recreate tables to match the exact schema required."""
    # Drop tables to ensure schema alignment
    cursor.execute("DROP TABLE IF EXISTS customer")
    cursor.execute("DROP TABLE IF EXISTS product")
    cursor.execute("DROP TABLE IF EXISTS sales")

    # Recreate customer table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer (
            CustomerID INTEGER PRIMARY KEY,
            Name TEXT,
            Region TEXT,
            JoinDate TEXT,
            Age INTEGER,
            PreferredContactMethod TEXT
        )
    """)

    # Recreate product table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product (
            ProductID INTEGER PRIMARY KEY,
            ProductName TEXT,
            Category TEXT,
            UnitPrice REAL,
            StockQuantity INTEGER,
            StoreSection TEXT
        )
    """)

    # Recreate sales table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sales (
            TransactionID INTEGER PRIMARY KEY,
            CustomerID INTEGER,
            ProductID INTEGER,
            SaleAmount REAL,
            SaleDate DATE,
            CampaignID INTEGER,
            DiscountPercent INTEGER,
            PaymentType TEXT,
            StoreID TEXT,
            FOREIGN KEY (CustomerID) REFERENCES customer (CustomerID),
            FOREIGN KEY (ProductID) REFERENCES product (ProductID)
        )
    """)

def delete_existing_records(cursor: sqlite3.Cursor) -> None:
    """Delete all existing records from the customer, product, and sales tables."""
    cursor.execute("DELETE FROM customer")
    cursor.execute("DELETE FROM product")
    cursor.execute("DELETE FROM sales")

def insert_customers(customers_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert customer data into the customer table."""
    # Drop the 'StandardDateTime' column if it exists
    if "StandardDateTime" in customers_df.columns:
        print("Dropping 'StandardDateTime' column from Customers DataFrame...")
        customers_df = customers_df.drop(columns=["StandardDateTime"])

    print(f"Inserting into 'customer' table: {customers_df.head()}")
    customers_df.to_sql("customer", cursor.connection, if_exists="append", index=False)

def insert_products(products_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert product data into the product table."""
    print(f"Inserting into 'product' table: {products_df.head()}")
    products_df.to_sql("product", cursor.connection, if_exists="append", index=False)

def insert_sales(sales_df: pd.DataFrame, cursor: sqlite3.Cursor) -> None:
    """Insert sales data into the sales table."""
    print(f"Inserting into 'sales' table: {sales_df.head()}")
    sales_df.to_sql("sales", cursor.connection, if_exists="append", index=False)

def load_data_to_db() -> None:
    try:
        # Connect to SQLite – will create the file if it doesn't exist
        print("Connecting to the database...")
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()

        # Drop any unnecessary tables
        print("Dropping unwanted tables...")
        drop_unwanted_tables(cursor)

        # Create schema and clear existing records
        print("Creating schema...")
        create_schema(cursor)

        print("Deleting existing records...")
        delete_existing_records(cursor)

        # Load prepared data using pandas
        print("Loading prepared data...")
        customers_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("customers_data_prepared.csv"))
        products_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("products_data_prepared.csv"))
        sales_df = pd.read_csv(PREPARED_DATA_DIR.joinpath("sales_data_prepared.csv"))

        # Insert data into the database
        print("Inserting customers...")
        insert_customers(customers_df, cursor)

        print("Inserting products...")
        insert_products(products_df, cursor)

        print("Inserting sales...")
        insert_sales(sales_df, cursor)

        conn.commit()
        print("Data successfully loaded into the database!")
    finally:
        if conn:
            conn.close()
            print("Database connection closed.")

if __name__ == "__main__":
    load_data_to_db()
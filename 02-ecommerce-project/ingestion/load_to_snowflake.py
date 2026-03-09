import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

# ── Connection ──────────────────────────────────────────────
print("Connecting to Snowflake...")
conn = snowflake.connector.connect(
    account=os.getenv("SF_ACCOUNT"),
    user=os.getenv("SF_USER"),
    password=os.getenv("SF_PASSWORD"),
    warehouse=os.getenv("SF_WAREHOUSE"),
    database="ECOMMERCE_RAW",
    schema="RAW",
    role="DBT_ROLE"
)
print("✅ Connected!\n")

# ── Table map ───────────────────────────────────────────────
TABLES = {
    "OLIST_ORDERS":           "olist_orders_dataset.csv",
    "OLIST_ORDER_ITEMS":      "olist_order_items_dataset.csv",
    "OLIST_CUSTOMERS":        "olist_customers_dataset.csv",
    "OLIST_PRODUCTS":         "olist_products_dataset.csv",
    "OLIST_SELLERS":          "olist_sellers_dataset.csv",
    "OLIST_ORDER_PAYMENTS":   "olist_order_payments_dataset.csv",
    "OLIST_ORDER_REVIEWS":    "olist_order_reviews_dataset.csv",
    "OLIST_GEOLOCATION":      "olist_geolocation_dataset.csv",
    "PRODUCT_CATEGORY_NAMES": "product_category_name_translation.csv",
}

DATA_PATH = "data/raw"


# ── Load each CSV ───────────────────────────────────────────
print("Starting ingestion...\n")

for table_name, filename in TABLES.items():
    filepath = os.path.join(DATA_PATH, filename)

    if not os.path.exists(filepath):
        print(f"⚠️  SKIPPED {filename} — file not found")
        continue

    print(f"Loading {filename}...")
    df = pd.read_csv(filepath)
    df.columns = [c.upper() for c in df.columns]

    conn.cursor().execute(
        f"DROP TABLE IF EXISTS ECOMMERCE_RAW.RAW.{table_name}"
    )

    success, num_chunks, num_rows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        database="ECOMMERCE_RAW",
        schema="RAW",
        auto_create_table=True,
        overwrite=True
    )

    print(f"  ✅ {table_name}: {num_rows:,} rows loaded\n")

conn.close()
print("🎉 All tables loaded into Snowflake successfully!")
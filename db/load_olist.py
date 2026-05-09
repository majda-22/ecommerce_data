import pandas as pd
from sqlalchemy import create_engine, text
import os
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[1]
BASE_DIR = ROOT_DIR / "data" / "olist"

DB_URL = os.getenv(
    "DATABASE_URL",
    "postgresql+psycopg2://shopflow:shopflow@localhost:5432/shopflow",
)

TIMESTAMP_COLS = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
    "shipping_limit_date",
    "review_creation_date",
    "review_answer_timestamp",
]

PRIMARY_KEYS = {
    "customers": ["customer_id"],
    "products": ["product_id"],
    "sellers": ["seller_id"],
    "product_category_translation": ["product_category_name"],
    "orders": ["order_id"],
    "order_items": ["order_id", "order_item_id"],
    "payments": ["order_id", "payment_sequential"],
    "reviews": ["review_id", "order_id"],
}

# Order matters because of foreign keys
LOAD_PLAN = [
    ("customers",                    "olist_customers_dataset.csv"),
    ("products",                     "olist_products_dataset.csv"),
    ("sellers",                      "olist_sellers_dataset.csv"),
    ("product_category_translation", "product_category_name_translation.csv"),
    ("orders",                       "olist_orders_dataset.csv"),
    ("order_items",                  "olist_order_items_dataset.csv"),
    ("payments",                     "olist_order_payments_dataset.csv"),
    ("reviews",                      "olist_order_reviews_dataset.csv"),
    ("geolocation",                  "olist_geolocation_dataset.csv"),
]

engine = create_engine(DB_URL)


def read_olist_csv(filename):
    filepath = BASE_DIR / filename
    if not filepath.exists():
        raise FileNotFoundError(f"Could not find {filepath}")

    return pd.read_csv(filepath, encoding="utf-8-sig")


def clean_dataframe(table_name, df):
    df = df.copy()

    for col in TIMESTAMP_COLS:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    key_cols = PRIMARY_KEYS.get(table_name)
    if key_cols:
        before = len(df)
        df = df.dropna(subset=key_cols)
        df = df.drop_duplicates(subset=key_cols, keep="first")
        dropped = before - len(df)
        if dropped:
            print(f"  Dropped {dropped:,} duplicate/null key rows")

    if table_name == "geolocation":
        before = len(df)
        df = df.drop_duplicates()
        dropped = before - len(df)
        if dropped:
            print(f"  Dropped {dropped:,} duplicate geolocation rows")

    return df


def reset_tables():
    table_names = ", ".join(table for table, _ in LOAD_PLAN)
    with engine.begin() as conn:
        conn.execute(text(f"TRUNCATE TABLE {table_names} RESTART IDENTITY CASCADE"))


def load_table(table_name, filename):
    print(f"\nLoading {table_name} from {filename}...")

    df = clean_dataframe(table_name, read_olist_csv(filename))

    df.to_sql(
        table_name,
        engine,
        if_exists="append",
        index=False,
        chunksize=5000,
        method="multi",
    )
    print(f"  Inserted {len(df):,} rows into {table_name}")


def verify_counts():
    print("\nVerification counts:")
    with engine.connect() as conn:
        for table, _ in LOAD_PLAN:
            count = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()
            print(f"  {table}: {count:,} rows")


if __name__ == "__main__":
    print("Starting Olist data load...")
    print("Resetting target tables...")
    reset_tables()

    for table, file in LOAD_PLAN:
        load_table(table, file)

    print("\nAll tables loaded successfully.")

    verify_counts()

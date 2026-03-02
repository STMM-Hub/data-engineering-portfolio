import pandas as pd
import psycopg2
from sqlalchemy import create_engine, text
import os
from dotenv import load_dotenv

# Explicitly load .env from project root
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
load_dotenv(os.path.join(BASE_DIR, '..', '.env'))

def get_connection_string() -> str:
    """Build PostgreSQL connection string from .env file"""
    user = os.getenv('POSTGRES_USER', 'sithu')
    password = os.getenv('POSTGRES_PASSWORD', 'sithu123')
    host = os.getenv('POSTGRES_HOST', 'postgres')  # ← fallback is 'postgres'
    port = os.getenv('POSTGRES_PORT', '5432')
    db = os.getenv('POSTGRES_DB', 'bangkok_property')
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"


def create_table_if_not_exists(engine):
    """Create the property_listings table if it doesn't exist"""
    print("[LOAD] Creating table if not exists...")
    
    sql_path = os.path.join(BASE_DIR, '..', 'sql', 'create_tables.sql')
    
    with open(sql_path, 'r') as f:
        sql = f.read()
    
    with engine.begin() as conn:  
        conn.execute(text(sql))
    
    print("[LOAD] Table ready ✅")


def load_to_postgres(df: pd.DataFrame):
    """Load cleaned DataFrame into PostgreSQL."""
    print(f"[LOAD] Connecting to PostgreSQL...")
    
    engine = create_engine(get_connection_string())
    
    # Create table with correct schema first
    create_table_if_not_exists(engine)
    
    # Select only columns that match the database schema
    db_columns = [
        'district', 'property_type', 'price', 'area_sqm',
        'price_per_sqm', 'bedrooms', 'price_tier', 'ingestion_date'
    ]
    
    cols_to_load = [c for c in db_columns if c in df.columns]
    df_to_load = df[cols_to_load]
    
    print(f"[LOAD] Loading {len(df_to_load)} rows into PostgreSQL...")
    
    df_to_load.to_sql(
        name='property_listings',
        con=engine,
        if_exists='append',   # ← changed from 'replace' to 'append'
        index=False,
        chunksize=500
    )
    
    print(f"[LOAD] Successfully loaded {len(df_to_load)} rows ✅")
    
    # Verify the load
    with engine.connect() as conn:
        result = conn.execute(text("SELECT COUNT(*) FROM property_listings"))
        count = result.fetchone()[0]
        print(f"[LOAD] Verified: {count} rows now in database")


if __name__ == "__main__":
    import sys
    sys.path.append('.')
    from ingestion.extract import extract_from_csv
    from transformation.transform import transform_property_data
    
    df_raw = extract_from_csv("data/raw_listings.csv")
    df_clean = transform_property_data(df_raw)
    load_to_postgres(df_clean)
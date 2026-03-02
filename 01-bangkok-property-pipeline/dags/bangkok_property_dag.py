from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow/project')

default_args = {
    'owner': 'sithu',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}

def extract_task():
    from ingestion.extract import extract_from_csv
    df = extract_from_csv('/opt/airflow/project/data/raw_listings.csv')
    df.to_csv('/tmp/extracted.csv', index=False)
    print(f"Extract complete: {len(df)} rows")

def check_extract_quality():
    import pandas as pd
    df = pd.read_csv('/tmp/extracted.csv')

    errors = []

    # Check 1: Not empty
    if len(df) == 0:
        errors.append("❌ Extract failed: 0 rows loaded")

    # Check 2: Minimum row threshold
    if len(df) < 10:
        errors.append(f"❌ Too few rows after extract: {len(df)}")

    # Check 3: Critical columns exist
    required_cols = ['Property Type', 'Location', 'Area (sq. ft.)', 'Price (THB)']
    missing_cols = [c for c in required_cols if c not in df.columns]
    if missing_cols:
        errors.append(f"❌ Missing columns: {missing_cols}")

    if errors:
        raise ValueError("\n".join(errors))

    print(f"✅ Extract quality passed: {len(df)} rows, all required columns present")

def transform_task():
    import pandas as pd
    from transformation.transform import transform_property_data
    df = pd.read_csv('/tmp/extracted.csv')
    df_clean = transform_property_data(df)
    df_clean.to_csv('/tmp/transformed.csv', index=False)
    print(f"Transform complete: {len(df_clean)} rows")

def check_transform_quality():
    import pandas as pd
    df_raw = pd.read_csv('/tmp/extracted.csv')
    df_clean = pd.read_csv('/tmp/transformed.csv')

    errors = []

    # Check 1: Not empty after transform
    if len(df_clean) == 0:
        errors.append("❌ Transform produced 0 rows")

    # Check 2: Didn't lose more than 80% of data
    drop_pct = (len(df_raw) - len(df_clean)) / len(df_raw) * 100
    if drop_pct > 80:
        errors.append(f"❌ Transform dropped {drop_pct:.1f}% of rows — too much data lost")

    # Check 3: No nulls in critical columns
    critical_cols = ['district', 'property_type', 'price', 'area_sqm']
    for col in critical_cols:
        if col in df_clean.columns:
            null_count = df_clean[col].isnull().sum()
            if null_count > 0:
                errors.append(f"❌ Column '{col}' has {null_count} null values")

    # Check 4: Price and area are positive
    if (df_clean['price'] <= 0).any():
        errors.append("❌ Some prices are zero or negative")
    if (df_clean['area_sqm'] <= 0).any():
        errors.append("❌ Some area values are zero or negative")

    # Check 5: price_tier column exists and has valid values
    valid_tiers = ['Budget (< 2M)', 'Mid-range (2M - 5M)', 'Premium (5M - 10M)', 'Luxury (> 10M)']
    if 'price_tier' in df_clean.columns:
        invalid_tiers = df_clean[~df_clean['price_tier'].isin(valid_tiers)]
        if len(invalid_tiers) > 0:
            errors.append(f"❌ {len(invalid_tiers)} rows have invalid price_tier values")

    if errors:
        raise ValueError("\n".join(errors))

    print(f"✅ Transform quality passed: {len(df_clean)} rows ({drop_pct:.1f}% dropped from raw)")

def load_task():
    import pandas as pd
    from transformation.load import load_to_postgres
    df = pd.read_csv('/tmp/transformed.csv')
    load_to_postgres(df)
    print("Load complete")

def check_load_quality():
    import pandas as pd
    from sqlalchemy import create_engine, text
    import os
    from dotenv import load_dotenv

    BASE_DIR = '/opt/airflow/project'
    load_dotenv(os.path.join(BASE_DIR, '.env'))

    df_loaded = pd.read_csv('/tmp/transformed.csv')
    expected_rows = len(df_loaded)

    engine = create_engine(
        f"postgresql+psycopg2://"
        f"{os.getenv('POSTGRES_USER', 'sithu')}:"
        f"{os.getenv('POSTGRES_PASSWORD', 'sithu123')}@"
        f"{os.getenv('POSTGRES_HOST', 'postgres')}:"
        f"{os.getenv('POSTGRES_PORT', '5432')}/"
        f"{os.getenv('POSTGRES_DB', 'bangkok_property')}"
    )

    errors = []

    with engine.connect() as conn:
        # Check 1: Row count matches
        result = conn.execute(text("SELECT COUNT(*) FROM property_listings"))
        db_count = result.fetchone()[0]
        if db_count != expected_rows:
            errors.append(f"❌ Row count mismatch: expected {expected_rows}, got {db_count} in DB")

        # Check 2: No nulls in critical DB columns
        for col in ['district', 'price', 'area_sqm']:
            result = conn.execute(text(f"SELECT COUNT(*) FROM property_listings WHERE {col} IS NULL"))
            null_count = result.fetchone()[0]
            if null_count > 0:
                errors.append(f"❌ Column '{col}' has {null_count} nulls in DB")

        # Check 3: Price tier distribution looks reasonable
        result = conn.execute(text("SELECT price_tier, COUNT(*) FROM property_listings GROUP BY price_tier"))
        tiers = result.fetchall()
        print(f"✅ Price tier distribution: {dict(tiers)}")

    if errors:
        raise ValueError("\n".join(errors))

    print(f"✅ Load quality passed: {db_count} rows verified in database")

with DAG(
    dag_id='bangkok_property_pipeline',
    default_args=default_args,
    description='Bangkok Property Market ETL Pipeline',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['bangkok', 'property', 'etl'],
) as dag:

    extract = PythonOperator(
        task_id='extract',
        python_callable=extract_task,
    )

    check_extract = PythonOperator(
        task_id='check_extract',
        python_callable=check_extract_quality,
    )

    transform = PythonOperator(
        task_id='transform',
        python_callable=transform_task,
    )

    check_transform = PythonOperator(
        task_id='check_transform',
        python_callable=check_transform_quality,
    )

    load = PythonOperator(
        task_id='load',
        python_callable=load_task,
    )

    check_load = PythonOperator(
        task_id='check_load',
        python_callable=check_load_quality,
    )

    extract >> check_extract >> transform >> check_transform >> load >> check_load
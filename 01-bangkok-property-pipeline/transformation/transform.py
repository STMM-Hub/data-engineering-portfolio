import pandas as pd

def transform_property_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and transform raw property listing data.
    Steps:
    - Standardize column names
    - Remove nulls and duplicates
    - Calculate price per sqm
    - Categorize property types
    - Remove outliers
    """
    print(f"[TRANSFORM] Starting transformation. Input rows: {len(df)}")
    
    # --- Step 1: Standardize column names ---
    # Make all column names lowercase with underscores
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_')
        .str.replace('-', '_')
    )
    print(f"[TRANSFORM] Cleaned column names: {list(df.columns)}")
    
    # --- Step 2: Rename columns to standard names ---
    # This handles different column names across Kaggle datasets
    # Adjust these based on what columns YOUR dataset actually has
    rename_map = {
        'property_type': 'property_type',
        'location': 'district',
        'area_(sq._ft.)': 'area_sqm',
        'bedrooms': 'bedrooms',
        'bathrooms': 'bathrooms',
        'price_(thb)': 'price',
    }

    # Only rename columns that actually exist
    rename_map = {k: v for k, v in rename_map.items() if k in df.columns}
    df = df.rename(columns=rename_map)
    print(f"[TRANSFORM] Renamed columns: {rename_map}")
    
    # --- Step 3: Keep only the columns we need ---
    required_cols = ['district', 'property_type', 'price', 'area_sqm',
                 'bedrooms', 'bathrooms', 'ingestion_date']
    optional_cols = ['listing_date', 'title', 'url']
    
    keep_cols = [c for c in required_cols + optional_cols if c in df.columns]
    df = df[keep_cols]
    
    # --- Step 4: Drop rows missing critical values ---
    before = len(df)
    df = df.dropna(subset=['price', 'area_sqm', 'district'])
    print(f"[TRANSFORM] Dropped {before - len(df)} rows with missing critical values")
    
    # --- Step 5: Remove duplicates ---
    before = len(df)
    df = df.drop_duplicates()
    print(f"[TRANSFORM] Dropped {before - len(df)} duplicate rows")
    
    # --- Step 6: Clean data types ---
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    df['area_sqm'] = pd.to_numeric(df['area_sqm'], errors='coerce')
    df['area_sqm'] = (df['area_sqm'] * 0.0929).round(2) # convert sq ft to sq meters (1 sq ft = 0.0929 sqm)
    df['bedrooms'] = pd.to_numeric(df['bedrooms'], errors='coerce').fillna(0).astype(int)
    df['district'] = df['district'].str.strip().str.title()
    df['property_type'] = df['property_type'].str.strip().str.title()
    
    # Drop rows where conversion failed
    df = df.dropna(subset=['price', 'area_sqm'])
    
    # --- Step 7: Calculate price per sqm ---
    df['price_per_sqm'] = (df['price'] / df['area_sqm']).round(2)
    
    # --- Step 8: Remove outliers ---
    # Price below 100,000 THB is likely bad data
    # Price per sqm above 500,000 THB is likely error
    before = len(df)
    df = df[df['price'] >= 100000]
    df = df[df['price_per_sqm'] <= 500000]
    df = df[df['area_sqm'] >= 10]   # Smaller than 10sqm is not valid
    print(f"[TRANSFORM] Removed {before - len(df)} outlier rows")
    
    # --- Step 9: Add price tier category ---
    def categorize_price(price):
        if price < 2_000_000:
            return 'Budget (< 2M)'
        elif price < 5_000_000:
            return 'Mid-range (2M - 5M)'
        elif price < 10_000_000:
            return 'Premium (5M - 10M)'
        else:
            return 'Luxury (> 10M)'
    
    df['price_tier'] = df['price'].apply(categorize_price)
    
    print(f"[TRANSFORM] Transformation complete. Output rows: {len(df)}")
    print(f"[TRANSFORM] Price tier breakdown:\n{df['price_tier'].value_counts()}")
    
    return df


if __name__ == "__main__":
    # Test transformer independently
    import sys
    sys.path.append('.')
    from ingestion.extract import extract_from_csv
    
    df_raw = extract_from_csv("data/raw_listings.csv")
    df_clean = transform_property_data(df_raw)
    print(df_clean.head(10))
    print(f"\nFinal shape: {df_clean.shape}")
# 🛒 SEA E-Commerce Customer Analytics Platform

> End-to-end data engineering project: Python → Snowflake → dbt → Power BI + Streamlit

## Architecture
```
Kaggle Olist Dataset (CSVs)
        ↓
Python Ingestion (pandas + snowflake-connector)
        ↓
Snowflake RAW Schema
        ↓
dbt (Bronze → Silver → Gold)
        ↓
Power BI Dashboard + Streamlit App
```

## Tech Stack

- **Cloud Warehouse:** Snowflake
- **Transformation:** dbt Core
- **Ingestion:** Python (pandas, snowflake-connector-python)
- **Visualization:** Power BI, Streamlit
- **Orchestration:** (coming soon)
- **CI/CD:** GitHub Actions

## Project Structure
```
├── ingestion/          # Python scripts to load raw data into Snowflake
├── data/raw/           # Raw CSV files (gitignored)
├── .env.example        # Environment variable template
└── README.md
```

## Setup

### 1. Clone the repo
```bash
git clone https://github.com/YOUR_USERNAME/data-engineering-portfolio.git
cd 02-ecommerce-project
```

### 2. Create virtual environment
```bash
python -m venv venv
venv\Scripts\activate       # Windows
source venv/bin/activate    # Mac/Linux
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure credentials
```bash
cp .env.example .env
# Fill in your Snowflake credentials in .env
```

### 5. Download data
Download the [Olist Brazilian E-Commerce dataset](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) and place CSVs in `data/raw/`

### 6. Run ingestion
```bash
python ingestion/load_to_snowflake.py
```

## Data Model

| Schema | Purpose |
|--------|---------|
| RAW | Raw ingested CSV data, never modified |
| BRONZE | Staging models: typed, renamed, 1-to-1 with source |
| SILVER | Intermediate: joined and cleaned |
| GOLD | Business marts: RFM, cohort retention, revenue |

## Status

- [x] Phase 1: Snowflake setup + Python ingestion
- [ ] Phase 2: dbt Bronze models
- [ ] Phase 3: dbt Silver + Gold models
- [ ] Phase 4: Streamlit dashboard
- [ ] Phase 5: CI/CD pipeline
import pandas as pd
from ingest import create_bronze
from settings import PROJECT_ROOT

dfp = create_bronze()

def clean_silver(dfp):
    df_silver = pd.read_parquet(dfp)
    df_silver["date_added"] = pd.to_datetime(df_silver["date_added"], errors="coerce")
    df_clean = df_silver.fillna({
        'director': 'Unknown',
        'cast': 'Unknown',
        'country': 'Unknown'
    })
    return df_clean

def save_silver(df_clean):
    silver_dir = PROJECT_ROOT / "data" / "silver"
    silver_path = silver_dir /'netflix.parquet'
    silver_dir.mkdir(parents=True, exist_ok=True)

    return df_clean.to_parquet(silver_path)


def create_silver():
    df_clean = clean_silver(dfp)
    save_silver(df_clean)
    silver_path = PROJECT_ROOT / "data" / "silver" / "netflix.parquet"
    return silver_path

def load_silver():
    df = pd.read_parquet(create_silver())
    return df
import pandas as pd
from pathlib import Path
from ingest import  dfp


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
    silver_dir = Path('data/silver/')
    silver_path = Path(silver_dir /'netflix.parquet')
    silver_dir.mkdir(parents=True, exist_ok=True)

    return df_clean.to_parquet(silver_path)


df_clean = clean_silver(dfp)
save_silver(df_clean)
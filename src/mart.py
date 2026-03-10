import pandas as pd
from pathlib import Path 
from transform import df_clean

def content_by_type(df_clean):
    df = pd.read_parquet(df_clean)
    grouped = df.groupby(df["type"])
    return grouped
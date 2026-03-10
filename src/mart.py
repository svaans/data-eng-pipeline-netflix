import pandas as pd
from settings import PROJECT_ROOT 
from transform import load_silver

df = load_silver()

def content_by_type(df):
    grouped = df.groupby("type").size().reset_index().rename(columns={0 : "Total Content"})
    return grouped

def save_gold(grouped):
    gold_dir = PROJECT_ROOT / "data" / "gold"
    gold_path = gold_dir / "content_by_type.parquet"

    gold_dir.mkdir(parents=True, exist_ok=True)
    return grouped.to_parquet(gold_path)

def create_gold():
    grouped = content_by_type(df)
    save_gold(grouped)
    return grouped

create_gold()

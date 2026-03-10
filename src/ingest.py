from pathlib import Path
import pandas as pd

data = Path('data/raw/netflix_titles.csv')

def extract(data): 
    
    if not data.exists():
        raise FileNotFoundError
    
    df = pd.read_csv(data)
    return df

def load_bronze(df):
    bronze_dir = Path('data/bronze/')
    parquet_path = Path(bronze_dir /'netflix_titles.parquet')
    if not bronze_dir.exists():
        bronze_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False)
    return parquet_path



df = extract(data)
dfp = load_bronze(df)

print(df.info())
print(df.isnull().sum())
print(df.head())










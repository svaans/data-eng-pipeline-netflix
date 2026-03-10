import pandas as pd
from settings import PROJECT_ROOT

data = PROJECT_ROOT / "data" / "raw" / "netflix_titles.csv"

def extract(data): 
    
    if not data.exists():
        raise FileNotFoundError
    
    df = pd.read_csv(data)
    return df

def load_bronze(df):
    bronze_dir = PROJECT_ROOT / "data" / "bronze"
    parquet_path = bronze_dir /'netflix_titles.parquet'
    if not bronze_dir.exists():
        bronze_dir.mkdir(parents=True, exist_ok=True)
    df.to_parquet(parquet_path, index=False)
    return parquet_path

def create_bronze():
    df = extract(data)
    dfp = load_bronze(df)
    return dfp




#print(df.info())
#print(df.isnull().sum())
#print(df.head())










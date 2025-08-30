import pandas as pd
from utils import insert_data
from config import Config

def clean_and_transform(raw_data):
    """
    Example transformation:
    - Convert dict into pandas DataFrame
    - Clean column names
    - Handle missing values
    - Return transformed list of tuples for DB insert
    """
    df = pd.DataFrame(raw_data)

    # Example transformations
    df.columns = [col.lower().strip() for col in df.columns]  # standardize col names
    df = df.fillna(0)  # replace NaN with 0
    
    # Example: create derived column (avg intensity)
    if "forecast" in df.columns and "actual" in df.columns:
        df["avg_intensity"] = (df["forecast"] + df["actual"]) / 2

    # Convert DataFrame rows → list of tuples
    transformed_data = [tuple(x) for x in df.to_numpy()]
    
    return df, transformed_data


def load_transformed_data(raw_data):
    """
    Runs transformation then loads into Postgres
    """
    df, transformed_data = clean_and_transform(raw_data)
    columns = list(df.columns)  # Use DataFrame column names
    
    insert_data(Config.TRANSFORMED_TABLE, transformed_data, columns)
    print(f"✅ Inserted {len(transformed_data)} rows into {Config.TRANSFORMED_TABLE}")

import pandas as pd

def transform_data(df):
    df = df.dropna().copy()

    df.columns = df.columns.str.lower().str.replace(" ", "_")

    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df['gender'] = df['gender'].str.lower()

    stress_map = {"Low": 1, "Medium": 2, "High": 3}
    df['stress_score'] = df['growing_stress'].map(stress_map)
    df['stress_score'] = df['stress_score'].fillna(0).astype(int)

    print("Data transformed")
    return df
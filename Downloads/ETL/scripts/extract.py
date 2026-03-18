import pandas as pd

def extract_data():
    df = pd.read_csv("data/mental_health.csv")
    print("Data extracted")
    return df
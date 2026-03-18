from scripts.extract import extract_data
from scripts.transform import transform_data
from scripts.load import load_data

def run_pipeline():
    df = extract_data()
    df = transform_data(df)
    load_data(df)

if __name__ == "__main__":
    run_pipeline()
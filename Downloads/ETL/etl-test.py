import pandas as pd
import psycopg2

# ---------------- EXTRACT ----------------
def extract_data():
    df = pd.read_csv("C:/Users/sarat/Downloads/ETL/Mental Health Dataset.csv")
    print("✅ Data extracted")
    return df


# ---------------- TRANSFORM ----------------
def transform_data(df):
    # Remove nulls safely
    df = df.dropna().copy()

    # Standardize column names
    df.columns = df.columns.str.lower().str.replace(" ", "_")

    # Convert timestamp
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')

    # Clean gender
    df['gender'] = df['gender'].str.lower()

    # Create stress score safely
    stress_map = {"Low": 1, "Medium": 2, "High": 3}
    df['stress_score'] = df['growing_stress'].map(stress_map)

    # Handle missing values
    df['stress_score'] = df['stress_score'].fillna(0).astype(int)

    print("✅ Data transformed")
    return df


# ---------------- LOAD ----------------
def load_data(df):
    try:
        conn = psycopg2.connect(
            dbname="etl_db",
            user="postgres",
            password="your_password",  # 👈 CHANGE THIS
            host="localhost",
            port="5432"
        )
        cursor = conn.cursor()

        for _, row in df.iterrows():
            cursor.execute("""
                INSERT INTO mental_health (
                    timestamp, gender, country, occupation, self_employed,
                    family_history, treatment, days_indoors, growing_stress,
                    changes_habits, mental_health_history, mood_swings,
                    coping_struggles, work_interest, social_weakness,
                    mental_health_interview, care_options, stress_score
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row['timestamp'],
                row['gender'],
                row['country'],
                row['occupation'],
                row['self_employed'],
                row['family_history'],
                row['treatment'],
                row['days_indoors'],
                row['growing_stress'],
                row['changes_habits'],
                row['mental_health_history'],
                row['mood_swings'],
                row['coping_struggles'],
                row['work_interest'],
                row['social_weakness'],
                row['mental_health_interview'],
                row['care_options'],
                row['stress_score']
            ))

        conn.commit()
        cursor.close()
        conn.close()

        print("✅ Data loaded successfully")

    except Exception as e:
        print("❌ Error:", e)


# ---------------- MAIN ----------------
if __name__ == "__main__":
    df = extract_data()
    df = transform_data(df)
    load_data(df)
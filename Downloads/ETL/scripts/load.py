import psycopg2

def load_data(df):
    print("Loading started...")

    conn = psycopg2.connect(
        dbname="etl_db",
        user="postgres",
        password="Sarath-postgres@07",
        host="localhost",
        port="5432"
    )

    cursor = conn.cursor()
    data = [
    (
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
    )
    for _, row in df.iterrows()   # ✅ THIS LINE WAS MISSING
]
    cursor.executemany("""
    INSERT INTO mental_health (
        timestamp, gender, country, occupation, self_employed,
        family_history, treatment, days_indoors, growing_stress,
        changes_habits, mental_health_history, mood_swings,
        coping_struggles, work_interest, social_weakness,
        mental_health_interview, care_options, stress_score
    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
""", data)
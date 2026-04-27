import pandas as pd
import os
import shutil
from sqlalchemy import create_engine, text

DB_NAME = "merged_database.db"
PROCESSED_FOLDER = "files"

engine = create_engine(f"sqlite:///{DB_NAME}")

# Create folder for processed files if it doesn't exist
if not os.path.exists(PROCESSED_FOLDER):
    os.makedirs(PROCESSED_FOLDER)

def optimize_dataframe(df):
    df = df.dropna(how="all")
    df = df.dropna(axis=1, how="all")
    df = df.drop_duplicates()
    df.columns = [c.strip().replace(" ", "_") for c in df.columns]

    for col in df.columns:
        if df[col].dtype == "object":
            try:
                df[col] = pd.to_numeric(df[col])
            except:
                df[col] = df[col].astype("string")

        if "int" in str(df[col].dtype):
            df[col] = pd.to_numeric(df[col], downcast="integer")

        if "float" in str(df[col].dtype):
            df[col] = pd.to_numeric(df[col], downcast="float")

    return df

def table_exists(table_name):
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT name FROM sqlite_master WHERE type='table' AND name=:name"),
            {"name": table_name}
        ).fetchone()
        return result is not None

excel_files = [f for f in os.listdir() if f.endswith((".xlsx", ".xls"))]

if not excel_files:
    print("No Excel files found.")
    input("Press Enter to exit...")
    exit()

for file in excel_files:
    print(f"\nProcessing {file} ...")

    try:
        df = pd.read_excel(file)
        df = optimize_dataframe(df)
        table_name = os.path.splitext(file)[0]

        if table_exists(table_name):
            df.to_sql(table_name, engine, if_exists="append", index=False)
        else:
            df.to_sql(table_name, engine, if_exists="replace", index=False)

        # Move processed file to /files folder
        shutil.move(file, os.path.join(PROCESSED_FOLDER, file))
        print(f"{file} moved to /files folder")

    except Exception as e:
        print(f"Error processing {file}: {e}")

print("\nOptimizing database...")
with engine.connect() as conn:
    conn.execute(text("VACUUM"))

print("DONE ✅")
input("Press Enter to exit...")
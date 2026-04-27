import pandas as pd
import os
import json
import hashlib
from sqlalchemy import create_engine, text

# ===============================
# DATABASE CONNECTION (use env variable in cloud)
# ===============================
DB_URL = os.environ.get("DB_URL", "sqlite:///merged_database.db")
engine = create_engine(DB_URL)

STATE_FILE = "file_state.json"
EXCEL_FOLDER = "excel"

# ===============================
# LOAD & SAVE STATE (hash memory)
# ===============================
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

# ===============================
# FILE HASH FUNCTION (NEW ⭐)
# ===============================
def get_file_hash(path):
    hasher = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()

# ===============================
# DATA CLEANING (your original logic)
# ===============================
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

# ===============================
# CHECK IF TABLE EXISTS
# ===============================
def table_exists(table_name):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=:name
        """), {"name": table_name}).fetchone()
        return result is not None

# ===============================
# MAIN PROCESS
# ===============================
file_state = load_state()

if not os.path.exists(EXCEL_FOLDER):
    print("Excel folder not found.")
    exit()

excel_files = [f for f in os.listdir(EXCEL_FOLDER) if f.endswith((".xlsx", ".xls"))]

if not excel_files:
    print("No Excel files found.")
    exit()

for file in excel_files:
    path = os.path.join(EXCEL_FOLDER, file)
    current_hash = get_file_hash(path)

    # Skip unchanged files
    if file in file_state and file_state[file] == current_hash:
        print(f"Skipping {file} (unchanged)")
        continue

    print(f"Processing updated file: {file}")

    try:
        df = pd.read_excel(path)
        df = optimize_dataframe(df)
        table_name = os.path.splitext(file)[0]

        if table_exists(table_name):
            df.to_sql(table_name, engine, if_exists="append", index=False)
        else:
            df.to_sql(table_name, engine, if_exists="replace", index=False)

        # Save new hash after success
        file_state[file] = current_hash
        print(f"{file} uploaded successfully")

    except Exception as e:
        print(f"Error processing {file}: {e}")

# Save state for next run
save_state(file_state)
print("State saved. Job finished ✅")

import pandas as pd
import os
import json
import hashlib
from sqlalchemy import create_engine, text

# =====================================================
# DATABASE CONNECTIONS
# (use env variables in cloud, SQLite locally)
# =====================================================
DEFAULT_DB_URL      = os.environ.get("DB_URL", "sqlite:///merged_database.db")
SALES_DB_URL        = os.environ.get("SALES_DB_URL", "sqlite:///tci_sales.db")
USERS_DB_URL        = os.environ.get("USERS_DB_URL", "sqlite:///tci_Users.db")
RESREPORT_DB_URL    = os.environ.get("RESREPORT_DB_URL", "sqlite:///tci_ResReports.db")
RESLIST_DB_URL      = os.environ.get("RESLIST_DB_URL", "sqlite:///tci_ResList.db")
COMMISSION_DB_URL   = os.environ.get("COMMISSION_DB_URL", "sqlite:///tci_Commissions.db")

ENGINES = {
    "excel": create_engine(DEFAULT_DB_URL),
    "Sale": create_engine(SALES_DB_URL),
    "Users": create_engine(USERS_DB_URL),
    "ResReport": create_engine(RESREPORT_DB_URL),
    "ResList": create_engine(RESLIST_DB_URL),
    "Commission": create_engine(COMMISSION_DB_URL),
}

# =====================================================
# FOLDER → FILENAME RULES
# =====================================================
ROUTING_RULES = {
    "excel": None,           # process all files
    "Sale": "_S_",
    "Users": "_Users_",
    "ResReport": "_Res_",
    "ResList": "_ResList_",
    "Commission": "_C_"
}

STATE_FILE = "file_state.json"

# =====================================================
# LOAD / SAVE HASH STATE
# =====================================================
def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

# =====================================================
# FILE HASH (detect real changes)
# =====================================================
def get_file_hash(path):
    hasher = hashlib.md5()
    with open(path, "rb") as f:
        while chunk := f.read(8192):
            hasher.update(chunk)
    return hasher.hexdigest()

# =====================================================
# DATA CLEANING (your original logic)
# =====================================================
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

# =====================================================
# CHECK IF TABLE EXISTS
# =====================================================
def table_exists(engine, table_name):
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT name FROM sqlite_master 
            WHERE type='table' AND name=:name
        """), {"name": table_name}).fetchone()
        return result is not None

# =====================================================
# MAIN ETL PROCESS
# =====================================================
file_state = load_state()

for folder, engine in ENGINES.items():

    if not os.path.exists(folder):
        print(f"Folder '{folder}' not found → skipping")
        continue

    pattern = ROUTING_RULES[folder]
    excel_files = [f for f in os.listdir(folder) if f.endswith((".xlsx", ".xls"))]

    for file in excel_files:

        # Apply filename rule if needed
        if pattern and pattern not in file:
            continue

        path = os.path.join(folder, file)
        state_key = f"{folder}/{file}"
        current_hash = get_file_hash(path)

        # Skip unchanged files
        if state_key in file_state and file_state[state_key] == current_hash:
            print(f"Skipping {state_key} (unchanged)")
            continue

        print(f"Processing {state_key}")

        try:
            df = pd.read_excel(path)
            df = optimize_dataframe(df)
            table_name = os.path.splitext(file)[0]

            if table_exists(engine, table_name):
                df.to_sql(table_name, engine, if_exists="append", index=False)
            else:
                df.to_sql(table_name, engine, if_exists="replace", index=False)

            file_state[state_key] = current_hash
            print(f"{state_key} uploaded successfully")

        except Exception as e:
            print(f"Error processing {state_key}: {e}")

# Save updated state
save_state(file_state)
print("All jobs finished successfully ✅")

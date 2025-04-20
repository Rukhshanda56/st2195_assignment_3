import os
import pandas as pd
import sqlite3

# === Set your directory paths ===
data_dir = "/Users/rukhshandaashraf/Desktop/st_2195/st2195_assignment_3_database/dataverse_files/"  # change this
db_path = os.path.join(data_dir, "airline.db")

# === Connect to SQLite database ===
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# === Process each .bz2 file in chunks ===
years = range(2006,2009)
for year in years:
    file_path = os.path.join(data_dir, f"{year}.csv.bz2")
    print(f"Processing {file_path}")
    
    chunk_iter = pd.read_csv(file_path, chunksize=100000)  # memory safe

    for i, chunk in enumerate(chunk_iter):
        chunk.to_sql("ontime", conn, if_exists="append", index=False)
        print(f"  → Chunk {i+1} written")
# === Load reference tables ===
for name, filename in [
    ("airports", "airports.csv"),
    ("carriers", "carriers.csv"),
    ("planes", "plane-data.csv")
]:
    print(f"Loading {name}")
    df = pd.read_csv(os.path.join(data_dir, filename))
    df.to_sql(name, conn, if_exists="replace", index=False)

# === Done ===
conn.close()
print(f"Database created at {db_path}")


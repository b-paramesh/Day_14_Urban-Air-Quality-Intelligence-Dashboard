from __future__ import annotations
import os
import pandas as pd
from pathlib import Path
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

# ---------------------------------------------------------------
# CONFIG
# ---------------------------------------------------------------
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

TABLE_NAME = os.getenv("SUPABASE_AIR_TABLE", "air_quality_data")

STAGED_DIR = Path(os.getenv("STAGED_DIR", Path(__file__).resolve().parents[0] / "data" / "staged"))
CSV_PATH = STAGED_DIR / "air_quality_transformed.csv"

# ---------------------------------------------------------------
# INIT SUPABASE
# ---------------------------------------------------------------
def init_supabase() -> Client:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise ValueError("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")
    return create_client(SUPABASE_URL, SUPABASE_KEY)

# ---------------------------------------------------------------
# CLEAN JSON VALUES
# ---------------------------------------------------------------
def clean_dataframe(df: pd.DataFrame):
    """Convert dataframe values so Supabase can accept them."""
    
    # Convert numeric columns
    for col in df.columns:
        if col not in ["city", "aqi_category", "risk_level", "time"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Convert timestamps → string
    df["time"] = pd.to_datetime(df["time"], errors="coerce").astype(str)

    df = df.replace([float("inf"), float("-inf")], None)
    df = df.where(df.notnull(), None)

    # Convert all numpy types → Python types
    df = df.astype(object)

    cleaned = []
    for rec in df.to_dict(orient="records"):
        row = {}
        for k, v in rec.items():
            if isinstance(v, float) and v != v:  # NaN
                row[k] = None
            else:
                row[k] = v
        cleaned.append(row)

    return cleaned

# ---------------------------------------------------------------
# LOAD TO SUPABASE
# ---------------------------------------------------------------
def load_to_supabase(records, table: str):
    supabase = init_supabase()
    total = len(records)
    batch_size = 500
    inserted = 0

    print(f"🚀 Loading into table: {table}")
    print(f"📦 Total rows: {total}")

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        try:
            supabase.table(table).upsert(batch).execute()
            inserted += len(batch)
            print(f"✅ Batch {i//batch_size + 1} inserted ({len(batch)} rows)")
        except Exception as e:
            print(f"❌ Batch {i//batch_size + 1} failed: {e}")

    print(f"\n🎉 Load finished → {inserted}/{total} rows inserted.")

# ---------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------
def main():
    if not CSV_PATH.exists():
        print(f"❌ CSV not found: {CSV_PATH}")
        return

    print(f"📄 Reading CSV: {CSV_PATH}")
    df = pd.read_csv(CSV_PATH)

    print("🧹 Cleaning dataframe for JSON compatibility...")
    safe_records = clean_dataframe(df)

    print("⬆ Uploading records to Supabase...")
    load_to_supabase(safe_records, TABLE_NAME)

if __name__ == "__main__":
    main()

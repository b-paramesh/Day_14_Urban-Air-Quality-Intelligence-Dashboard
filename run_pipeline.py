# run_pipeline.py

import time
from extract import fetch_all_cities
from transform import transform
from load import main as load_to_supabase
from etl_analysis import run_analysis


def run_full_pipeline():
    print("\n🚀 Starting Full Air Quality ETL Pipeline...\n")

    # 1️⃣ Extract
    print("📥 Step 1: Extracting raw AQI data...")
    extract_results = fetch_all_cities()
    time.sleep(1)

    # 2️⃣ Transform
    print("\n🔄 Step 2: Transforming raw air quality data...")
    transformed_csv = transform()
    time.sleep(1)

    # 3️⃣ Load
    print("\n⬆ Step 3: Loading transformed data into Supabase...")
    load_to_supabase()
    time.sleep(1)

    # 4️⃣ Analysis
    print("\n📊 Step 4: Running analysis and generating reports...")
    run_analysis()

    print("\n🎉 Pipeline completed successfully! All outputs generated.\n")


if __name__ == "__main__":
    run_full_pipeline()

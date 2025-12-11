# transform.py
"""
Transforms raw OpenAQ or Open-Meteo Air Quality API data into
a clean hourly dataset.

Outputs:
    data/staged/air_quality_transformed.csv
"""

import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

RAW_DIR = Path("data/raw")
STAGED_DIR = Path("data/staged")
STAGED_DIR.mkdir(parents=True, exist_ok=True)


# ---------------------------------------------
# Helper: Load single raw file
# ---------------------------------------------
def load_raw_file(path: Path):
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


# ---------------------------------------------
# Helper: Flatten Open-Meteo style hourly data
# ---------------------------------------------
def flatten_hourly(payload: dict, city: str) -> pd.DataFrame:

    hourly = payload.get("hourly", {})

    # Extract arrays
    time = hourly.get("time", [])

    df = pd.DataFrame({
        "city": city,
        "time": time,
        "pm10": hourly.get("pm10", []),
        "pm2_5": hourly.get("pm2_5", []),
        "carbon_monoxide": hourly.get("carbon_monoxide", []),
        "nitrogen_dioxide": hourly.get("nitrogen_dioxide", []),
        "sulphur_dioxide": hourly.get("sulphur_dioxide", []),
        "ozone": hourly.get("ozone", []),
        "uv_index": hourly.get("uv_index", [None]*len(time))
    })

    return df


# ---------------------------------------------
# Feature Engineering
# ---------------------------------------------
def apply_features(df: pd.DataFrame):

    # Convert time to datetime
    df["time"] = pd.to_datetime(df["time"], errors="coerce")

    # Extract hour
    df["hour"] = df["time"].dt.hour

    # Convert pollutants to numeric
    pollutant_cols = [
        "pm2_5","pm10","carbon_monoxide",
        "nitrogen_dioxide","sulphur_dioxide","ozone"
    ]

    for col in pollutant_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ------------------------------------------------------
    # AQI CATEGORY Based on PM2.5
    # ------------------------------------------------------
    def pm25_category(v):
        if pd.isna(v): return "Unknown"
        if v <= 50: return "Good"
        if v <= 100: return "Moderate"
        if v <= 200: return "Unhealthy"
        if v <= 300: return "Very Unhealthy"
        return "Hazardous"

    df["aqi_category"] = df["pm2_5"].apply(pm25_category)

    # ------------------------------------------------------
    # Pollution Severity Score
    # ------------------------------------------------------
    df["severity"] = (
        (df["pm2_5"] * 5) +
        (df["pm10"] * 3) +
        (df["nitrogen_dioxide"] * 4) +
        (df["sulphur_dioxide"] * 4) +
        (df["carbon_monoxide"] * 2) +
        (df["ozone"] * 3)
    )

    # ------------------------------------------------------
    # Risk Classification
    # ------------------------------------------------------
    def risk(v):
        if pd.isna(v): return "Low Risk"
        if v > 400: return "High Risk"
        if v > 200: return "Moderate Risk"
        return "Low Risk"

    df["risk_level"] = df["severity"].apply(risk)

    return df


# ---------------------------------------------
# Main transform
# ---------------------------------------------
def transform():

    print("🔍 Transform step started...")

    all_rows = []

    for file in RAW_DIR.glob("*_raw_*.json"):
        city = str(file.name).split("_raw_")[0]
        print(f"📄 Processing {city} → {file}")

        payload = load_raw_file(file)
        df_city = flatten_hourly(payload, city)
        all_rows.append(df_city)

    # Merge all city frames
    df = pd.concat(all_rows, ignore_index=True)

    # Apply features
    df = apply_features(df)

    # Remove rows with all pollutant values missing
    df = df.dropna(subset=["pm2_5","pm10","ozone","nitrogen_dioxide"], how="all")

    # Save final CSV
    output_path = STAGED_DIR / "air_quality_transformed.csv"
    df.to_csv(output_path, index=False)

    print(f"✅ Transformation complete → {output_path}")


if __name__ == "__main__":
    transform()

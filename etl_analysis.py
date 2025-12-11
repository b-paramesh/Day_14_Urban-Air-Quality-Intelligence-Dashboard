# etl_analysis.py (FIXED VERSION)
import os
import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = "air_quality_data"

PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# -------------------------------------------------------
# SUPABASE
# -------------------------------------------------------
def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


# -------------------------------------------------------
# SAFE LOAD + ADD MISSING COLUMNS
# -------------------------------------------------------
def fetch_data():
    print("🌐 Fetching data from Supabase...")
    client = get_supabase()

    response = client.table(TABLE_NAME).select("*").execute()
    df = pd.DataFrame(response.data)

    print(f"✔ Retrieved {len(df)} rows.")

    # --------------------------------------------------
    # ENSURE REQUIRED COLUMNS EXIST
    # --------------------------------------------------
    required_cols = [
        "pm2_5", "pm10", "ozone", "sulphur_dioxide",
        "nitrogen_dioxide", "carbon_monoxide"
    ]

    for col in required_cols:
        if col not in df.columns:
            df[col] = None

    # Convert all numeric-like columns
    for col in required_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # --------------------------------------------------
    # RECREATE severity IF NOT PRESENT
    # --------------------------------------------------
    if "severity" not in df.columns:
        df["severity"] = (
            df["pm2_5"] * 5 +
            df["pm10"] * 3 +
            df["nitrogen_dioxide"] * 4 +
            df["sulphur_dioxide"] * 4 +
            df["carbon_monoxide"] * 2 +
            df["ozone"] * 3
        )

    # --------------------------------------------------
    # AQI CATEGORY
    # --------------------------------------------------
    if "aqi_category" not in df.columns:
        def categorize_aqi(val):
            if val <= 50: return "Good"
            elif val <= 100: return "Moderate"
            elif val <= 200: return "Unhealthy"
            elif val <= 300: return "Very Unhealthy"
            return "Hazardous"

        df["aqi_category"] = df["pm2_5"].apply(categorize_aqi)

    # --------------------------------------------------
    # RISK LEVEL
    # --------------------------------------------------
    if "risk_level" not in df.columns:
        def risk(val):
            if val > 400: return "High Risk"
            elif val > 200: return "Moderate Risk"
            return "Low Risk"

        df["risk_level"] = df["severity"].apply(risk)

    # --------------------------------------------------
    # HOUR OF DAY
    # --------------------------------------------------
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    if "hour" not in df.columns:
        df["hour"] = df["time"].dt.hour

    return df


# -------------------------------------------------------
# KPI METRICS
# -------------------------------------------------------
def compute_kpis(df):
    print("📊 Computing KPIs...")

    kpis = {
        "city_highest_pm25": df.groupby("city")["pm2_5"].mean().idxmax(),
        "city_highest_severity": df.groupby("city")["severity"].mean().idxmax(),
        "worst_hour": df.groupby("hour")["pm2_5"].mean().idxmax()
    }

    risk_pct = df["risk_level"].value_counts(normalize=True) * 100
    kpis["High Risk %"] = risk_pct.get("High Risk", 0)
    kpis["Moderate Risk %"] = risk_pct.get("Moderate Risk", 0)
    kpis["Low Risk %"] = risk_pct.get("Low Risk", 0)

    pd.DataFrame([kpis]).to_csv(PROCESSED_DIR / "summary_metrics.csv", index=False)
    print("✔ Saved summary_metrics.csv")


# -------------------------------------------------------
# RISK DISTRIBUTION
# -------------------------------------------------------
def compute_risk_distribution(df):
    dist = df.groupby("city")["risk_level"].value_counts().unstack(fill_value=0)
    dist.to_csv(PROCESSED_DIR / "city_risk_distribution.csv")
    print("✔ Saved city_risk_distribution.csv")


# -------------------------------------------------------
# POLLUTION TRENDS
# -------------------------------------------------------
def compute_pollution_trends(df):
    trend = df[["city", "time", "pm2_5", "pm10", "ozone"]].copy()
    trend.to_csv(PROCESSED_DIR / "pollution_trends.csv", index=False)
    print("✔ Saved pollution_trends.csv")


# -------------------------------------------------------
# PLOTS
# -------------------------------------------------------
def make_plots(df):
    # PM2.5 Histogram
    plt.figure(figsize=(8,5))
    df["pm2_5"].hist(bins=40)
    plt.title("PM2.5 Histogram")
    plt.savefig(PROCESSED_DIR / "hist_pm25.png")
    plt.close()

    # Risk per city
    risk_city = df.groupby("city")["risk_level"].value_counts().unstack(fill_value=0)
    risk_city.plot(kind="bar", figsize=(10,6))
    plt.title("Risk Levels per City")
    plt.savefig(PROCESSED_DIR / "risk_by_city.png")
    plt.close()

    # Hourly PM2.5 Trend
    plt.figure(figsize=(12,6))
    for c in df["city"].unique():
        d = df[df["city"] == c].sort_values("time")
        plt.plot(d["time"], d["pm2_5"], label=c)
    plt.legend()
    plt.title("Hourly PM2.5 Trend")
    plt.savefig(PROCESSED_DIR / "pm25_hourly_trend.png")
    plt.close()

    # Scatter: severity vs PM2.5
    plt.scatter(df["pm2_5"], df["severity"], alpha=0.6)
    plt.xlabel("PM2.5")
    plt.ylabel("Severity")
    plt.title("Severity vs PM2.5")
    plt.savefig(PROCESSED_DIR / "severity_vs_pm25.png")
    plt.close()

    print("✔ All plots saved!")


# -------------------------------------------------------
# MAIN
# -------------------------------------------------------
def run_analysis():
    df = fetch_data()
    compute_kpis(df)
    compute_risk_distribution(df)
    compute_pollution_trends(df)
    make_plots(df)
    print("\n🎉 Analysis Completed Successfully!")


if __name__ == "__main__":
    run_analysis()

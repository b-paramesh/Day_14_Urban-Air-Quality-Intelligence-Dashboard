Here is a **clean, professional, GitHub-ready README.md** for your
📌 **Urban Air Quality Intelligence Dashboard**
You can **copy-paste directly** into your repository.

---

# 🌆 Urban Air Quality Intelligence Dashboard

*A Multi-City Real-Time AQI ETL & Analytics System using OpenAQ, Python, and Supabase*

---

## 📌 Overview

This project builds an **end-to-end Air Quality Intelligence System** that collects live environmental data from OpenAQ, transforms it into structured analytics-ready datasets, loads it into Supabase, and generates meaningful insights & visualizations for decision-making.

Developed as part of the **TekWorks 300-Hour AIDS Training Program (Day 14)** under the mentorship of **Karunakar Eede Sir**.

---

## 🧱 Features

### ✔ **1. Extract – Multi-City AQI Collection (OpenAQ API)**

Fetches real-time air-quality readings for the following Indian cities:

* Delhi
* Bengaluru
* Hyderabad
* Mumbai
* Kolkata

Each API call retrieves:

* PM2.5
* PM10
* Ozone
* Nitrogen Dioxide
* Sulphur Dioxide
* Carbon Monoxide
* UV Index (if available)

Includes:

* Retry logic
* Exponential backoff
* Timestamped raw JSON storage (`data/raw/`)
* Graceful error handling

---

### ✔ **2. Transform – Air Quality Feature Engineering**

Raw JSON is converted into a structured hourly dataset.

**Generated Columns:**

* `city`
* `time`
* `pm2_5`, `pm10`, `carbon_monoxide`, `nitrogen_dioxide`, `sulphur_dioxide`, `ozone`, `uv_index`

**Feature Engineering:**

#### 🟦 AQI Categorization (Based on PM2.5)

* 0–50 → Good
* 51–100 → Moderate
* 101–200 → Unhealthy
* 201–300 → Very Unhealthy
* > 300 → Hazardous

#### 🟧 Pollution Severity Score

```
severity = (pm2_5 * 5) + (pm10 * 3)
         + (nitrogen_dioxide * 4) + (sulphur_dioxide * 4)
         + (carbon_monoxide * 2) + (ozone * 3)
```

#### 🟥 Risk Classification

* **High Risk** → severity > 400
* **Moderate Risk** → severity > 200
* **Low Risk** → otherwise

#### 🕒 Hour-of-Day Feature

Extracted from timestamp.

Transformed output saved to:
📄 `data/staged/air_quality_transformed.csv`

---

### ✔ **3. Load – Supabase Cloud Database**

Uploads processed records into a Supabase table: **`air_quality_data`**

Features:

* Batch insertion
* JSON-safe cleaning
* NaN → NULL conversions
* Timestamp normalization
* Auto table creation guidance

---

### ✔ **4. Analytics & Visualizations**

Generates deep insights from transformed and loaded data.

### 📊 **KPIs Generated**

* City with **highest average PM2.5**
* City with **highest pollution severity**
* % of **High / Moderate / Low Risk** hours
* Hour of day with **worst AQI**

### 🗂️ Exported Reports (`data/processed/`)

* `summary_metrics.csv`
* `city_risk_distribution.csv`
* `pollution_trends.csv`

### 📈 Visualizations Saved

* Histogram → PM2.5 Levels
* Bar Chart → City-wise Risk Flags
* Line Chart → Hourly PM2.5 Trends
* Scatter Plot → Severity vs PM2.5

---

## 📁 Project Structure

```
Urban-Air-Quality-Intelligence-Dashboard/
│
├── extract.py
├── transform.py
├── load.py
├── etl_analysis.py
├── run_pipeline.py
│
├── data/
│   ├── raw/
│   ├── staged/
│   ├── processed/
│
└── README.md
```

---

## ▶ Run the Full ETL Pipeline

Run everything (Extract → Transform → Load → Analyze):

```bash
python run_pipeline.py
```

Or individually:

```bash
python extract.py
python transform.py
python load.py
python etl_analysis.py
```

---

## 🛠 Technologies Used

* Python 3.x
* Pandas
* Requests
* OpenAQ Public API
* Supabase (PostgreSQL)
* Matplotlib / Seaborn
* dotenv

---

## 🌍 API Used

**OpenAQ Public API:**
[https://api.openaq.org/v2/latest](https://api.openaq.org/v2/latest)

---

## 🙌 Acknowledgment

Developed under the mentorship of **Karunakar Eede Sir**
at **TekWorks – Advanced AIDS Training Program (300 Hours).**

---

## 📎 GitHub Repository

[https://github.com/b-paramesh/Day_14_Urban-Air-Quality-Intelligence-Dashboard.git](https://github.com/b-paramesh/Day_14_Urban-Air-Quality-Intelligence-Dashboard.git)

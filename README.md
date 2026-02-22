# Weather Analytics Pipeline — End-to-End Mini Project

## Goal

Complete data pipeline using public data with automatic deployment to Databricks:

```
Open-Meteo API → GitHub → GitHub Actions → Databricks → Dashboard
(public source)   (repo)   (CI/CD deploy)   (Bronze→Silver→Gold)  (SQL)
```

---

## Data Source

**Open-Meteo** — free weather API, no API key, no registration required.

```
URL: https://api.open-meteo.com/v1/forecast
Parameters:
  - latitude / longitude  → city coordinates
  - daily                 → max/min temperature, precipitation, wind speed
  - past_days=90          → last 90 days of historical data
  - timezone=auto

Example — Mexico City:
https://api.open-meteo.com/v1/forecast?latitude=19.43&longitude=-99.13&daily=temperature_2m_max,temperature_2m_min,precipitation_sum,windspeed_10m_max&past_days=90&timezone=auto
```

**Cities to monitor:**

| City | Latitude | Longitude |
|---|---|---|
| Mexico City | 19.43 | -99.13 |
| New York | 40.71 | -74.01 |
| London | 51.51 | -0.13 |
| Tokyo | 35.69 | 139.69 |
| Sydney | -33.87 | 151.21 |

---

## Project Structure

```
weather-pipeline/
├── .github/
│   └── workflows/
│       └── deploy.yml            ← GitHub Actions: deploy to Databricks on every push
│
├── bronze/
│   └── ingest_weather.py         ← calls the API, saves raw JSON as Delta
│
├── silver/
│   └── clean_weather.py          ← casts types, validates, normalizes city names
│
├── gold/
│   └── weather_summary.py        ← aggregates: monthly averages per city
│
├── config/
│   └── settings.py               ← paths, cities, variables per ENV
│
├── databricks.yml                ← bundle: defines Job, tasks, schedule, cluster
├── requirements.txt              ← requests (only extra dependency)
└── README.md
```

---

## Full Flow

```
1. git push → main
      ↓
2. GitHub Actions
   - installs Databricks CLI
   - runs: databricks bundle deploy
   - Job is created/updated in the workspace
      ↓
3. Job runs every day at 6am UTC (or manually triggered)

   Task 1: bronze_ingest_weather
     - Calls Open-Meteo API for all 5 cities
     - Saves raw response as Delta
     - Path: /Volumes/workspace/demo/weather/bronze/

   Task 2: silver_clean_weather  (depends_on Task 1)
     - Reads Bronze
     - Casts temperature: string → float
     - Casts date: string → date
     - Filters invalid temperatures (nulls, outliers)
     - Path: /Volumes/workspace/demo/weather/silver/

   Task 3: gold_weather_summary  (depends_on Task 2)
     - Reads Silver
     - groupBy city + month
     - avg(temp_max), avg(temp_min), sum(precipitation)
     - MERGE → idempotent reruns
     - Path: /Volumes/workspace/demo/weather/gold/
      ↓
4. Dashboard in Databricks SQL
   - Connects to the Gold table
   - Line chart: avg temperature per city per month
   - Bar chart: monthly precipitation
   - Auto-refreshes every day
```

---

## Databricks Asset Bundle (databricks.yml)

```yaml
bundle:
  name: weather_pipeline

variables:
  catalog:  { default: workspace }
  schema:   { default: demo }

resources:
  jobs:
    weather_pipeline:
      name: "Weather Pipeline — Bronze → Silver → Gold"

      schedule:
        quartz_cron_expression: "0 0 6 * * ?"
        timezone_id: "UTC"

      email_notifications:
        on_failure:
          - you@email.com

      tasks:
        - task_key: bronze_ingest_weather
          spark_python_task:
            python_file: bronze/ingest_weather.py
          libraries:
            - pypi: { package: requests }

        - task_key: silver_clean_weather
          depends_on:
            - task_key: bronze_ingest_weather
          spark_python_task:
            python_file: silver/clean_weather.py

        - task_key: gold_weather_summary
          depends_on:
            - task_key: silver_clean_weather
          spark_python_task:
            python_file: gold/weather_summary.py
```

---

## GitHub Actions (.github/workflows/deploy.yml)

```yaml
name: Deploy to Databricks

on:
  push:
    branches: [ main ]        # auto-deploy on every push to main

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Install Databricks CLI
        run: pip install databricks-cli

      - name: Deploy bundle
        env:
          DATABRICKS_HOST:  ${{ secrets.DATABRICKS_HOST }}
          DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
        run: databricks bundle deploy
```

**Secrets to configure in GitHub:**
```
Repository → Settings → Secrets → Actions → New secret

DATABRICKS_HOST   = https://adb-XXXX.azuredatabricks.net
DATABRICKS_TOKEN  = dapiXXXXXXXXXXXXXXXX
```

---

## Dashboard — SQL Queries

```sql
-- 1. Monthly average temperature per city (line chart)
SELECT
  city,
  month,
  ROUND(avg_temp_max, 1) AS avg_max,
  ROUND(avg_temp_min, 1) AS avg_min
FROM workspace.demo.gold_weather_summary
ORDER BY city, month;

-- 2. Monthly precipitation per city (bar chart)
SELECT
  city,
  month,
  ROUND(total_precipitation, 1) AS rain_mm
FROM workspace.demo.gold_weather_summary
ORDER BY month, city;

-- 3. Hottest city per month
SELECT month, city, avg_temp_max
FROM workspace.demo.gold_weather_summary
WHERE avg_temp_max = (
  SELECT MAX(avg_temp_max)
  FROM workspace.demo.gold_weather_summary s2
  WHERE s2.month = gold_weather_summary.month
)
ORDER BY month;
```

---

## Implementation Steps

```
1. Create GitHub repo
   git init weather-pipeline
   git remote add origin https://github.com/your-username/weather-pipeline

2. Copy project files
   (bronze/, silver/, gold/, config/, databricks.yml, .github/)

3. Add secrets in GitHub
   DATABRICKS_HOST + DATABRICKS_TOKEN

4. Push to main
   git add .
   git commit -m "initial pipeline"
   git push origin main
   → GitHub Actions deploys automatically

5. Go to Databricks → Jobs & Pipelines
   → see the "Weather Pipeline" Job created
   → trigger it manually for the first run

6. Create Dashboard in Databricks SQL
   → New Dashboard
   → Add visualization → paste the queries above
   → set auto-refresh: every 24h
```

---

## What Happens if a Task Fails

```
Task 2 (Silver) fails
  ↓
Task 3 (Gold) does NOT run   ← blocked by depends_on
  ↓
Email sent → you@email.com   ← on_failure in databricks.yml
  ↓
Engineer checks logs in Databricks UI
  ↓
Fixes the issue
  ↓
Re-runs only Task 2 and Task 3  ← no need to re-run Bronze
  ↓
Gold is updated
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Data source | Open-Meteo REST API |
| Ingestion | Python requests |
| Processing | PySpark + Spark 4.1 |
| Format | Delta Lake |
| Storage | Databricks Unity Catalog Volumes |
| Orchestration | Databricks Workflows |
| IaC | Databricks Asset Bundles |
| CI/CD | GitHub Actions |
| Dashboard | Databricks SQL |

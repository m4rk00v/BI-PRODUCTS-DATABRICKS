"""Bronze layer: ingest raw weather data from Open-Meteo API and save as Delta."""

import os
import json
import requests
from pyspark.sql import SparkSession

CATALOG = os.getenv("CATALOG", "workspace")
SCHEMA = os.getenv("SCHEMA", "demo")
BRONZE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/weather/bronze/"

API_URL = "https://api.open-meteo.com/v1/forecast"
DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "windspeed_10m_max",
]
PAST_DAYS = 90

CITIES = [
    {"name": "Mexico City", "latitude": 19.43, "longitude": -99.13},
    {"name": "New York", "latitude": 40.71, "longitude": -74.01},
    {"name": "London", "latitude": 51.51, "longitude": -0.13},
    {"name": "Tokyo", "latitude": 35.69, "longitude": 139.69},
    {"name": "Sydney", "latitude": -33.87, "longitude": 151.21},
]

spark = SparkSession.builder.getOrCreate()


def fetch_weather(city: dict) -> dict:
    """Call Open-Meteo API for a single city and return raw JSON response."""
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "daily": ",".join(DAILY_VARIABLES),
        "past_days": PAST_DAYS,
        "timezone": "auto",
    }
    response = requests.get(API_URL, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()
    data["city"] = city["name"]
    return data


def ingest_all_cities():
    """Fetch weather data for all cities and write raw JSON rows as Delta."""
    rows = []
    for city in CITIES:
        raw = fetch_weather(city)
        rows.append({"city": city["name"], "raw_json": json.dumps(raw)})

    df = spark.createDataFrame(rows, schema=["city", "raw_json"])
    df.write.format("delta").mode("overwrite").save(BRONZE_PATH)
    print(f"Bronze: wrote {df.count()} rows to {BRONZE_PATH}")


ingest_all_cities()

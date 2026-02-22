"""Bronze layer: ingest raw weather data from Open-Meteo API and save as Delta."""

import json
import requests
from pyspark.sql import SparkSession

import sys, os
try:
    _dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    _dir = os.getcwd()
sys.path.insert(0, _dir)
from config.settings import CITIES, API_URL, DAILY_VARIABLES, PAST_DAYS, BRONZE_PATH

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


if __name__ == "__main__":
    ingest_all_cities()

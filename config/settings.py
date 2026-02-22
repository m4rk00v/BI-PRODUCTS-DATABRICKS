import os

ENV = os.getenv("ENV", "dev")

CATALOG = os.getenv("CATALOG", "workspace")
SCHEMA = os.getenv("SCHEMA", "demo")

VOLUME_BASE = f"/Volumes/{CATALOG}/{SCHEMA}/weather"

BRONZE_PATH = f"{VOLUME_BASE}/bronze/"
SILVER_PATH = f"{VOLUME_BASE}/silver/"
GOLD_PATH = f"{VOLUME_BASE}/gold/"

CITIES = [
    {"name": "Mexico City", "latitude": 19.43, "longitude": -99.13},
    {"name": "New York", "latitude": 40.71, "longitude": -74.01},
    {"name": "London", "latitude": 51.51, "longitude": -0.13},
    {"name": "Tokyo", "latitude": 35.69, "longitude": 139.69},
    {"name": "Sydney", "latitude": -33.87, "longitude": 151.21},
]

API_URL = "https://api.open-meteo.com/v1/forecast"

DAILY_VARIABLES = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "windspeed_10m_max",
]

PAST_DAYS = 90

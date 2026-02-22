"""Silver layer: clean and normalize Bronze weather data."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, arrays_zip, from_json, get_json_object
from pyspark.sql.types import (
    StructType, StructField, StringType, ArrayType, FloatType,
)

import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config.settings import BRONZE_PATH, SILVER_PATH

spark = SparkSession.builder.getOrCreate()


def clean_weather():
    """Read Bronze Delta, parse JSON, cast types, filter invalid rows, write Silver."""
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)

    # Extract daily arrays from raw JSON
    parsed = bronze_df.select(
        col("city"),
        get_json_object(col("raw_json"), "$.daily.time").alias("dates"),
        get_json_object(col("raw_json"), "$.daily.temperature_2m_max").alias("temp_max"),
        get_json_object(col("raw_json"), "$.daily.temperature_2m_min").alias("temp_min"),
        get_json_object(col("raw_json"), "$.daily.precipitation_sum").alias("precipitation"),
        get_json_object(col("raw_json"), "$.daily.windspeed_10m_max").alias("wind_speed"),
    )

    # Define schemas for the JSON arrays
    array_string = ArrayType(StringType())
    array_float = ArrayType(FloatType())

    exploded = parsed.select(
        col("city"),
        explode(
            arrays_zip(
                from_json(col("dates"), array_string).alias("date"),
                from_json(col("temp_max"), array_float).alias("temp_max"),
                from_json(col("temp_min"), array_float).alias("temp_min"),
                from_json(col("precipitation"), array_float).alias("precipitation"),
                from_json(col("wind_speed"), array_float).alias("wind_speed"),
            )
        ).alias("record"),
    )

    silver_df = exploded.select(
        col("city"),
        col("record.date").cast("date").alias("date"),
        col("record.temp_max").cast("float").alias("temp_max"),
        col("record.temp_min").cast("float").alias("temp_min"),
        col("record.precipitation").cast("float").alias("precipitation"),
        col("record.wind_speed").cast("float").alias("wind_speed"),
    )

    # Filter out invalid rows
    silver_df = silver_df.filter(
        col("date").isNotNull()
        & col("temp_max").isNotNull()
        & col("temp_min").isNotNull()
        & col("temp_max").between(-60, 60)
        & col("temp_min").between(-60, 60)
    )

    silver_df.write.format("delta").mode("overwrite").save(SILVER_PATH)
    print(f"Silver: wrote {silver_df.count()} rows to {SILVER_PATH}")


if __name__ == "__main__":
    clean_weather()

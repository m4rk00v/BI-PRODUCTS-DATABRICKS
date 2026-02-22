"""Gold layer: aggregate Silver data into monthly summaries per city."""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, sum as _sum, date_format, round as _round

import sys, os
try:
    _dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
except NameError:
    _dir = os.getcwd()
sys.path.insert(0, _dir)
from config.settings import SILVER_PATH, GOLD_PATH, CATALOG, SCHEMA

spark = SparkSession.builder.getOrCreate()

GOLD_TABLE = f"{CATALOG}.{SCHEMA}.gold_weather_summary"


def build_summary():
    """Read Silver, compute monthly aggregates per city, MERGE into Gold table."""
    silver_df = spark.read.format("delta").load(SILVER_PATH)

    summary = (
        silver_df
        .withColumn("month", date_format(col("date"), "yyyy-MM"))
        .groupBy("city", "month")
        .agg(
            _round(avg("temp_max"), 2).alias("avg_temp_max"),
            _round(avg("temp_min"), 2).alias("avg_temp_min"),
            _round(_sum("precipitation"), 2).alias("total_precipitation"),
            _round(avg("wind_speed"), 2).alias("avg_wind_speed"),
        )
    )

    # Write as Delta — use MERGE for idempotent reruns
    summary.createOrReplaceTempView("updates")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
            city STRING,
            month STRING,
            avg_temp_max FLOAT,
            avg_temp_min FLOAT,
            total_precipitation FLOAT,
            avg_wind_speed FLOAT
        )
        USING DELTA
    """)

    spark.sql(f"""
        MERGE INTO {GOLD_TABLE} AS target
        USING updates AS source
        ON target.city = source.city AND target.month = source.month
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

    count = spark.table(GOLD_TABLE).count()
    print(f"Gold: {GOLD_TABLE} has {count} rows after merge")


if __name__ == "__main__":
    build_summary()

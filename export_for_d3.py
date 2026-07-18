from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split
import os

# 1. Initialize a lightweight local Spark Session
spark = SparkSession.builder \
    .appName("ParquetToJSON") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

PARQUET_PATH = "./cyber_threat_data_lake/alerts"
OUTPUT_JSON_FILE = "alerts_data.json"

print("\n[STEP 1] Extracting records from Parquet Data Lake...")

try:
    # 2. Read the binary parquet files into a DataFrame
    df = spark.read.parquet(PARQUET_PATH)

    # CORE FIX: window_end (e.g., "2026-07-18 19:30:00") ko split karke date aur time columns banayein
    # split(col("window_end"), " ")[0] -> "2026-07-18"
    # split(col("window_end"), " ")[1] -> "19:30:00"
    df_with_datetime = df \
        .withColumn("date", split(col("window_end"), " ")[0]) \
        .withColumn("time", split(col("window_end"), " ")[1])

    # Sort data by window execution time so the charts look chronological
    df_sorted = df_with_datetime.orderBy("window_end")

    # 3. Convert the Spark DataFrame directly to a native Python list of dictionaries
    local_data = [row.asDict() for row in df_sorted.collect()]

    # 4. Standard Python file-handling to dump clean JSON structure
    import json
    with open(OUTPUT_JSON_FILE, "w") as f:
        json.dump(local_data, f, indent=4)

    print(f"[SUCCESS] Successfully exported {len(local_data)} rows to '{OUTPUT_JSON_FILE}'!")

except Exception as e:
    print(f"[ERROR] Export failed. Reason: {e}")

finally:
    # 5. Clean resource allocation
    spark.stop()

from pyspark.sql import SparkSession

# 1. Initialize an independent Spark Session for analytical verification
spark = SparkSession.builder \
    .appName("ParquetReader") \
    .getOrCreate()

# Mute verbose warnings and restrict logging to errors only
spark.sparkContext.setLogLevel("ERROR")

# Define target data lake location
PATH = "./cyber_threat_data_lake/alerts"

print("\n--- READING DATA FROM PARQUET DATA LAKE ---")

try:
    # 2. Read all immutable historical part-files from the directory into a DataFrame
    df = spark.read.parquet(PATH)

    # 3. Calculate and display total accumulated metrics count
    print(f"Total Rows Saved in Data Lake: {df.count()}")

    # 4. Print structured schema format to verify column data types
    print("\n--- STRUCTURAL SCHEMA ---")
    df.printSchema()

    # 5. Output top 20 captured analytical rows without truncating string data
    print("\n--- TOP 20 DETECTED VOLUMETRIC ANOMALIES ---")
    df.show(truncate=False)

except Exception as e:
    print(f"[ERROR] Failed to execute data lake read. Reason: {e}")

finally:
    # 6. Safely stop the Spark Session to release computing resources
    spark.stop()
    print("[SUCCESS] Spark Session closed cleanly.")

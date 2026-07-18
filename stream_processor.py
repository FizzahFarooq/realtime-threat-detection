import os
import warnings
import joblib
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, window
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# 1. PATH CONFIGURATIONS
DATA_LAKE_PATH = "./cyber_threat_data_lake/alerts"
CHECKPOINT_PATH = "./cyber_threat_data_lake/checkpoints"

os.makedirs(DATA_LAKE_PATH, exist_ok=True)
os.makedirs(CHECKPOINT_PATH, exist_ok=True)

# 2. INITIALIZE SPARK SESSION WITH KAFKA PACKAGE
spark = SparkSession.builder \
    .appName("CyberThreatDetectorML") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Mute Spark Core Logs
spark.sparkContext.setLogLevel("ERROR")

# 3. SETUP SCHEMA FOR INCOMING JSON KAFKA DATA
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("source_ip", StringType(), True),
    StructField("dest_ip", StringType(), True),
    StructField("packet_size", IntegerType(), True),
    StructField("connection_duration", DoubleType(), True),
    StructField("is_malicious", IntegerType(), True),
    StructField("attack_type", StringType(), True)
])

# 4. LOAD TRAINED ML MODEL & REGISTER SPARK UDF
trained_model = joblib.load('isolation_forest_cyber.joblib')

def predict_anomaly_udf(packet_size, duration):
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=UserWarning)
        if packet_size is None or duration is None:
            return 0
        # Isolation Forest prediction: 1 = Normal, -1 = Anomaly
        prediction = trained_model.predict([[packet_size, duration]])
        return 1 if prediction[0] == -1 else 0

# Registering function into Spark Engine
predict_anomaly = udf(predict_anomaly_udf, IntegerType())

# 5. READ REAL-TIME STREAM FROM KAFKA BROKER
raw_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "cyber_threats") \
    .option("startingOffsets", "latest") \
    .load()
# 6. PARSE DATA & APPLY ML PREDICTION IN REAL-TIME
parsed_stream = raw_stream \
    .selectExpr("CAST(value AS STRING) as json_payload") \
    .select(from_json(col("json_payload"), schema).alias("data")) \
    .select("data.*")

ml_scored_stream = parsed_stream \
    .withColumn("timestamp", col("timestamp").cast("timestamp")) \
    .withColumn("ml_is_anomalous", predict_anomaly(col("packet_size"), col("connection_duration")))

# 7. HYBRID DETECTION: WATERMARK + SLIDING WINDOW GROUPING & FLATTENING
windowed_alerts = ml_scored_stream \
    .withWatermark("timestamp", "10 seconds") \
    .groupBy(
        window(col("timestamp"), "10 seconds", "5 seconds"),
        col("source_ip")
    ) \
    .agg({"ml_is_anomalous": "sum"}) \
    .withColumnRenamed("sum(ml_is_anomalous)", "ml_attack_count") \
    .filter(col("ml_attack_count") >= 3) \
    .select(
        col("window.end").cast("string").alias("window_end"),
        col("source_ip"),
        col("ml_attack_count")
    )

# 8. OUTPUT TARGET: INDUSTRY SINK (PARQUET DATA LAKE)
print(f"\n[INFO] Streaming engine started successfully.")
print(f"[INFO] Storing live volumetric anomalies to Data Lake path: {DATA_LAKE_PATH}\n")

query = windowed_alerts.writeStream \
    .format("parquet") \
    .option("path", DATA_LAKE_PATH) \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .outputMode("append") \
    .trigger(processingTime="5 seconds") \
    .start()

# 9. GRACEFUL SHUTDOWN
import signal
import sys

def handle_signal(signum, frame):
    print("\n[INFO] Shutdown signal received! Stopping streaming query gracefully...")

    if 'query' in globals() and query.isActive:
        query.stop()
    if 'spark' in globals():
        spark.stop()
    print("[SUCCESS] Spark Session closed cleanly. Exiting.")
    sys.exit(0)

signal.signal(signal.SIGINT, handle_signal)

try:
    query.awaitTermination()
except Exception as e:
    pass

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when
import sys

# Initialize Spark Session with GCS Connector capability
spark = SparkSession.builder \
    .appName("LogisticsOptimization") \
    .getOrCreate()

# Path arguments passed from Airflow
input_path = sys.argv[1]
output_path = sys.argv[2]

# --- 1. EXTRACT ---
# Read JSON data from GCS (Data Lake)
print(f"Reading data from {input_path}")
df = spark.read.json(input_path)

# --- 2. TRANSFORM ---
# Business Logic:
# - Calculate average shipping cost per route (Origin -> Destination)
# - Flag high-cost shipments (Cost > 200)

transformed_df = df.withColumn(
    "is_high_cost",
    when(col("shipping_cost") > 200, True).otherwise(False)
)

# Aggregation: Group by Route
route_stats = transformed_df.groupBy("origin", "destination_city") \
    .agg(
        avg("shipping_cost").alias("avg_cost"),
        avg("weight_kg").alias("avg_weight"),
        count("shipment_id").alias("total_shipments")
    )

# --- 3. LOAD ---
# Write processed data back to GCS in Parquet format (Optimized for BigQuery)
print(f"Writing data to {output_path}")
route_stats.write.mode("overwrite").parquet(output_path)

spark.stop()
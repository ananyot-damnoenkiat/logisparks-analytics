import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when

def process_data(input_file, output_dir):
    # Initialize Spark Session (Local Mode)
    spark = SparkSession.builder \
        .appName("LogisticsEtlLocal") \
        .getOrCreate()
        
    print(f"Reading JSON from: {input_file}")
    df = spark.read.json(input_file)
    
    # --- Transformation Logic ---
    # 1. Flag Expensive Shipments
    df_transformed = df.withColumn(
        "is_expensive",
        when(col("cost_usd") > 500, True).otherwise(False)
    )
    
    # 2. Aggregation: Calculate Avg Cost per Route
    route_stats = df_transformed.groupBy("origin", "destination") \
        .agg(
            count("transaction_id").alias("total_shipments"),
            avg("weight_kg").alias("avg_weight"),
            avg("cost_usd").alias("avg_cost")
        )
        
    # --- Write to Parquet ---
    print(f"Writing Parquet to: {output_dir}")
    # mode('overwrite') is critical to prevent errors on re-runs
    route_stats.write.mode("overwrite").parquet(output_dir)
    
    print("Spark Job Finished Successfully.")
    spark.stop()

if __name__ == "__main__":
    # Get arguments from command line
    if len(sys.argv) != 3:
        print("Usage: spark_transformation.py <input_file> <output_dir>")
        sys.exit(1)
        
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    
    process_data(input_path, output_path)
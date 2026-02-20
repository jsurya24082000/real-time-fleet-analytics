"""
Run Spark job locally, reading from S3 and writing aggregations back.
This demonstrates the same logic that would run on EMR.
"""

import os
os.environ['PYSPARK_PYTHON'] = 'py'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, max as spark_max, min as spark_min
import time

S3_BUCKET = "fleet-analytics-data-lake-054375299485"

def main():
    print("="*60)
    print("FLEET ANALYTICS SPARK JOB (Local Mode)")
    print("="*60)
    
    start_time = time.time()
    
    # Initialize Spark with S3 support
    spark = SparkSession.builder \
        .appName("FleetAnalyticsLocal") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
        .config("spark.driver.memory", "2g") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("\n[1/5] Reading GPS data from S3...")
    gps_df = spark.read.parquet(f"s3a://{S3_BUCKET}/raw/gps/")
    gps_count = gps_df.count()
    print(f"  GPS records: {gps_count:,}")
    
    print("\n[2/5] Reading Delivery data from S3...")
    delivery_df = spark.read.parquet(f"s3a://{S3_BUCKET}/raw/delivery/")
    delivery_count = delivery_df.count()
    print(f"  Delivery records: {delivery_count:,}")
    
    print("\n[3/5] Computing vehicle daily summary...")
    vehicle_summary = gps_df.groupBy("vehicle_id", "year", "month", "day").agg(
        count("*").alias("gps_events"),
        avg("speed_mph").alias("avg_speed"),
        spark_max("speed_mph").alias("max_speed"),
        avg("fuel_level").alias("avg_fuel_level"),
        spark_max("odometer").alias("max_odometer"),
        spark_min("odometer").alias("min_odometer")
    ).withColumn(
        "miles_driven",
        col("max_odometer") - col("min_odometer")
    )
    
    vehicle_count = vehicle_summary.count()
    print(f"  Vehicle summaries: {vehicle_count}")
    print("\n  Sample vehicle summary:")
    vehicle_summary.show(5, truncate=False)
    
    print("\n[4/5] Computing delivery metrics...")
    delivery_metrics = delivery_df.groupBy("vehicle_id", "year", "month", "day", "status").agg(
        count("*").alias("deliveries"),
        avg("distance_miles").alias("avg_distance"),
        avg("delivery_time_minutes").alias("avg_delivery_time"),
        spark_sum("tip_amount").alias("total_tips")
    )
    
    delivery_metric_count = delivery_metrics.count()
    print(f"  Delivery metrics: {delivery_metric_count}")
    print("\n  Sample delivery metrics:")
    delivery_metrics.show(5, truncate=False)
    
    print("\n[5/5] Saving aggregations to S3...")
    
    # Save vehicle summary
    output_vehicle = f"s3a://{S3_BUCKET}/aggregated/vehicle_daily/"
    vehicle_summary.write.mode("overwrite").partitionBy("year", "month", "day").parquet(output_vehicle)
    print(f"  Saved vehicle summary to {output_vehicle}")
    
    # Save delivery metrics
    output_delivery = f"s3a://{S3_BUCKET}/aggregated/delivery_daily/"
    delivery_metrics.write.mode("overwrite").partitionBy("year", "month", "day").parquet(output_delivery)
    print(f"  Saved delivery metrics to {output_delivery}")
    
    elapsed = time.time() - start_time
    
    print("\n" + "="*60)
    print("JOB COMPLETE")
    print("="*60)
    print(f"Input GPS records:        {gps_count:,}")
    print(f"Input Delivery records:   {delivery_count:,}")
    print(f"Output vehicle summaries: {vehicle_count:,}")
    print(f"Output delivery metrics:  {delivery_metric_count:,}")
    print(f"Total execution time:     {elapsed:.2f} seconds")
    
    spark.stop()


if __name__ == "__main__":
    main()

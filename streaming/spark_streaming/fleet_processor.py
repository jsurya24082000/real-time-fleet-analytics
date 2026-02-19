"""
Spark Streaming: Fleet Event Processor
Processes GPS and delivery events in real-time using Spark Structured Streaming.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, avg, count, sum as spark_sum,
    when, lit, expr, current_timestamp, date_format, hour, dayofweek,
    approx_count_distinct, percentile_approx, struct, to_json
)
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType,
    BooleanType, TimestampType
)
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Schema definitions
GPS_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("speed_mph", DoubleType(), True),
    StructField("heading", IntegerType(), True),
    StructField("fuel_level", DoubleType(), True),
    StructField("engine_status", StringType(), True),
    StructField("odometer_miles", DoubleType(), True)
])

DELIVERY_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("delivery_id", StringType(), True),
    StructField("order_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("customer_id", StringType(), True),
    StructField("package_weight_lbs", DoubleType(), True),
    StructField("estimated_delivery_time", StringType(), True),
    StructField("actual_delivery_time", StringType(), True),
    StructField("delay_minutes", IntegerType(), True),
    StructField("notes", StringType(), True)
])


class FleetStreamProcessor:
    """
    Spark Structured Streaming processor for fleet analytics.
    
    Processes GPS and delivery events from Kinesis, computes aggregations,
    and writes results to S3 and Redshift.
    """
    
    def __init__(self, app_name: str = "FleetAnalytics"):
        """Initialize Spark session with required configurations."""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.streaming.checkpointLocation", "s3://fleet-data-lake/checkpoints/") \
            .config("spark.sql.shuffle.partitions", "10") \
            .config("spark.streaming.stopGracefullyOnShutdown", "true") \
            .getOrCreate()
            
        self.spark.sparkContext.setLogLevel("WARN")
        logger.info("Spark session initialized")
        
    def read_kinesis_stream(self, stream_name: str, schema: StructType):
        """
        Read from Kinesis stream.
        
        Args:
            stream_name: Kinesis stream name
            schema: Schema for parsing JSON data
            
        Returns:
            Streaming DataFrame
        """
        return self.spark.readStream \
            .format("kinesis") \
            .option("streamName", stream_name) \
            .option("region", "us-east-1") \
            .option("initialPosition", "LATEST") \
            .option("awsUseInstanceProfile", "true") \
            .load() \
            .selectExpr("CAST(data AS STRING) as json_data") \
            .select(from_json(col("json_data"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("event_timestamp", to_timestamp(col("timestamp")))
            
    def process_gps_stream(self, gps_df):
        """
        Process GPS events and compute fleet metrics.
        
        Args:
            gps_df: Streaming DataFrame with GPS events
            
        Returns:
            Processed DataFrame with aggregations
        """
        # Add derived columns
        enriched_df = gps_df \
            .withColumn("event_date", date_format(col("event_timestamp"), "yyyy-MM-dd")) \
            .withColumn("event_hour", hour(col("event_timestamp"))) \
            .withColumn("day_of_week", dayofweek(col("event_timestamp"))) \
            .withColumn("speed_category", 
                when(col("speed_mph") == 0, "STOPPED")
                .when(col("speed_mph") < 25, "SLOW")
                .when(col("speed_mph") < 45, "NORMAL")
                .when(col("speed_mph") < 65, "FAST")
                .otherwise("VERY_FAST")
            ) \
            .withColumn("fuel_status",
                when(col("fuel_level") < 0.1, "CRITICAL")
                .when(col("fuel_level") < 0.25, "LOW")
                .when(col("fuel_level") < 0.5, "MEDIUM")
                .otherwise("GOOD")
            ) \
            .withColumn("is_moving", col("engine_status") == "ON")
            
        return enriched_df
        
    def compute_fleet_aggregations(self, gps_df):
        """
        Compute real-time fleet aggregations.
        
        Args:
            gps_df: Enriched GPS DataFrame
            
        Returns:
            Aggregated metrics DataFrame
        """
        # 5-minute window aggregations
        fleet_metrics = gps_df \
            .withWatermark("event_timestamp", "1 minute") \
            .groupBy(
                window(col("event_timestamp"), "5 minutes", "1 minute"),
                col("event_date")
            ) \
            .agg(
                count("*").alias("total_events"),
                approx_count_distinct("vehicle_id").alias("active_vehicles"),
                avg("speed_mph").alias("avg_speed"),
                percentile_approx("speed_mph", 0.5).alias("median_speed"),
                avg("fuel_level").alias("avg_fuel_level"),
                spark_sum(when(col("fuel_status") == "CRITICAL", 1).otherwise(0)).alias("critical_fuel_count"),
                spark_sum(when(col("is_moving"), 1).otherwise(0)).alias("moving_vehicles"),
                spark_sum(when(col("engine_status") == "IDLE", 1).otherwise(0)).alias("idle_vehicles")
            ) \
            .withColumn("fleet_utilization_pct", 
                (col("moving_vehicles") / col("active_vehicles") * 100).cast("decimal(5,2)")
            ) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
            
        return fleet_metrics
        
    def process_delivery_stream(self, delivery_df):
        """
        Process delivery events and compute KPIs.
        
        Args:
            delivery_df: Streaming DataFrame with delivery events
            
        Returns:
            Processed DataFrame
        """
        enriched_df = delivery_df \
            .withColumn("event_date", date_format(col("event_timestamp"), "yyyy-MM-dd")) \
            .withColumn("event_hour", hour(col("event_timestamp"))) \
            .withColumn("delay_category",
                when(col("delay_minutes") <= 0, "ON_TIME")
                .when(col("delay_minutes") <= 15, "SLIGHT_DELAY")
                .when(col("delay_minutes") <= 30, "MODERATE_DELAY")
                .when(col("delay_minutes") <= 60, "SIGNIFICANT_DELAY")
                .otherwise("SEVERE_DELAY")
            ) \
            .withColumn("is_delivered", col("event_type") == "DELIVERED") \
            .withColumn("is_failed", col("event_type") == "FAILED_ATTEMPT") \
            .withColumn("package_size",
                when(col("package_weight_lbs") < 1, "SMALL")
                .when(col("package_weight_lbs") < 5, "MEDIUM")
                .when(col("package_weight_lbs") < 20, "LARGE")
                .otherwise("EXTRA_LARGE")
            )
            
        return enriched_df
        
    def compute_delivery_kpis(self, delivery_df):
        """
        Compute delivery KPIs in real-time.
        
        Args:
            delivery_df: Enriched delivery DataFrame
            
        Returns:
            KPI DataFrame
        """
        # Filter to completed deliveries only
        completed = delivery_df.filter(col("event_type") == "DELIVERED")
        
        # 15-minute window KPIs
        delivery_kpis = completed \
            .withWatermark("event_timestamp", "2 minutes") \
            .groupBy(
                window(col("event_timestamp"), "15 minutes", "5 minutes"),
                col("event_date")
            ) \
            .agg(
                count("*").alias("total_deliveries"),
                avg("delay_minutes").alias("avg_delivery_time_minutes"),
                percentile_approx("delay_minutes", 0.5).alias("median_delay"),
                percentile_approx("delay_minutes", 0.95).alias("p95_delay"),
                spark_sum(when(col("delay_category") == "ON_TIME", 1).otherwise(0)).alias("on_time_deliveries"),
                approx_count_distinct("driver_id").alias("active_drivers"),
                approx_count_distinct("vehicle_id").alias("active_vehicles"),
                avg("package_weight_lbs").alias("avg_package_weight")
            ) \
            .withColumn("on_time_percentage", 
                (col("on_time_deliveries") / col("total_deliveries") * 100).cast("decimal(5,2)")
            ) \
            .withColumn("deliveries_per_driver",
                (col("total_deliveries") / col("active_drivers")).cast("decimal(5,2)")
            ) \
            .withColumn("window_start", col("window.start")) \
            .withColumn("window_end", col("window.end")) \
            .drop("window")
            
        return delivery_kpis
        
    def compute_route_efficiency(self, gps_df, delivery_df):
        """
        Compute route efficiency metrics by joining GPS and delivery data.
        
        Args:
            gps_df: GPS events DataFrame
            delivery_df: Delivery events DataFrame
            
        Returns:
            Route efficiency DataFrame
        """
        # This would require more complex stateful processing
        # Simplified version: aggregate by vehicle
        vehicle_metrics = gps_df \
            .withWatermark("event_timestamp", "1 minute") \
            .groupBy(
                window(col("event_timestamp"), "1 hour"),
                col("vehicle_id")
            ) \
            .agg(
                count("*").alias("gps_events"),
                avg("speed_mph").alias("avg_speed"),
                spark_sum(when(col("is_moving"), 1).otherwise(0)).alias("moving_time_events"),
                spark_sum(when(col("engine_status") == "IDLE", 1).otherwise(0)).alias("idle_time_events")
            ) \
            .withColumn("efficiency_score",
                (col("moving_time_events") / col("gps_events") * 100).cast("decimal(5,2)")
            )
            
        return vehicle_metrics
        
    def write_to_s3(self, df, path: str, checkpoint_path: str):
        """
        Write streaming DataFrame to S3 in Parquet format.
        
        Args:
            df: Streaming DataFrame
            path: S3 output path
            checkpoint_path: Checkpoint location
        """
        return df.writeStream \
            .format("parquet") \
            .option("path", path) \
            .option("checkpointLocation", checkpoint_path) \
            .partitionBy("event_date") \
            .outputMode("append") \
            .start()
            
    def write_to_console(self, df, name: str):
        """Write streaming DataFrame to console for debugging."""
        return df.writeStream \
            .format("console") \
            .outputMode("update") \
            .option("truncate", False) \
            .queryName(name) \
            .start()
            
    def run(self):
        """Run the streaming pipeline."""
        logger.info("Starting Fleet Stream Processor...")
        
        # Read streams
        gps_stream = self.read_kinesis_stream("fleet-gps-events", GPS_SCHEMA)
        delivery_stream = self.read_kinesis_stream("fleet-delivery-events", DELIVERY_SCHEMA)
        
        # Process GPS events
        enriched_gps = self.process_gps_stream(gps_stream)
        fleet_metrics = self.compute_fleet_aggregations(enriched_gps)
        
        # Process delivery events
        enriched_delivery = self.process_delivery_stream(delivery_stream)
        delivery_kpis = self.compute_delivery_kpis(enriched_delivery)
        
        # Write outputs
        gps_query = self.write_to_s3(
            enriched_gps,
            "s3://fleet-data-lake/processed/gps/",
            "s3://fleet-data-lake/checkpoints/gps/"
        )
        
        metrics_query = self.write_to_s3(
            fleet_metrics,
            "s3://fleet-data-lake/aggregated/fleet_metrics/",
            "s3://fleet-data-lake/checkpoints/fleet_metrics/"
        )
        
        delivery_query = self.write_to_s3(
            enriched_delivery,
            "s3://fleet-data-lake/processed/deliveries/",
            "s3://fleet-data-lake/checkpoints/deliveries/"
        )
        
        kpi_query = self.write_to_s3(
            delivery_kpis,
            "s3://fleet-data-lake/aggregated/delivery_kpis/",
            "s3://fleet-data-lake/checkpoints/delivery_kpis/"
        )
        
        logger.info("All streaming queries started")
        
        # Wait for termination
        self.spark.streams.awaitAnyTermination()


def main():
    """Entry point for Spark submit."""
    processor = FleetStreamProcessor()
    processor.run()


if __name__ == "__main__":
    main()

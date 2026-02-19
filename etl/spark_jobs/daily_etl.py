"""
Daily ETL Job: Process raw data and load into star schema.
Runs on EMR as a scheduled Spark job.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, when, coalesce, current_timestamp, date_format,
    hour, minute, dayofweek, dayofmonth, dayofyear, weekofyear,
    month, quarter, year, to_date, expr, row_number, sum as spark_sum,
    avg, count, max as spark_max, min as spark_min
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FleetDailyETL:
    """
    Daily ETL pipeline for fleet analytics.
    
    Processes raw GPS and delivery data from S3,
    transforms into star schema, and loads to Redshift.
    """
    
    def __init__(self, process_date: str = None):
        """
        Initialize ETL job.
        
        Args:
            process_date: Date to process (YYYY-MM-DD), defaults to yesterday
        """
        self.spark = SparkSession.builder \
            .appName("FleetDailyETL") \
            .config("spark.sql.shuffle.partitions", "20") \
            .getOrCreate()
            
        self.process_date = process_date or (
            datetime.now() - timedelta(days=1)
        ).strftime('%Y-%m-%d')
        
        # S3 paths
        self.raw_gps_path = f"s3://fleet-data-lake/raw/gps/event_date={self.process_date}/"
        self.raw_delivery_path = f"s3://fleet-data-lake/raw/deliveries/event_date={self.process_date}/"
        self.processed_path = "s3://fleet-data-lake/processed/"
        
        # Redshift connection
        self.redshift_url = "jdbc:redshift://fleet-cluster.xxxxx.us-east-1.redshift.amazonaws.com:5439/fleetdb"
        self.redshift_props = {
            "user": "etl_user",
            "password": "${REDSHIFT_PASSWORD}",
            "driver": "com.amazon.redshift.jdbc42.Driver"
        }
        
        logger.info(f"Initialized ETL for date: {self.process_date}")
        
    def extract_gps_data(self):
        """Extract raw GPS data from S3."""
        logger.info(f"Extracting GPS data from {self.raw_gps_path}")
        
        return self.spark.read.parquet(self.raw_gps_path)
        
    def extract_delivery_data(self):
        """Extract raw delivery data from S3."""
        logger.info(f"Extracting delivery data from {self.raw_delivery_path}")
        
        return self.spark.read.parquet(self.raw_delivery_path)
        
    def transform_gps_data(self, gps_df):
        """
        Transform GPS data: clean, deduplicate, enrich.
        
        Args:
            gps_df: Raw GPS DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Transforming GPS data...")
        
        # Deduplicate by event_id
        window_spec = Window.partitionBy("event_id").orderBy(col("processed_at").desc())
        
        deduped = gps_df \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")
            
        # Data quality checks
        cleaned = deduped \
            .filter(col("latitude").between(-90, 90)) \
            .filter(col("longitude").between(-180, 180)) \
            .filter(col("speed_mph") >= 0) \
            .filter(col("fuel_level").between(0, 1))
            
        # Add dimension keys
        transformed = cleaned \
            .withColumn("date_key", 
                date_format(col("event_timestamp"), "yyyyMMdd").cast("int")) \
            .withColumn("time_key",
                (hour(col("event_timestamp")) * 100 + minute(col("event_timestamp"))).cast("int")) \
            .withColumn("geohash", 
                expr("substring(cast(latitude as string), 1, 5) || substring(cast(longitude as string), 1, 6)"))
                
        return transformed
        
    def transform_delivery_data(self, delivery_df):
        """
        Transform delivery data: clean, deduplicate, calculate metrics.
        
        Args:
            delivery_df: Raw delivery DataFrame
            
        Returns:
            Transformed DataFrame
        """
        logger.info("Transforming delivery data...")
        
        # Deduplicate - keep latest event per delivery_id + event_type
        window_spec = Window.partitionBy("delivery_id", "event_type").orderBy(col("timestamp").desc())
        
        deduped = delivery_df \
            .withColumn("row_num", row_number().over(window_spec)) \
            .filter(col("row_num") == 1) \
            .drop("row_num")
            
        # Filter to completed deliveries for fact table
        completed = deduped.filter(col("event_type").isin(["DELIVERED", "FAILED_ATTEMPT", "RETURNED"]))
        
        # Add dimension keys and metrics
        transformed = completed \
            .withColumn("date_key",
                date_format(col("event_timestamp"), "yyyyMMdd").cast("int")) \
            .withColumn("time_key",
                (hour(col("event_timestamp")) * 100 + minute(col("event_timestamp"))).cast("int")) \
            .withColumn("is_on_time", col("delay_minutes") <= 0) \
            .withColumn("is_successful", col("event_type") == "DELIVERED") \
            .withColumn("attempt_number", lit(1))  # Would need stateful tracking for real implementation
            
        return transformed
        
    def build_vehicle_trips(self, gps_df):
        """
        Aggregate GPS events into vehicle trips.
        
        Args:
            gps_df: Transformed GPS DataFrame
            
        Returns:
            Trip-level DataFrame
        """
        logger.info("Building vehicle trips...")
        
        # Group by vehicle and detect trip boundaries
        # Simplified: aggregate all events per vehicle per day
        trips = gps_df \
            .groupBy("vehicle_id", "date_key") \
            .agg(
                count("*").alias("gps_events"),
                spark_min("event_timestamp").alias("start_time"),
                spark_max("event_timestamp").alias("end_time"),
                avg("speed_mph").alias("avg_speed_mph"),
                spark_max("speed_mph").alias("max_speed_mph"),
                spark_min("fuel_level").alias("min_fuel_level"),
                spark_max("fuel_level").alias("max_fuel_level"),
                spark_sum(when(col("engine_status") == "IDLE", 1).otherwise(0)).alias("idle_events"),
                spark_sum(when(col("engine_status") == "ON", 1).otherwise(0)).alias("moving_events")
            ) \
            .withColumn("duration_minutes",
                (col("end_time").cast("long") - col("start_time").cast("long")) / 60) \
            .withColumn("fuel_consumed",
                col("max_fuel_level") - col("min_fuel_level")) \
            .withColumn("idle_time_pct",
                col("idle_events") / col("gps_events") * 100)
                
        return trips
        
    def build_daily_summary(self, gps_df, delivery_df):
        """
        Build daily fleet summary aggregations.
        
        Args:
            gps_df: Transformed GPS DataFrame
            delivery_df: Transformed delivery DataFrame
            
        Returns:
            Daily summary DataFrame
        """
        logger.info("Building daily summary...")
        
        # GPS aggregations
        gps_summary = gps_df \
            .groupBy("date_key") \
            .agg(
                count("*").alias("total_gps_events"),
                expr("approx_count_distinct(vehicle_id)").alias("active_vehicles"),
                avg("speed_mph").alias("avg_speed"),
                avg("fuel_level").alias("avg_fuel_level")
            )
            
        # Delivery aggregations
        delivery_summary = delivery_df \
            .groupBy("date_key") \
            .agg(
                count("*").alias("total_deliveries"),
                spark_sum(when(col("is_successful"), 1).otherwise(0)).alias("successful_deliveries"),
                spark_sum(when(col("is_on_time"), 1).otherwise(0)).alias("on_time_deliveries"),
                avg("delay_minutes").alias("avg_delay_minutes"),
                expr("approx_count_distinct(driver_id)").alias("active_drivers"),
                expr("approx_count_distinct(vehicle_id)").alias("delivery_vehicles")
            )
            
        # Join summaries
        daily_summary = gps_summary.join(delivery_summary, "date_key", "outer") \
            .withColumn("summary_date", to_date(col("date_key").cast("string"), "yyyyMMdd")) \
            .withColumn("on_time_delivery_pct",
                (col("on_time_deliveries") / col("total_deliveries") * 100).cast("decimal(5,2)")) \
            .withColumn("delivery_success_rate",
                (col("successful_deliveries") / col("total_deliveries") * 100).cast("decimal(5,2)")) \
            .withColumn("created_at", current_timestamp())
            
        return daily_summary
        
    def load_to_redshift(self, df, table_name: str, mode: str = "append"):
        """
        Load DataFrame to Redshift.
        
        Args:
            df: DataFrame to load
            table_name: Target table name
            mode: Write mode (append, overwrite)
        """
        logger.info(f"Loading {df.count()} rows to {table_name}...")
        
        df.write \
            .format("jdbc") \
            .option("url", self.redshift_url) \
            .option("dbtable", table_name) \
            .options(**self.redshift_props) \
            .mode(mode) \
            .save()
            
        logger.info(f"Loaded to {table_name}")
        
    def save_to_s3(self, df, path: str, partition_cols: list = None):
        """
        Save DataFrame to S3 in Parquet format.
        
        Args:
            df: DataFrame to save
            path: S3 path
            partition_cols: Columns to partition by
        """
        logger.info(f"Saving to {path}...")
        
        writer = df.write.mode("overwrite").format("parquet")
        
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
            
        writer.save(path)
        
    def run(self):
        """Execute the full ETL pipeline."""
        logger.info(f"Starting ETL pipeline for {self.process_date}")
        
        try:
            # Extract
            gps_raw = self.extract_gps_data()
            delivery_raw = self.extract_delivery_data()
            
            # Transform
            gps_transformed = self.transform_gps_data(gps_raw)
            delivery_transformed = self.transform_delivery_data(delivery_raw)
            
            # Build aggregations
            vehicle_trips = self.build_vehicle_trips(gps_transformed)
            daily_summary = self.build_daily_summary(gps_transformed, delivery_transformed)
            
            # Save processed data to S3
            self.save_to_s3(
                gps_transformed,
                f"{self.processed_path}gps/",
                ["date_key"]
            )
            self.save_to_s3(
                delivery_transformed,
                f"{self.processed_path}deliveries/",
                ["date_key"]
            )
            self.save_to_s3(
                vehicle_trips,
                f"{self.processed_path}vehicle_trips/",
                ["date_key"]
            )
            
            # Load to Redshift
            self.load_to_redshift(gps_transformed, "fact_gps_events")
            self.load_to_redshift(delivery_transformed, "fact_deliveries")
            self.load_to_redshift(vehicle_trips, "fact_vehicle_trips")
            self.load_to_redshift(daily_summary, "agg_daily_fleet_summary")
            
            logger.info("ETL pipeline completed successfully")
            
        except Exception as e:
            logger.error(f"ETL pipeline failed: {e}")
            raise
        finally:
            self.spark.stop()


def main():
    """Entry point for Spark submit."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Fleet Daily ETL")
    parser.add_argument("--date", type=str, help="Process date (YYYY-MM-DD)")
    args = parser.parse_args()
    
    etl = FleetDailyETL(process_date=args.date)
    etl.run()


if __name__ == "__main__":
    main()

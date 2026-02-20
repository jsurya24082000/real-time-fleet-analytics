"""
Create EMR cluster and run Spark jobs.
"""

import boto3
import time
import json

REGION = "us-east-1"
S3_BUCKET = "fleet-analytics-data-lake-054375299485"

emr = boto3.client('emr', region_name=REGION)
s3 = boto3.client('s3', region_name=REGION)


def upload_spark_job():
    """Upload Spark job to S3."""
    spark_code = '''
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, sum as spark_sum, max as spark_max, min as spark_min

# Initialize Spark with Glue catalog
spark = SparkSession.builder \\
    .appName("FleetAnalyticsETL") \\
    .config("hive.metastore.client.factory.class", 
            "com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory") \\
    .enableHiveSupport() \\
    .getOrCreate()

print("="*60)
print("FLEET ANALYTICS SPARK JOB")
print("="*60)

# Read from Glue tables
print("\\nReading GPS data from Glue catalog...")
gps_df = spark.sql("SELECT * FROM fleet_analytics.raw_gps")
gps_count = gps_df.count()
print(f"GPS records: {gps_count:,}")

print("\\nReading Delivery data from Glue catalog...")
delivery_df = spark.sql("SELECT * FROM fleet_analytics.raw_delivery")
delivery_count = delivery_df.count()
print(f"Delivery records: {delivery_count:,}")

# Compute vehicle daily summary
print("\\nComputing vehicle daily summary...")
vehicle_summary = gps_df.groupBy("vehicle_id", "year", "month", "day").agg(
    count("*").alias("gps_events"),
    avg("speed_mph").alias("avg_speed"),
    spark_max("speed_mph").alias("max_speed"),
    avg("fuel_level").alias("avg_fuel_level"),
    spark_max("odometer").alias("max_odometer"),
    spark_min("odometer").alias("min_odometer")
)

vehicle_summary = vehicle_summary.withColumn(
    "miles_driven",
    col("max_odometer") - col("min_odometer")
)

print(f"Vehicle summaries: {vehicle_summary.count()}")
vehicle_summary.show(10)

# Compute delivery metrics
print("\\nComputing delivery metrics...")
delivery_metrics = delivery_df.groupBy("vehicle_id", "year", "month", "day", "status").agg(
    count("*").alias("deliveries"),
    avg("distance_miles").alias("avg_distance"),
    avg("delivery_time_minutes").alias("avg_delivery_time"),
    spark_sum("tip_amount").alias("total_tips")
)

print(f"Delivery metrics: {delivery_metrics.count()}")
delivery_metrics.show(10)

# Save aggregations to S3
output_path = "s3://fleet-analytics-data-lake-054375299485/aggregated/"

print(f"\\nSaving vehicle summary to {output_path}vehicle_daily/")
vehicle_summary.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
    f"{output_path}vehicle_daily/"
)

print(f"Saving delivery metrics to {output_path}delivery_daily/")
delivery_metrics.write.mode("overwrite").partitionBy("year", "month", "day").parquet(
    f"{output_path}delivery_daily/"
)

# Summary stats
print("\\n" + "="*60)
print("JOB COMPLETE")
print("="*60)
print(f"Input GPS records: {gps_count:,}")
print(f"Input Delivery records: {delivery_count:,}")
print(f"Output vehicle summaries: {vehicle_summary.count()}")
print(f"Output delivery metrics: {delivery_metrics.count()}")

spark.stop()
'''
    
    s3.put_object(
        Bucket=S3_BUCKET,
        Key="scripts/fleet_etl.py",
        Body=spark_code.encode('utf-8')
    )
    print(f"Uploaded Spark job to s3://{S3_BUCKET}/scripts/fleet_etl.py")


def create_emr_cluster():
    """Create EMR cluster with Spark step."""
    
    print("Creating EMR cluster...")
    
    response = emr.run_job_flow(
        Name='fleet-analytics-cluster',
        LogUri=f's3://{S3_BUCKET}/emr-logs/',
        ReleaseLabel='emr-6.15.0',
        Applications=[
            {'Name': 'Spark'},
            {'Name': 'Hadoop'}
        ],
        Instances={
            'InstanceGroups': [
                {
                    'Name': 'Master',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'MASTER',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                },
                {
                    'Name': 'Core',
                    'Market': 'ON_DEMAND',
                    'InstanceRole': 'CORE',
                    'InstanceType': 'm5.xlarge',
                    'InstanceCount': 1
                }
            ],
            'KeepJobFlowAliveWhenNoSteps': False,
            'TerminationProtected': False
        },
        Steps=[
            {
                'Name': 'Fleet Analytics ETL',
                'ActionOnFailure': 'TERMINATE_CLUSTER',
                'HadoopJarStep': {
                    'Jar': 'command-runner.jar',
                    'Args': [
                        'spark-submit',
                        '--deploy-mode', 'cluster',
                        '--conf', 'spark.sql.catalogImplementation=hive',
                        '--conf', 'hive.metastore.client.factory.class=com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory',
                        f's3://{S3_BUCKET}/scripts/fleet_etl.py'
                    ]
                }
            }
        ],
        Configurations=[
            {
                'Classification': 'spark-hive-site',
                'Properties': {
                    'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
                }
            }
        ],
        ServiceRole='EMR_DefaultRole',
        JobFlowRole='EMR_EC2_DefaultRole',
        VisibleToAllUsers=True,
        Tags=[
            {'Key': 'Project', 'Value': 'fleet-analytics'},
            {'Key': 'Environment', 'Value': 'demo'}
        ]
    )
    
    cluster_id = response['JobFlowId']
    print(f"Cluster ID: {cluster_id}")
    
    return cluster_id


def wait_for_cluster(cluster_id: str):
    """Wait for cluster to complete."""
    print(f"\nMonitoring cluster {cluster_id}...")
    
    while True:
        response = emr.describe_cluster(ClusterId=cluster_id)
        state = response['Cluster']['Status']['State']
        
        print(f"  Cluster state: {state}")
        
        if state in ['TERMINATED', 'TERMINATED_WITH_ERRORS']:
            break
        elif state == 'WAITING':
            print("  Cluster is waiting (no steps running)")
            break
        
        time.sleep(30)
    
    # Get step status
    steps = emr.list_steps(ClusterId=cluster_id)
    print("\nStep Results:")
    for step in steps['Steps']:
        print(f"  {step['Name']}: {step['Status']['State']}")
        if step['Status']['State'] == 'FAILED':
            print(f"    Reason: {step['Status'].get('FailureDetails', {}).get('Reason', 'Unknown')}")
    
    return state


def main():
    # Upload Spark job
    upload_spark_job()
    
    # Create cluster
    cluster_id = create_emr_cluster()
    
    # Wait for completion
    final_state = wait_for_cluster(cluster_id)
    
    print(f"\nFinal cluster state: {final_state}")
    
    if final_state == 'TERMINATED':
        print("\n✓ EMR job completed successfully!")
        
        # Check output
        response = s3.list_objects_v2(
            Bucket=S3_BUCKET,
            Prefix='aggregated/',
            MaxKeys=10
        )
        
        if response.get('KeyCount', 0) > 0:
            print("\nOutput files created:")
            for obj in response.get('Contents', []):
                print(f"  s3://{S3_BUCKET}/{obj['Key']}")
    else:
        print("\n✗ EMR job failed")


if __name__ == "__main__":
    main()

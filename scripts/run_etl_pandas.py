"""
Run ETL job using pandas, reading from S3 and writing aggregations back.
Demonstrates the same logic that would run on EMR Spark.
"""

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import io
import time
from datetime import datetime

S3_BUCKET = "fleet-analytics-data-lake-054375299485"
REGION = "us-east-1"

s3 = boto3.client('s3', region_name=REGION)


def read_parquet_from_s3(prefix: str) -> pd.DataFrame:
    """Read all parquet files from S3 prefix."""
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix=prefix)
    
    dfs = []
    for obj in response.get('Contents', []):
        key = obj['Key']
        if key.endswith('.parquet'):
            # Extract partition values from path
            parts = key.split('/')
            partition_values = {}
            for part in parts:
                if '=' in part:
                    k, v = part.split('=')
                    partition_values[k] = int(v) if v.isdigit() else v
            
            # Read parquet
            response = s3.get_object(Bucket=S3_BUCKET, Key=key)
            df = pd.read_parquet(io.BytesIO(response['Body'].read()))
            
            # Add partition columns
            for k, v in partition_values.items():
                df[k] = v
            
            dfs.append(df)
    
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()


def write_parquet_to_s3(df: pd.DataFrame, prefix: str, partition_cols: list):
    """Write DataFrame to S3 as partitioned parquet."""
    for partition_values, group_df in df.groupby(partition_cols):
        if not isinstance(partition_values, tuple):
            partition_values = (partition_values,)
        
        # Build partition path
        partition_path = '/'.join(f"{col}={val}" for col, val in zip(partition_cols, partition_values))
        key = f"{prefix}{partition_path}/data.parquet"
        
        # Write to S3
        table = pa.Table.from_pandas(group_df.drop(columns=partition_cols))
        buffer = io.BytesIO()
        pq.write_table(table, buffer, compression='snappy')
        buffer.seek(0)
        
        s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buffer.getvalue())
        print(f"  Wrote: s3://{S3_BUCKET}/{key}")


def main():
    print("="*60)
    print("FLEET ANALYTICS ETL JOB (Pandas)")
    print("="*60)
    
    start_time = time.time()
    
    # Read GPS data
    print("\n[1/5] Reading GPS data from S3...")
    gps_df = read_parquet_from_s3("raw/gps/")
    print(f"  GPS records: {len(gps_df):,}")
    print(f"  Columns: {list(gps_df.columns)}")
    
    # Read Delivery data
    print("\n[2/5] Reading Delivery data from S3...")
    delivery_df = read_parquet_from_s3("raw/delivery/")
    print(f"  Delivery records: {len(delivery_df):,}")
    
    # Compute vehicle daily summary
    print("\n[3/5] Computing vehicle daily summary...")
    vehicle_summary = gps_df.groupby(['vehicle_id', 'year', 'month', 'day']).agg({
        'event_id': 'count',
        'speed_mph': ['mean', 'max'],
        'fuel_level': 'mean',
        'odometer': ['min', 'max']
    }).reset_index()
    
    # Flatten column names
    vehicle_summary.columns = [
        'vehicle_id', 'year', 'month', 'day',
        'gps_events', 'avg_speed', 'max_speed',
        'avg_fuel_level', 'min_odometer', 'max_odometer'
    ]
    vehicle_summary['miles_driven'] = vehicle_summary['max_odometer'] - vehicle_summary['min_odometer']
    
    print(f"  Vehicle summaries: {len(vehicle_summary):,}")
    print("\n  Sample:")
    print(vehicle_summary.head(5).to_string())
    
    # Compute delivery metrics
    print("\n\n[4/5] Computing delivery metrics...")
    delivery_metrics = delivery_df.groupby(['vehicle_id', 'year', 'month', 'day', 'status']).agg({
        'delivery_id': 'count',
        'distance_miles': 'mean',
        'delivery_time_minutes': 'mean',
        'tip_amount': 'sum'
    }).reset_index()
    
    delivery_metrics.columns = [
        'vehicle_id', 'year', 'month', 'day', 'status',
        'deliveries', 'avg_distance', 'avg_delivery_time', 'total_tips'
    ]
    
    print(f"  Delivery metrics: {len(delivery_metrics):,}")
    print("\n  Sample:")
    print(delivery_metrics.head(5).to_string())
    
    # Save aggregations to S3
    print("\n\n[5/5] Saving aggregations to S3...")
    
    write_parquet_to_s3(vehicle_summary, "aggregated/vehicle_daily/", ['year', 'month', 'day'])
    write_parquet_to_s3(delivery_metrics, "aggregated/delivery_daily/", ['year', 'month', 'day'])
    
    elapsed = time.time() - start_time
    
    print("\n" + "="*60)
    print("JOB COMPLETE")
    print("="*60)
    print(f"Input GPS records:        {len(gps_df):,}")
    print(f"Input Delivery records:   {len(delivery_df):,}")
    print(f"Output vehicle summaries: {len(vehicle_summary):,}")
    print(f"Output delivery metrics:  {len(delivery_metrics):,}")
    print(f"Total execution time:     {elapsed:.2f} seconds")
    
    # Verify output
    print("\n" + "="*60)
    print("VERIFYING OUTPUT")
    print("="*60)
    
    response = s3.list_objects_v2(Bucket=S3_BUCKET, Prefix='aggregated/', MaxKeys=20)
    print(f"\nAggregated files in S3:")
    for obj in response.get('Contents', []):
        print(f"  s3://{S3_BUCKET}/{obj['Key']} ({obj['Size']} bytes)")


if __name__ == "__main__":
    main()
